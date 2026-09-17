"""Transactional feed delivery, rollback, filtering and competing consumers."""

import asyncio
import contextlib
import sys
from datetime import timedelta

import psycopg
import pytest
from psycopg import sql

from fronta import (
    ConfigurationError,
    State,
    TaskNotFound,
    Worker,
    get_task,
    store,
    subscribe,
    unsubscribe,
)
from fronta import (
    feed as feed_module,
)
from fronta.model import Completion, NewTask, Policy
from tests.conftest import wait_until
from tests.stress.backfill import feed_backfill_soak
from tests.workers import In, sleep_task


async def pull(feed):
    return await asyncio.wait_for(anext(feed), 3)


async def test_buffered_hints_coalesce_before_querying_empty_backlog(conn, settings, monkeypatch):
    queries = []
    execute = psycopg.AsyncConnection.execute
    async with subscribe("hints", settings=settings) as feed:
        await conn.execute("SELECT pg_notify('fronta_feed', g::text) FROM generate_series(1,100) g")
        # Receive the notices while notifies() is inactive, as happens while processing a batch.
        await feed.conn.execute("SELECT 1")
        await feed.conn.rollback()

        async def counted(self, query, *args, **kwargs):
            if self is feed.conn and isinstance(query, str) and query.startswith("SELECT seq,"):
                queries.append(query)
            return await execute(self, query, *args, **kwargs)

        monkeypatch.setattr(psycopg.AsyncConnection, "execute", counted)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(feed), 0.1)
        assert 1 <= len(queries) <= 2


async def claim(conn):
    return (
        await store.claim(
            conn, types=["sleep"], worker="feed-test", lease_s=30, deadline_s=1, count=1
        )
        or [None]
    )[0]


async def seed(conn):
    await store.publish_task_type(conn, sleep_task.spec)
    return await store.enqueue(conn, NewTask("sleep", "{}", Policy()))


async def backlog(conn, name="workflow"):
    return await (
        await conn.execute(
            "SELECT task_id, state, attempt FROM fronta.events WHERE subscription=%s ORDER BY seq",
            (name,),
        )
    ).fetchall()


async def test_every_transition_is_durable_and_ack_deletes(conn, settings):
    async with subscribe("workflow", settings=settings, states=list(State)) as feed:
        task_id = await seed(conn)
        row = await claim(conn)
        assert (await store.complete(conn, [Completion(task_id, row.token, "succeed", "42")])).get(
            task_id
        )
        assert not (
            await store.complete(conn, [Completion(task_id, row.token, "succeed", "43")])
        ).get(task_id)
        batch = await pull(feed)
        assert [(e.id, e.state, e.attempt) for e in batch.events] == [
            (task_id, State.QUEUED, 0),
            (task_id, State.RUNNING, 1),
            (task_id, State.SUCCEEDED, 1),
        ]
        assert [e.seq for e in batch.events] == sorted(e.seq for e in batch.events)
        await batch.ack()
        assert await backlog(conn) == []
        with pytest.raises(RuntimeError, match="no longer active"):
            await batch.ack()


async def test_no_ack_and_early_exit_redeliver(conn, settings):
    async with subscribe("workflow", states=[State.QUEUED], settings=settings) as feed:
        task_id = await seed(conn)
        first = await pull(feed)
        second = await pull(feed)
        assert first.events == second.events
        with pytest.raises(RuntimeError, match="no longer active"):
            await first.ack()
    async with subscribe("workflow", states=[State.QUEUED], settings=settings) as feed:
        batch = await pull(feed)
        assert [e.id for e in batch.events] == [task_id]
        await batch.ack()


async def test_concurrent_consumers_never_hold_the_same_row(conn, settings):
    async with (
        subscribe("workflow", states=[State.QUEUED], settings=settings, batch_size=1) as one,
        subscribe("workflow", states=[State.QUEUED], settings=settings, batch_size=1) as two,
    ):
        ids = [await seed(conn) for _ in range(2)]
        a, b = await asyncio.gather(pull(one), pull(two))
        assert {a.events[0].id, b.events[0].id} == set(ids)
        await a.ack()
        await b.ack()
        assert await backlog(conn) == []


async def test_filters_and_registration_start_with_later_transitions(conn, settings):
    old_id = await seed(conn)
    async with (
        subscribe("workflow", states=[State.QUEUED], types=["sleep"], settings=settings) as feed,
        subscribe("terminal", settings=settings),
    ):
        new_id = await seed(conn)
        await store.enqueue(conn, NewTask("other", "{}", Policy()))
        row = await claim(conn)
        assert row.id == old_id
        (await store.complete(conn, [Completion(row.id, row.token, "succeed", "null")])).get(row.id)
        batch = await pull(feed)
        assert [e.id for e in batch.events] == [new_id]
        await batch.ack()
        assert await backlog(conn, "terminal") == [(old_id, "succeeded", 1)]


@pytest.mark.usefixtures("sdk")
async def test_reaction_enqueue_and_ack_share_a_transaction(conn, settings):
    async with subscribe(
        "workflow", states=[State.QUEUED], types=["source"], settings=settings
    ) as feed:
        source_id = await store.enqueue(conn, NewTask("source", "{}", Policy()))
        batch = await pull(feed)
        child = await sleep_task.enqueue(In(), conn=batch.conn)
        assert await store.get_task(conn, child) is None
        # Losing the delivery rolls back its reaction as well.
        batch = await pull(feed)
        assert [e.id for e in batch.events] == [source_id]
        assert await store.get_task(conn, child) is None
        child = await sleep_task.enqueue(In(), conn=batch.conn)
        await batch.ack()
        assert await get_task(child, conn=conn) is not None
        assert await backlog(conn) == []


@pytest.mark.usefixtures("sdk")
async def test_rollback_savepoints_and_dedupe_publish_nothing_extra(conn, dsn, settings):
    async with subscribe("workflow", settings=settings, states=list(State)):
        async with await psycopg.AsyncConnection.connect(dsn) as caller:
            first = await sleep_task.enqueue(In(), conn=caller, key="same")
            async with caller.transaction(force_rollback=True):
                await sleep_task.enqueue(In(), conn=caller, key="rolled-back")
            assert await sleep_task.enqueue(In(), conn=caller, key="same") == first
            assert await backlog(conn) == []
            await caller.commit()
            assert await backlog(conn) == [(first, "queued", 0)]
            await store.publish_task_type(caller, sleep_task.spec)
            row = await claim(caller)
            (await store.complete(caller, [Completion(row.id, row.token, "succeed", "42")])).get(
                row.id
            )
            await caller.rollback()
        assert await backlog(conn) == [(first, "queued", 0)]


async def test_delayed_commit_with_lower_sequence_is_never_skipped(conn, dsn, settings):
    async with (
        subscribe("workflow", settings=settings, states=[State.QUEUED]) as feed,
        await psycopg.AsyncConnection.connect(dsn) as caller,
    ):
        late = await store.enqueue(caller, NewTask("sleep", "{}", Policy()))
        early = await seed(conn)
        batch = await pull(feed)
        assert [e.id for e in batch.events] == [early]
        high_seq = batch.events[0].seq
        await batch.ack()
        await caller.commit()
        batch = await pull(feed)
        assert [e.id for e in batch.events] == [late]
        assert batch.events[0].seq < high_seq
        await batch.ack()


@pytest.mark.parametrize("outcome", ["retry", "fail", "queued_cancel", "running_cancel", "reap"])
async def test_terminal_paths_and_retry_sequences(conn, settings, outcome):
    async with subscribe("workflow", settings=settings, states=list(State)) as feed:
        task_id = await seed(conn)
        if outcome == "queued_cancel":
            await store.request_cancel(conn, task_id)
            expected = [("queued", 0), ("cancelled", 0)]
        else:
            row = await claim(conn)
            if outcome == "running_cancel":
                await store.request_cancel(conn, task_id)
                await store.request_cancel(conn, task_id)
                (await store.complete(conn, [Completion(task_id, row.token, "cancel")])).get(
                    task_id
                )
                expected = [("queued", 0), ("running", 1), ("cancelled", 1)]
            elif outcome == "fail":
                (
                    await store.complete(conn, [Completion(task_id, row.token, "fail_final", "{}")])
                ).get(task_id)
                expected = [("queued", 0), ("running", 1), ("failed", 1)]
            else:
                if outcome == "reap":
                    await conn.execute("UPDATE fronta.tasks SET lease_until=now()-interval '1s'")
                    await store.reap(conn)
                else:
                    (
                        await store.complete(conn, [Completion(task_id, row.token, "fail", "{}")])
                    ).get(task_id)
                await conn.execute("UPDATE fronta.tasks SET run_at=now()")
                row = await claim(conn)
                (
                    await store.complete(conn, [Completion(task_id, row.token, "succeed", "null")])
                ).get(task_id)
                expected = [
                    ("queued", 0),
                    ("running", 1),
                    ("queued", 1),
                    ("running", 2),
                    ("succeeded", 2),
                ]
        batch = await pull(feed)
        assert [(e.state.value, e.attempt) for e in batch.events] == expected
        await batch.ack()


@pytest.mark.usefixtures("sdk")
async def test_unsubscribe_removes_backlog(conn, settings):
    async with subscribe("workflow", settings=settings, states=list(State)):
        await seed(conn)
    await unsubscribe("workflow")
    assert await backlog(conn) == []


@pytest.mark.usefixtures("sdk")
async def test_unsubscribe_waits_for_its_batch_without_blocking_other_subscriptions(conn, settings):
    async with (
        subscribe("workflow", settings=settings, states=[State.QUEUED], types=["sleep"]) as feed,
        subscribe("other", settings=settings, states=[State.QUEUED], types=["other"]) as other,
    ):
        await seed(conn)
        batch = await pull(feed)
        removing = asyncio.create_task(unsubscribe("workflow"))
        try:

            async def waiting():
                row = await (
                    await conn.execute(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity"
                        " WHERE datname=current_database()"
                        " AND wait_event_type='Lock'"
                        " AND query LIKE 'DELETE FROM fronta.subscriptions%')"
                    )
                ).fetchone()
                return row[0]

            await wait_until(waiting)
            unrelated = await asyncio.wait_for(
                store.enqueue(conn, NewTask("other", "{}", Policy())), 1
            )
            other_batch = await pull(other)
            assert [event.id for event in other_batch.events] == [unrelated]
            await other_batch.ack()
            # A reaction can still acquire compatible key-share locks while DELETE waits.
            await asyncio.wait_for(sleep_task.enqueue(In(), conn=batch.conn), 1)
        finally:
            await batch.ack()
            await asyncio.wait_for(removing, 5)
        assert await backlog(conn) == []
        with pytest.raises(StopAsyncIteration):
            await anext(feed)


async def test_old_snapshot_cannot_publish_after_subscription_deletion(conn, dsn, settings):
    async with subscribe("workflow", settings=settings, states=[State.QUEUED]):
        async with await psycopg.AsyncConnection.connect(dsn) as deleting:
            await deleting.execute("DELETE FROM fronta.subscriptions WHERE name='workflow'")
            publishing = asyncio.create_task(store.enqueue(conn, NewTask("sleep", "{}", Policy())))
            try:

                async def waiting():
                    row = await (
                        await deleting.execute(
                            "SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=%s",
                            (conn.info.backend_pid,),
                        )
                    ).fetchone()
                    return row[0]

                await wait_until(waiting)
            finally:
                await deleting.commit()
                task_id = await asyncio.wait_for(publishing, 5)
        assert (await store.get_task(conn, task_id)).state is State.QUEUED
        assert await backlog(conn) == []
    await seed(conn)
    assert await backlog(conn) == []


async def test_purge_warns_for_stale_unacknowledged_events(conn, settings, run_worker, caplog):
    async with subscribe("workflow", settings=settings, states=[State.QUEUED]):
        await seed(conn)
        await conn.execute("UPDATE fronta.events SET created_at = now()-interval '8 days'")
        async with run_worker(
            Worker([sleep_task], settings=settings.model_copy(update={"purge_interval_s": 0.05}))
        ):

            async def empty():
                return not await backlog(conn)

            await wait_until(empty)
    assert "workflow" in caplog.text
    assert "unacknowledged events" in caplog.text


async def test_get_task_unknown_raises(conn):
    with pytest.raises(TaskNotFound):
        await get_task(999, conn=conn)


async def retained(conn, count=20, *, states=("succeeded",), types=("sleep",)):
    """Bulk snapshots with every state/type combination and nontrivial attempt numbers."""
    return await (
        await conn.execute(
            "INSERT INTO fronta.tasks (type, state, input, attempt, max_attempts, "
            "attempt_timeout_s, backoff_base_s, backoff_factor, backoff_cap_s, finished_at) "
            "SELECT (%s::text[])[1 + ((g-1) / %s) %% %s], "
            "(%s::text[])[1 + (g-1) %% %s], '{}', g %% 3, 3, 30, 1, 2, 3600, now() "
            "FROM generate_series(1, %s) g RETURNING id, type, state, attempt",
            (list(types), len(states), len(types), list(states), len(states), count),
        )
    ).fetchall()


def snapshots(rows):
    return {(task_id, state, attempt) for task_id, _typ, state, attempt in rows}


async def register(settings, name="workflow", **kwargs):
    async with subscribe(name, settings=settings, **kwargs):
        pass


@contextlib.asynccontextmanager
async def paused_backfill(monkeypatch, *, after=1):
    """Pause a consumer after a committed chunk, leaving other consumers free to resume."""
    entered, release = asyncio.Event(), asyncio.Event()
    results = []
    original = store.backfill_chunk

    async def chunk(*args):
        result = await original(*args)
        results.append(result)
        if len(results) == after:
            entered.set()
            await release.wait()
        return result

    with monkeypatch.context() as patch:
        patch.setattr(feed_module, "_BACKFILL_CHUNK", 7)
        patch.setattr(store, "backfill_chunk", chunk)
        try:
            yield entered, release, results
        finally:
            release.set()


async def pending_marker(conn, name="workflow"):
    row = await (
        await conn.execute("SELECT backfill FROM fronta.subscriptions WHERE name=%s", (name,))
    ).fetchone()
    assert row is not None
    return row[0]


async def subscription_generation(conn, name="workflow"):
    row = await (
        await conn.execute("SELECT generation FROM fronta.subscriptions WHERE name=%s", (name,))
    ).fetchone()
    assert row is not None
    return row[0]


@pytest.mark.parametrize("backfill", [None, False])
async def test_backfill_off_matches_release_semantics(conn, dsn, settings, monkeypatch, backfill):
    await retained(conn, 1)
    original = store.register_subscription

    async def inspected(c, *args):
        locks = await (
            await conn.execute(
                "SELECT mode FROM pg_locks WHERE pid=%s "
                "AND relation='fronta.subscriptions'::regclass",
                (c.info.backend_pid,),
            )
        ).fetchall()
        assert ("ExclusiveLock",) not in locks
        return await original(c, *args)

    monkeypatch.setattr(store, "register_subscription", inspected)
    # Ordinary registration neither locks the table nor waits for older transactions.
    async with await psycopg.AsyncConnection.connect(dsn) as holder:
        await holder.execute("SELECT * FROM fronta.subscriptions FOR KEY SHARE")
        await asyncio.wait_for(register(settings, backfill=backfill), 1)
    assert await backlog(conn) == []


async def test_backfill_true_projects_matching_rows(conn, settings):
    rows = await retained(conn, 10, states=list(State), types=("sleep", "other"))
    await register(settings, "all", states=list(State), backfill=True)
    await register(settings, "some", states=[State.SUCCEEDED], types=["sleep"], backfill=True)
    assert set(await backlog(conn, "all")) == snapshots(rows)
    expected = [row for row in rows if row[1:3] == ("sleep", "succeeded")]
    assert set(await backlog(conn, "some")) == snapshots(expected)


@pytest.mark.parametrize("window", [3600, 3600.0, timedelta(hours=1)])
async def test_backfill_window_bounds_terminal_rows(conn, settings, window):
    rows = await retained(conn, 10, states=list(State))
    await conn.execute(
        "UPDATE fronta.tasks SET created_at=now()-interval '3h', "
        "finished_at=now()-CASE WHEN id <= 5 THEN interval '3h' ELSE interval '30m' END"
    )
    await register(settings, states=list(State), backfill=window)
    expected = [r for r in rows if r[0] > 5 or r[2] in ("queued", "running")]
    assert set(await backlog(conn)) == snapshots(expected)


async def test_zero_window_is_enabled_and_empty_states_clear_marker(conn, settings):
    rows = await retained(conn, 5, states=list(State))
    await register(settings, states=list(State), backfill=0)
    assert set(await backlog(conn)) == snapshots([r for r in rows if r[2] in ("queued", "running")])
    await register(settings, "empty", states=[], backfill=True)
    assert await backlog(conn, "empty") == []
    assert await pending_marker(conn, "empty") is None


@pytest.mark.usefixtures("sdk")
async def test_existing_name_never_backfills(conn, settings):
    await retained(conn, 3)
    async with subscribe("workflow", settings=settings, backfill=True) as feed:
        await (await pull(feed)).ack()
    task_id = await seed(conn)
    row = await claim(conn)
    await store.complete(conn, [Completion(task_id, row.token, "succeed", "null")])
    expected = await backlog(conn)
    assert expected == [(task_id, "succeeded", 1)]
    for backfill in (None, False, True, 3600):
        await register(settings, backfill=backfill)
        assert await backlog(conn) == expected
    await register(settings, states=list(State), types=["other"], backfill=True)
    assert await backlog(conn) == expected
    await unsubscribe("workflow")
    await register(settings, backfill=True)
    assert len(await backlog(conn)) == 4


async def test_backfill_chunks_and_marker_lifecycle(conn, settings, monkeypatch):
    rows = await retained(conn)
    async with (
        paused_backfill(monkeypatch) as (entered, release, results),
        asyncio.TaskGroup() as tg,
    ):
        creating = tg.create_task(register(settings, states=[State.SUCCEEDED], backfill=True))
        await asyncio.wait_for(entered.wait(), 3)
        assert not creating.done()
        marker = await pending_marker(conn)
        generation = await subscription_generation(conn)
        assert marker["since"] is None
        assert marker["pending"] == {"succeeded": 7}
        assert (await store.stats(conn))["subscriptions"][0]["backfill_pending"] is True
        assert len(await backlog(conn)) == 7
        release.set()
    assert results == [(7, True), (7, True), (6, False)]
    assert set(await backlog(conn)) == snapshots(rows)
    assert len(await backlog(conn)) == len(rows)
    assert await pending_marker(conn) is None
    assert (await store.stats(conn))["subscriptions"][0]["backfill_pending"] is False
    assert await subscription_generation(conn) == generation


@pytest.mark.parametrize("resume", [None, False, True, 0])
async def test_backfill_resumes_after_connection_loss(conn, settings, monkeypatch, resume):
    rows = await retained(conn)
    original = store.backfill_chunk
    monkeypatch.setattr(feed_module, "_BACKFILL_CHUNK", 7)

    async def disconnect(c, *args):
        result = await original(c, *args)
        await c.close()
        return result

    with monkeypatch.context() as patch:
        patch.setattr(store, "backfill_chunk", disconnect)
        with pytest.raises(psycopg.OperationalError):
            await register(settings, states=[State.SUCCEEDED], backfill=3600)
    marker = await pending_marker(conn)
    assert marker["pending"] == {"succeeded": 7}
    assert marker["since"] is not None
    # Make a remaining row older than the persisted boundary: a resume with True must still
    # apply the original window; a resume with zero must not move that boundary to now.
    await conn.execute(
        "UPDATE fronta.tasks SET finished_at=%s::timestamptz-interval '1s' WHERE id=20",
        (marker["since"],),
    )
    await register(settings, backfill=resume)
    assert set(await backlog(conn)) == snapshots(rows[:-1])
    assert len(await backlog(conn)) == 19
    assert await pending_marker(conn) is None


async def test_concurrent_creators_backfill_once(conn, settings, monkeypatch):
    rows = await retained(conn, 50)
    original = store.register_subscription
    both_read, reads = asyncio.Event(), 0

    async def read(*args):
        nonlocal reads
        if reads < 2:
            reads += 1
            if reads == 2:
                both_read.set()
            await both_read.wait()
        return await original(*args)

    monkeypatch.setattr(store, "register_subscription", read)
    async with contextlib.AsyncExitStack() as stack:
        one, two = await asyncio.gather(
            *(
                stack.enter_async_context(
                    subscribe("workflow", settings=settings, backfill=True, batch_size=25)
                )
                for _ in range(2)
            )
        )
        assert set(await backlog(conn)) == snapshots(rows)
        assert len(await backlog(conn)) == 50
        first, second = await asyncio.gather(pull(one), pull(two))
        assert {e.id for e in first.events}.isdisjoint(e.id for e in second.events)
        await first.ack()
        await second.ack()
        assert await backlog(conn) == []


async def test_concurrent_resumers_do_not_duplicate(conn, settings, monkeypatch):
    rows = await retained(conn)
    async with paused_backfill(monkeypatch) as (entered, release, _), asyncio.TaskGroup() as tg:
        tg.create_task(register(settings, states=[State.SUCCEEDED], backfill=True))
        await asyncio.wait_for(entered.wait(), 3)
        await asyncio.gather(register(settings), register(settings))
        release.set()
    assert set(await backlog(conn)) == snapshots(rows)
    assert len(await backlog(conn)) == len(rows)
    assert await pending_marker(conn) is None


@pytest.mark.usefixtures("sdk")
async def test_unsubscribe_during_backfill_leaves_no_orphans(conn, settings, monkeypatch, caplog):
    caplog.set_level("INFO", logger="fronta.feed")
    await retained(conn)
    entered, release = asyncio.Event(), asyncio.Event()
    execute = psycopg.AsyncConnection.execute
    monkeypatch.setattr(feed_module, "_BACKFILL_CHUNK", 7)

    async def paused(c, query, *args, **kwargs):
        if isinstance(query, str) and query.startswith("UPDATE fronta.subscriptions SET backfill"):
            entered.set()
            await release.wait()
        return await execute(c, query, *args, **kwargs)

    monkeypatch.setattr(psycopg.AsyncConnection, "execute", paused)

    async def consumer():
        async with subscribe(
            "workflow", settings=settings, states=[State.SUCCEEDED], backfill=True
        ) as feed:
            with pytest.raises(StopAsyncIteration):
                await pull(feed)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(consumer())
        await asyncio.wait_for(entered.wait(), 3)
        removing = tg.create_task(unsubscribe("workflow"))

        async def waiting():
            return (
                await (
                    await conn.execute(
                        "SELECT EXISTS (SELECT FROM pg_stat_activity "
                        "WHERE datname=current_database() AND wait_event_type='Lock' "
                        "AND query LIKE 'DELETE FROM fronta.subscriptions%')"
                    )
                ).fetchone()
            )[0]

        try:
            await wait_until(waiting)
            assert not removing.done()
        finally:
            release.set()
    assert await (await conn.execute("SELECT name FROM fronta.subscriptions")).fetchall() == []
    assert await backlog(conn) == []
    assert "backfill stopped name=workflow: registration removed" in caplog.text
    assert "backfill finished name=workflow" not in caplog.text


async def test_backfilled_events_get_fresh_retention(conn, settings):
    await retained(conn, 3)
    await conn.execute("UPDATE fronta.tasks SET finished_at=now()-interval '2h'")
    await register(settings, backfill=True)
    assert await store.purge_events(conn, 3600, 1000) == []
    assert len(await backlog(conn)) == 3
    await conn.execute("UPDATE fronta.events SET created_at=now()-interval '2h'")
    assert await store.purge_events(conn, 3600, 1000) == [("workflow", 3)]
    assert await backlog(conn) == []


@pytest.mark.parametrize("backfill", [None, False, True, 0])
@pytest.mark.parametrize("column", ["backfill", "generation"])
async def test_missing_column_is_actionable(conn, settings, backfill, column):
    await conn.execute(
        sql.SQL("ALTER TABLE fronta.subscriptions DROP COLUMN {}").format(sql.Identifier(column))
    )
    try:
        with pytest.raises(ConfigurationError, match="fronta db init"):
            await register(settings, backfill=backfill)
        assert await (await conn.execute("SELECT name FROM fronta.subscriptions")).fetchall() == []
    finally:
        await store.init_schema(conn)
    await register(settings, states=[State.QUEUED], backfill=backfill)
    task_id = await seed(conn)
    assert await backlog(conn) == [(task_id, "queued", 0)]
    assert (await store.stats(conn))["subscriptions"][0]["backfill_pending"] is False


@pytest.mark.parametrize(
    "backfill",
    [
        -1,
        float("nan"),
        float("inf"),
        -float("inf"),
        timedelta(days=-1),
        "1",
        [],
        object(),
        pytest.param(-(10**1000), id="large-negative"),
        pytest.param(10**1000, id="large-positive"),
    ],
)
async def test_backfill_argument_validation(monkeypatch, backfill):
    async def unexpected_connect(*_args, **_kwargs):
        pytest.fail("invalid backfill opened a connection")

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", unexpected_connect)
    with pytest.raises(ValueError, match="backfill"):
        async with subscribe("invalid", backfill=backfill):
            pytest.fail("invalid backfill accepted")


@pytest.mark.parametrize("types", [[], None, ["a"]])
async def test_backfill_type_filters(conn, settings, types):
    rows = await retained(conn, types=("a", "b"))
    await register(settings, types=types, backfill=True)
    assert set(await backlog(conn)) == snapshots(
        [r for r in rows if types is None or r[1] in types]
    )


async def test_backfill_reads_current_types_between_chunks(conn, settings, monkeypatch):
    rows = await retained(conn, types=("a", "b"))
    async with paused_backfill(monkeypatch) as (entered, release, _), asyncio.TaskGroup() as tg:
        tg.create_task(register(settings, states=[State.SUCCEEDED], backfill=True))
        await asyncio.wait_for(entered.wait(), 3)
        await register(settings, types=["b"])
        release.set()
    assert set(await backlog(conn)) == snapshots([r for r in rows if r[0] <= 7 or r[1] == "b"])


async def test_backfill_statement_timeout_rolls_back_events_and_marker(conn, settings, monkeypatch):
    rows = await retained(conn)
    monkeypatch.setattr(feed_module, "_BACKFILL_CHUNK", 7)
    execute, updates = psycopg.AsyncConnection.execute, 0

    async def timeout(c, query, *args, **kwargs):
        nonlocal updates
        if isinstance(query, str) and query.startswith("UPDATE fronta.subscriptions SET backfill"):
            updates += 1
            if updates == 2:
                await execute(c, "SELECT pg_sleep(0.5)")
        return await execute(c, query, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(psycopg.AsyncConnection, "execute", timeout)
        with pytest.raises(psycopg.errors.QueryCanceled):
            await register(
                settings.model_copy(update={"statement_timeout_s": 0.1}),
                states=[State.SUCCEEDED],
                backfill=True,
            )
    marker = await pending_marker(conn)
    assert marker["since"] is None
    assert marker["pending"] == {"succeeded": 7}
    assert set(await backlog(conn)) == snapshots(rows[:7])
    await register(settings)
    assert set(await backlog(conn)) == snapshots(rows)
    assert len(await backlog(conn)) == len(rows)


async def test_live_backfill_duplicates_have_distinct_sequences(conn, settings, monkeypatch):
    old = await retained(conn, 1)
    live_id = await seed(conn)
    running = await claim(conn)
    original = store.backfill_chunk
    entered, release = asyncio.Event(), asyncio.Event()

    async def paused(*args):
        entered.set()
        await release.wait()
        return await original(*args)

    monkeypatch.setattr(store, "backfill_chunk", paused)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(register(settings, states=[State.SUCCEEDED], backfill=True))
        await asyncio.wait_for(entered.wait(), 3)
        await store.complete(conn, [Completion(live_id, running.token, "succeed", "null")])
        release.set()
    events = await (
        await conn.execute("SELECT task_id, state, attempt, seq FROM fronta.events ORDER BY seq")
    ).fetchall()
    assert len([e for e in events if e[0] == old[0][0]]) == 1
    duplicate = [e for e in events if e[0] == live_id]
    assert len(duplicate) == 2
    assert {e[:3] for e in duplicate} == {(live_id, "succeeded", 1)}
    assert duplicate[0][3] != duplicate[1][3]


async def test_chunk_lock_allows_publishers_and_pullers(conn, settings, monkeypatch):
    await retained(conn, 20)
    live_id = await seed(conn)
    running = await claim(conn)
    entered, release = asyncio.Event(), asyncio.Event()
    execute = psycopg.AsyncConnection.execute
    updates = 0
    monkeypatch.setattr(feed_module, "_BACKFILL_CHUNK", 7)

    async def paused(c, query, *args, **kwargs):
        nonlocal updates
        if isinstance(query, str) and query.startswith("UPDATE fronta.subscriptions SET backfill"):
            updates += 1
            if updates == 2:
                entered.set()
                await release.wait()
        return await execute(c, query, *args, **kwargs)

    async with subscribe("workflow", settings=settings, states=[State.SUCCEEDED]) as feed:
        # Simulate a previously interrupted consumer while another feed is already open.
        await conn.execute(
            "UPDATE fronta.subscriptions SET backfill="
            '\'{"since":null,"pending":{"succeeded":0}}\'::jsonb'
        )
        monkeypatch.setattr(psycopg.AsyncConnection, "execute", paused)
        async with asyncio.TaskGroup() as tg:
            tg.create_task(register(settings))
            await asyncio.wait_for(entered.wait(), 3)
            try:
                assert await asyncio.wait_for(
                    store.complete(conn, [Completion(live_id, running.token, "succeed", "null")]), 1
                )
                batch = await asyncio.wait_for(anext(feed), 1)
                assert len(batch.events) == 8  # seven committed snapshots plus the live event
                assert live_id in {e.id for e in batch.events}
                await asyncio.wait_for(batch.ack(), 1)
            finally:
                release.set()
    assert len(await backlog(conn)) == 14  # remaining snapshots, including the concurrent live row


@pytest.mark.slow
@pytest.mark.parametrize("seed_value", range(50 if sys.platform == "linux" else 10))
async def test_creating_registration_has_no_gap_under_load(
    conn, settings, seed_value, record_property
):
    result = await feed_backfill_soak(conn, settings, seed_value)
    record_property("backfill_duplicates", result["duplicates"])
