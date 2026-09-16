"""Transactional feed delivery, rollback, filtering and competing consumers."""

import asyncio

import psycopg
import pytest

from fronta import State, TaskNotFound, Worker, get_task, store, subscribe, unsubscribe
from fronta.model import Completion, NewTask, Policy
from tests.conftest import wait_until
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
