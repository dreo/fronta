"""Adversarial registration timing against real PostgreSQL transaction locks."""

# ruff: noqa: PLR0913, PLR0917  # pytest fixtures and the isolation/visibility/timing matrix

import asyncio
import contextlib
import secrets

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from psycopg.conninfo import make_conninfo

from fronta import State, Worker, store, subscribe, unsubscribe
from fronta import feed as feed_module
from fronta.model import NewTask, Policy
from tests.conftest import MAINT_DSN, wait_until
from tests.test_feed import (
    backlog,
    pending_marker,
    pull,
    register,
    retained,
    snapshots,
    subscription_generation,
)
from tests.workers import In, sleep_task


async def scalar(conn, query, params=None):
    return (await (await conn.execute(query, params)).fetchone())[0]


@pytest_asyncio.fixture
async def plain_settings(conn, dsn, settings):
    """A consumer with only ordinary Fronta table grants, distinct from the producer role."""
    role = f"fronta_barrier_{secrets.token_hex(6)}"
    password = secrets.token_hex(16)
    ident = sql.Identifier(role)
    await conn.execute(
        sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(ident, sql.Literal(password))
    )
    try:
        for grant in (
            "GRANT USAGE ON SCHEMA fronta TO {}",
            "GRANT ALL ON ALL TABLES IN SCHEMA fronta TO {}",
            "GRANT USAGE ON ALL SEQUENCES IN SCHEMA fronta TO {}",
        ):
            await conn.execute(sql.SQL(grant).format(ident))
        assert not await scalar(
            conn, "SELECT pg_has_role(%s, 'pg_read_all_stats', 'MEMBER')", (role,)
        )
        yield settings.model_copy(update={"dsn": make_conninfo(dsn, user=role, password=password)})
    finally:
        await conn.execute(sql.SQL("DROP OWNED BY {}").format(ident))
        await conn.execute(sql.SQL("DROP ROLE {}").format(ident))


@pytest.fixture
def captures(monkeypatch):
    captured = []
    original = store.backfill_blockers

    async def observed(conn, transactions=None):
        rows = await original(conn, transactions)
        if transactions is None:
            captured.append((conn.info.backend_pid, rows))
        return rows

    monkeypatch.setattr(store, "backfill_blockers", observed)
    monkeypatch.setattr(feed_module, "_BACKFILL_POLL_S", 0.01)
    return captured


async def wait_captured(captures, count=1):
    async def ready():
        return len(captures) >= count

    await wait_until(ready)


@pytest.mark.parametrize("isolation", ["READ_COMMITTED", "REPEATABLE_READ"])
@pytest.mark.parametrize("tracking", [True, False])
@pytest.mark.parametrize("commit", [True, False])
async def test_old_snapshot_late_xid_then_idle_transaction(
    conn, dsn, plain_settings, captures, isolation, tracking, commit
):
    # Pause an enqueue after its statement snapshot but before its first permanent XID.
    # Prime the identity sequence: its first WAL reservation could otherwise assign an XID.
    await conn.execute("SELECT nextval('fronta.tasks_id_seq')")
    await conn.execute(
        "CREATE FUNCTION fronta.backfill_gate() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN PERFORM pg_advisory_xact_lock(730019); RETURN NEW; END $$; "
        "CREATE TRIGGER backfill_gate BEFORE INSERT ON fronta.tasks "
        "FOR EACH ROW EXECUTE FUNCTION fronta.backfill_gate()"
    )
    try:
        async with (
            asyncio.timeout(10),
            await psycopg.AsyncConnection.connect(dsn, autocommit=True) as gate,
            await psycopg.AsyncConnection.connect(dsn, autocommit=True) as writer,
            asyncio.TaskGroup() as tg,
        ):
            if not tracking:
                await writer.execute("SET track_activities=off")
            await writer.set_autocommit(False)
            await writer.set_isolation_level(psycopg.IsolationLevel[isolation])
            await gate.execute("SELECT pg_advisory_lock(730019)")
            writing = tg.create_task(store.enqueue(writer, NewTask("sleep", "{}", Policy())))

            async def gated():
                return await scalar(
                    conn,
                    "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=%s "
                    "AND locktype='advisory' AND NOT granted)",
                    (writer.info.backend_pid,),
                )

            await wait_until(gated)
            assert not await scalar(
                conn,
                "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=%s "
                "AND locktype='transactionid' AND granted)",
                (writer.info.backend_pid,),
            )
            creating = tg.create_task(
                register(plain_settings, states=[State.QUEUED], backfill=True)
            )
            await wait_captured(captures)
            assert writer.info.backend_pid in {r[1] for r in captures[0][1]}
            marker = await pending_marker(conn)
            await gate.execute("SELECT pg_advisory_unlock(730019)")
            task_id = await writing
            # The statement is now over and has an XID, but has not committed. Both the
            # captured-real-XID and active-statement proposals missed this exact state.
            assert await scalar(
                conn,
                "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=%s "
                "AND locktype='transactionid' AND granted)",
                (writer.info.backend_pid,),
            )
            await asyncio.sleep(0.05)
            assert not creating.done()
            assert await pending_marker(conn) == marker
            assert await backlog(conn) == []
            if commit:
                await writer.commit()
            else:
                await writer.rollback()
            await creating
        assert await backlog(conn) == ([(task_id, "queued", 0)] if commit else [])
        assert await pending_marker(conn) is None
    finally:
        await conn.execute("DROP TRIGGER backfill_gate ON fronta.tasks")
        await conn.execute("DROP FUNCTION fronta.backfill_gate()")


async def test_single_role_read_only_snapshot_then_enqueue(conn, plain_settings, captures):
    async with (
        asyncio.timeout(10),
        await psycopg.AsyncConnection.connect(plain_settings.dsn) as writer,
        asyncio.TaskGroup() as tg,
    ):
        await writer.set_isolation_level(psycopg.IsolationLevel.REPEATABLE_READ)
        assert await scalar(writer, "SELECT count(*) FROM fronta.subscriptions") == 0
        creating = tg.create_task(register(plain_settings, states=[State.QUEUED], backfill=True))
        await wait_captured(captures)
        assert writer.info.backend_pid in {r[1] for r in captures[0][1]}
        task_id = await store.enqueue(writer, NewTask("sleep", "{}", Policy()))
        assert not creating.done()
        assert await backlog(conn) == []
        await writer.commit()
        await creating
    assert await backlog(conn) == [(task_id, "queued", 0)]


@pytest.mark.parametrize("first_backfills", [True, False])
async def test_racing_plain_and_backfill_creators_keep_winners_marker(
    conn, dsn, settings, first_backfills
):
    history = await retained(conn, 3)
    async with (
        asyncio.timeout(10),
        await psycopg.AsyncConnection.connect(dsn) as first,
        asyncio.TaskGroup() as tg,
    ):
        await store.register_subscription(first, "workflow", ["succeeded"], None, first_backfills)
        second = tg.create_task(register(settings, backfill=not first_backfills))

        async def waiting():
            return await scalar(
                conn,
                "SELECT EXISTS(SELECT FROM pg_stat_activity "
                "WHERE datname=current_database() AND application_name='fronta-feed' "
                "AND wait_event_type='Lock')",
            )

        await wait_until(waiting)
        await first.commit()
        await second
    assert set(await backlog(conn)) == (snapshots(history) if first_backfills else set())
    assert await pending_marker(conn) is None


@pytest.mark.parametrize("before_capture", [True, False])
async def test_cancelled_barrier_resumes_with_independent_fixed_sets(
    conn, dsn, settings, captures, monkeypatch, before_capture
):
    async with (
        asyncio.timeout(10),
        await psycopg.AsyncConnection.connect(dsn) as old,
        await psycopg.AsyncConnection.connect(dsn) as later,
        asyncio.TaskGroup() as tg,
    ):
        old_id = await store.enqueue(old, NewTask("sleep", "{}", Policy()))
        entered = asyncio.Event()
        original = store.backfill_blockers

        async def interrupt(c, transactions=None):
            if not before_capture:
                await original(c, transactions)
            entered.set()
            await asyncio.Event().wait()

        with monkeypatch.context() as patch:
            patch.setattr(store, "backfill_blockers", interrupt)
            creating = tg.create_task(register(settings, states=[State.QUEUED], backfill=True))
            await entered.wait()
            marker = await pending_marker(conn)
            creating.cancel()
            with pytest.raises(asyncio.CancelledError):
                await creating
        # Both resumers must capture, including when the first consumer never got that far.
        captures.clear()
        consumers = [tg.create_task(register(settings, states=[State.QUEUED])) for _ in range(2)]
        await wait_captured(captures, 2)
        assert all(old.info.backend_pid in {r[1] for r in rows} for _, rows in captures)
        assert await pending_marker(conn) == marker
        later_id = await store.enqueue(later, NewTask("sleep", "{}", Policy()))
        await old.commit()
        await asyncio.gather(*consumers)
        # New transactions do not extend either wait; this one still has an uncommitted event.
        assert await backlog(conn) == [(old_id, "queued", 0)]
        assert await pending_marker(conn) is None
        await later.commit()
    assert set(await backlog(conn)) == {(old_id, "queued", 0), (later_id, "queued", 0)}


@pytest.mark.usefixtures("sdk")
@pytest.mark.parametrize("after_chunk", [False, True])
async def test_recreated_name_cannot_use_previous_generations_barrier(
    conn, dsn, settings, captures, monkeypatch, caplog, after_chunk
):
    caplog.set_level("INFO", logger="fronta.feed")
    history = await retained(conn, 1, states=("queued",))
    entered, release = asyncio.Event(), asyncio.Event()
    original = store.backfill_chunk
    paused = False
    monkeypatch.setattr(feed_module, "_BACKFILL_CHUNK", 1)

    async def pause(*args):
        nonlocal paused
        if paused:
            return await original(*args)
        paused = True
        result = await original(*args) if after_chunk else None
        entered.set()
        await release.wait()
        return result if after_chunk else await original(*args)

    monkeypatch.setattr(store, "backfill_chunk", pause)
    async with (
        asyncio.timeout(10),
        await psycopg.AsyncConnection.connect(dsn) as writer,
        asyncio.TaskGroup() as tg,
    ):
        old = tg.create_task(register(settings, states=[State.QUEUED], backfill=True))
        await entered.wait()
        old_generation = await subscription_generation(conn)
        await unsubscribe("workflow")
        task_id = await store.enqueue(writer, NewTask("sleep", "{}", Policy()))
        new = tg.create_task(register(settings, states=[State.QUEUED], backfill=True))
        await wait_captured(captures, 2)
        new_marker = await pending_marker(conn)
        assert await subscription_generation(conn) != old_generation
        release.set()
        await old
        assert not new.done()
        assert await pending_marker(conn) == new_marker
        assert await backlog(conn) == []
        assert "backfill stopped name=workflow: registration removed or replaced" in caplog.text
        await writer.commit()
        await new
    assert set(await backlog(conn)) == snapshots(history) | {(task_id, "queued", 0)}
    assert len(await backlog(conn)) == 2
    assert caplog.text.count("backfill finished name=workflow") == 1


@pytest.mark.usefixtures("sdk")
async def test_open_reaction_batch_does_not_stall_workers_or_other_feeds(
    conn, settings, run_worker, captures
):
    async with (
        asyncio.timeout(15),
        run_worker(Worker([sleep_task], settings=settings)),
        subscribe("reaction", settings=settings, states=[State.QUEUED], types=["source"]) as feed,
        subscribe("other", settings=settings) as other,
        asyncio.TaskGroup() as tg,
    ):
        await store.enqueue(conn, NewTask("source", "{}", Policy()))
        held = await pull(feed)
        creating = tg.create_task(register(settings, backfill=True))
        await wait_captured(captures)
        assert held.conn.info.backend_pid in {r[1] for r in captures[0][1]}
        assert not await scalar(
            conn,
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=%s "
            "AND relation='fronta.subscriptions'::regclass)",
            (captures[0][0],),
        )
        for _ in range(10):
            task_id = await sleep_task.enqueue(In())
            batch = await pull(other)
            assert task_id in {e.id for e in batch.events}
            await batch.ack()
            assert not creating.done()
        await held.ack()
        await creating
    assert len(await backlog(conn)) >= 10
    assert await pending_marker(conn) is None


@pytest.mark.parametrize("hidden", [False, True])
async def test_long_blocker_warning_is_diagnostic(
    conn, dsn, settings, plain_settings, captures, monkeypatch, caplog, hidden
):
    caplog.set_level("WARNING", logger="fronta.feed")
    monkeypatch.setattr(feed_module, "_BACKFILL_WARN_S", 0.02)
    async with (
        asyncio.timeout(10),
        await psycopg.AsyncConnection.connect(
            dsn, autocommit=True, application_name="backfill-blocker"
        ) as writer,
        asyncio.TaskGroup() as tg,
    ):
        if hidden:
            await writer.execute("SET track_activities=off")
        await writer.set_autocommit(False)
        await store.enqueue(writer, NewTask("sleep", "{}", Policy()))
        creating = tg.create_task(register(plain_settings if hidden else settings, backfill=True))
        await wait_captured(captures)

        async def warned():
            return "backfill waiting" in caplog.text

        await wait_until(warned)
        assert f"pid={writer.info.backend_pid}" in caplog.text
        assert "user=" in caplog.text
        assert "application=" in caplog.text
        assert "transaction_age=" in caplog.text
        assert "this process's own batches" in caplog.text
        if not hidden:
            assert "application=backfill-blocker" in caplog.text
            assert "transaction_age=None" not in caplog.text
        assert not creating.done()
        await writer.rollback()
        await creating
    assert await backlog(conn) == []


async def test_other_database_transactions_do_not_delay_backfill(conn, settings, captures):
    async with await psycopg.AsyncConnection.connect(MAINT_DSN) as elsewhere:
        await elsewhere.execute("SELECT 1")
        rows = await retained(conn, 2)
        await asyncio.wait_for(register(settings, backfill=True), 3)
        assert elsewhere.info.backend_pid not in {r[1] for r in captures[0][1]}
        assert set(await backlog(conn)) == snapshots(rows)


async def test_disconnected_blocker_releases_barrier(conn, dsn, settings, captures):
    async with asyncio.timeout(10), contextlib.AsyncExitStack() as stack:
        writer = await stack.enter_async_context(await psycopg.AsyncConnection.connect(dsn))
        await store.enqueue(writer, NewTask("sleep", "{}", Policy()))
        async with asyncio.TaskGroup() as tg:
            creating = tg.create_task(register(settings, states=[State.QUEUED], backfill=True))
            await wait_captured(captures)
            await writer.close()
            await creating
        assert await backlog(conn) == []
        assert await pending_marker(conn) is None
