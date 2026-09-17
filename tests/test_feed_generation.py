"""A feed belongs to one registration for its entire lifetime, even across name reuse."""

import asyncio
import contextlib

import psycopg
import pytest

from fronta import State, store, subscribe, unsubscribe
from fronta import feed as feed_module
from fronta.model import NewTask, Policy
from tests.test_feed import (
    backlog,
    pending_marker,
    pull,
    register,
    retained,
    subscription_generation,
)

pytestmark = pytest.mark.usefixtures("sdk")


async def replacement(conn, settings, mode):
    await unsubscribe("workflow")
    await store.register_subscription(conn, "workflow", ["queued"], ["replacement"], mode != "off")
    if mode == "complete":
        await register(settings, states=[State.QUEUED], types=["replacement"])
    return await store.enqueue(conn, NewTask("replacement", "{}", Policy()))


@pytest.mark.parametrize("backfill", [False, True])
@pytest.mark.parametrize("mode", ["off", "pending", "complete"])
async def test_open_feed_never_attaches_to_a_recreated_name(conn, settings, backfill, mode):
    async with subscribe(
        "workflow", settings=settings, states=[State.QUEUED], backfill=backfill
    ) as old:
        first = await store.enqueue(conn, NewTask("original", "{}", Policy()))
        batch = await pull(old)
        assert [e.id for e in batch.events] == [first]
        await batch.ack()
        generation = await subscription_generation(conn)
        task_id = await replacement(conn, settings, mode)
        assert await subscription_generation(conn) != generation
        for _ in range(2):
            with pytest.raises(StopAsyncIteration):
                await pull(old)
        assert await backlog(conn) == [(task_id, "queued", 0)]
        async with subscribe(
            "workflow", settings=settings, states=[State.QUEUED], types=["replacement"]
        ) as new:
            batch = await pull(new)
            assert {e.id for e in batch.events} == {task_id}
            await batch.ack()
        assert await backlog(conn) == []


async def test_exhausted_feed_stays_closed_after_name_reuse(conn, settings):
    async with subscribe("workflow", settings=settings, states=[State.QUEUED]) as old:
        await unsubscribe("workflow")
        with pytest.raises(StopAsyncIteration):
            await pull(old)
        task_id = await replacement(conn, settings, "off")
        with pytest.raises(StopAsyncIteration):
            await pull(old)
        assert await backlog(conn) == [(task_id, "queued", 0)]


async def test_filter_updates_and_backfill_completion_preserve_generation(conn, settings):
    first = await store.register_subscription(conn, "workflow", ["queued"], None, True)
    assert first["backfill"] is not None
    async with subscribe("workflow", settings=settings, states=[State.QUEUED]) as feed:
        assert await pending_marker(conn) is None
        await register(settings, states=[State.QUEUED], types=["updated"])
        assert await subscription_generation(conn) == first["generation"]
        task_id = await store.enqueue(conn, NewTask("updated", "{}", Policy()))
        batch = await pull(feed)
        assert [e.id for e in batch.events] == [task_id]
        await batch.ack()


@pytest.mark.parametrize("timing", ["before-chunk", "between-chunks", "after-final-chunk"])
@pytest.mark.parametrize("mode", ["off", "pending", "complete"])
async def test_replacement_during_startup_never_yields_replacement_events(
    conn, settings, monkeypatch, timing, mode
):
    await retained(conn, 2, states=("queued",))
    monkeypatch.setattr(feed_module, "_BACKFILL_CHUNK", 1 if timing == "between-chunks" else 10)
    entered, release = asyncio.Event(), asyncio.Event()
    original = store.backfill_chunk
    paused = False

    async def pause(*args):
        nonlocal paused
        if paused:
            return await original(*args)
        paused = True
        result = await original(*args) if timing != "before-chunk" else None
        entered.set()
        await release.wait()
        return result if timing != "before-chunk" else await original(*args)

    monkeypatch.setattr(store, "backfill_chunk", pause)
    async with (
        asyncio.timeout(10),
        contextlib.AsyncExitStack() as stack,
        asyncio.TaskGroup() as tg,
    ):
        creating = tg.create_task(
            stack.enter_async_context(
                subscribe("workflow", settings=settings, states=[State.QUEUED], backfill=True)
            )
        )
        await entered.wait()
        try:
            old_generation = await subscription_generation(conn)
            task_id = await replacement(conn, settings, mode)
            assert await subscription_generation(conn) != old_generation
        finally:
            release.set()
        old = await creating
        for _ in range(2):
            with pytest.raises(StopAsyncIteration):
                await pull(old)
        assert await backlog(conn) == [(task_id, "queued", 0)]
        assert (await pending_marker(conn) is not None) == (mode == "pending")


@pytest.mark.parametrize("recreate", [False, True])
async def test_removal_during_barrier_ends_old_feed_before_blocker_finishes(
    conn, dsn, settings, monkeypatch, recreate
):
    captured = asyncio.Event()
    original = store.backfill_blockers

    async def observe(*args):
        rows = await original(*args)
        captured.set()
        return rows

    monkeypatch.setattr(store, "backfill_blockers", observe)
    monkeypatch.setattr(feed_module, "_BACKFILL_POLL_S", 0.01)
    async with (
        asyncio.timeout(10),
        await psycopg.AsyncConnection.connect(dsn) as blocker,
        contextlib.AsyncExitStack() as stack,
        asyncio.TaskGroup() as tg,
    ):
        await blocker.execute("SELECT 1")
        creating = tg.create_task(
            stack.enter_async_context(
                subscribe("workflow", settings=settings, states=[State.QUEUED], backfill=True)
            )
        )
        await captured.wait()
        if recreate:
            task_id = await replacement(conn, settings, "pending")
        else:
            await unsubscribe("workflow")
        old = await asyncio.wait_for(creating, 2)
        with pytest.raises(StopAsyncIteration):
            await pull(old)
        if recreate:
            assert await backlog(conn) == [(task_id, "queued", 0)]
            assert await pending_marker(conn) is not None
        await blocker.rollback()
