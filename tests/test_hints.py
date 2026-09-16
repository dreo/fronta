"""Hints coalesce, survive failures and leave task correctness to rows and polling."""

import asyncio

import psycopg
import pytest

from fronta import Worker, runtime, subscribe, task
from fronta.hints import Hints
from tests.conftest import wait_until
from tests.workers import In


async def test_thousand_identical_wakes_coalesce_and_connection_is_reused(settings, monkeypatch):
    sent, connections = [], []

    async def send(_hints, conn, notices):
        sent.append(notices)
        connections.append(conn)

    monkeypatch.setattr(Hints, "_send", send)
    hints = runtime.hints(settings)
    for _ in range(1000):
        hints.wake(["sleep"])

    async def sent_once():
        return len(sent) == 1

    await wait_until(sent_once)
    assert sent == [[("fronta_wake", "sleep")]]
    hints.cancel([42, 42])
    hints.feed(["succeeded"])

    async def sent_twice():
        return len(sent) == 2

    await wait_until(sent_twice)
    assert sorted(sent[1]) == [("fronta_cancel", "42"), ("fronta_feed", "succeeded")]
    assert connections[0] is connections[1]
    await runtime.close_hints()


async def test_failed_flush_keeps_all_hints_for_retry(settings, monkeypatch):
    sent = []

    async def send(_hints, _conn, notices):
        sent.append(notices)
        if len(sent) == 1:
            raise psycopg.OperationalError("outage")

    monkeypatch.setattr(Hints, "_send", send)
    hints = runtime.hints(settings)
    hints.wake(["sleep"])
    hints.cancel([42])
    hints.feed(["succeeded"])

    async def retried():
        return len(sent) == 2

    await wait_until(retried)
    assert sorted(sent[0]) == sorted(sent[1])
    await runtime.close_hints()


async def test_immediate_shutdown_cancels_a_stuck_flush(settings, monkeypatch):
    started = asyncio.Event()

    async def send(*_args):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(Hints, "_send", send)
    hints = runtime.hints(settings)
    hints.wake(["sleep"])
    await started.wait()
    hints.cancel([123])
    await asyncio.wait_for(runtime.close_hints(immediate=True), 1)
    assert not hints._cancel
    assert not hints._wake
    assert hints._task.done()


async def test_feed_hints_only_wake_subscribers_to_the_changed_states(settings):
    async with (
        subscribe("terminal", settings=settings),
        await psycopg.AsyncConnection.connect(settings.dsn, autocommit=True) as listener,
    ):
        await listener.execute("LISTEN fronta_feed")
        await listener.execute("LISTEN fronta_wake")
        hints = runtime.hints(settings)
        hints.feed(["queued", "running"])
        hints.wake(["sleep"])
        await runtime.close_hints()
        notices = [n.channel async for n in listener.notifies(timeout=0.1)]
        assert notices == ["fronta_wake"]
        runtime.hints(settings).feed(["succeeded"])
        await runtime.close_hints()
        notices = [n.channel async for n in listener.notifies(timeout=0.1)]
        assert notices == ["fronta_feed"]


@pytest.mark.usefixtures("sdk")
async def test_local_child_wake_works_with_hints_broken(conn, settings, run_worker, monkeypatch):
    async def send(*_args):
        raise psycopg.OperationalError("hints unavailable")

    monkeypatch.setattr(Hints, "_send", send)
    enqueue_child, child_started, finish_parent, idle = (asyncio.Event() for _ in range(4))

    @task("chain", input=In)
    async def chain(ctx, inp):
        if inp.n == 0:
            await enqueue_child.wait()
            await ctx.enqueue(chain, In(n=1))
            await finish_parent.wait()  # parent completion must not be the source of the wake
        else:
            child_started.set()

    await chain.enqueue(In(n=0))
    worker = Worker([chain], settings=settings.model_copy(update={"poll_interval_s": 5}))
    original_wait = worker._wake.wait
    waits = 0

    async def waiting():
        nonlocal waits
        waits += 1
        if waits == 6:  # the sixth idle wait has backed off to at least 1.28 seconds
            idle.set()
        return await original_wait()

    monkeypatch.setattr(worker._wake, "wait", waiting)
    async with run_worker(worker):
        await asyncio.wait_for(idle.wait(), 5)
        enqueue_child.set()
        try:
            await asyncio.wait_for(child_started.wait(), 1)
        finally:
            finish_parent.set()
    rows = await (
        await conn.execute("SELECT count(*) FROM fronta.tasks WHERE state='succeeded'")
    ).fetchone()
    assert rows[0] == 2


async def test_supplied_autocommit_connection_hints_use_its_password_and_share_the_emitter(
    conn, settings
):
    hints = runtime.hints(settings, conn=conn)
    assert hints is runtime.hints(settings)
    async with await psycopg.AsyncConnection.connect(settings.dsn, autocommit=True) as listener:
        await listener.execute("LISTEN fronta_wake")
        hints.wake(["caller-auth"])
        notices = [n.payload async for n in listener.notifies(timeout=1, stop_after=1)]
    assert notices == ["caller-auth"]
