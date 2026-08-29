"""Crash recovery: reaping, fencing, lost leases, listener reconnection."""

from __future__ import annotations

import asyncio
import contextlib
import signal
import time

import pytest

from fronta import Settings, State, Worker, store, task
from tests.conftest import FAST, spawn_worker, wait_until, worker_env
from tests.workers import In, sleep_task


async def get(conn, task_id):
    row = await store.get_task(conn, task_id)
    assert row is not None
    return row


async def running_on(conn, task_id, worker_prefix):
    row = await get(conn, task_id)
    return row.state is State.RUNNING and (row.worker or "").startswith(worker_prefix)


@pytest.fixture
def subprocess_worker(settings):
    procs = []

    def start(target="tests.subworkers:crash_worker"):
        proc = spawn_worker(target, worker_env(settings))
        procs.append(proc)
        return proc

    yield start
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)


@pytest.mark.usefixtures("sdk")
async def test_sigkilled_worker_is_reaped_and_its_task_rerun(
    conn, settings, run_worker, subprocess_worker
):
    await store.publish_task_type(conn, sleep_task.spec)
    task_id = await sleep_task.enqueue(In(n=3, sleep_s=3))
    victim = subprocess_worker()
    await wait_until(lambda: running_on(conn, task_id, ""), timeout=20)
    claimed = await get(conn, task_id)
    assert claimed.attempt == 1
    killed_at = time.monotonic()
    victim.kill()
    victim.wait(timeout=10)
    async with run_worker(Worker([sleep_task], settings=settings)):
        await wait_until(
            lambda: _reaped(conn, task_id),
            timeout=settings.lease_s + settings.reaper_interval_s + 5,
        )
        reaped = await get(conn, task_id)
        assert time.monotonic() - killed_at <= settings.lease_s + settings.reaper_interval_s + 3
        assert reaped.failures == 1
        assert reaped.error["type"] == "LeaseExpired"
        assert claimed.worker in reaped.error["message"]
        # ... and the fresh worker cannot run it before the retry delay, then runs it.
        await wait_until(lambda: _state(conn, task_id, State.SUCCEEDED), timeout=30)
    final = await get(conn, task_id)
    assert final.attempt == 2
    assert final.failures == 1
    assert final.result["n"] == 3
    assert final.worker != claimed.worker


@pytest.mark.usefixtures("sdk")
async def test_fencing_rejects_the_stale_completion_of_a_stopped_worker(
    conn, settings, run_worker, subprocess_worker
):
    await store.publish_task_type(conn, sleep_task.spec)
    task_id = await sleep_task.enqueue(In(n=1, sleep_s=3))
    stale = subprocess_worker()
    await wait_until(lambda: running_on(conn, task_id, ""), timeout=20)
    first = await get(conn, task_id)
    stale.send_signal(signal.SIGSTOP)
    async with run_worker(Worker([sleep_task], settings=settings)) as fresh:
        await wait_until(
            lambda: _reaped(conn, task_id),
            timeout=settings.lease_s + settings.reaper_interval_s + 5,
        )
        await wait_until(lambda: _state(conn, task_id, State.SUCCEEDED), timeout=30)
        second = await get(conn, task_id)
        assert second.attempt == 2
        assert second.worker == fresh.worker_id
        stale.send_signal(signal.SIGCONT)
        await asyncio.sleep(2.0)  # the stale worker wakes up and tries to complete attempt 1
        stale.send_signal(signal.SIGTERM)
        stale.wait(timeout=15)
    final = await get(conn, task_id)
    assert final.state is State.SUCCEEDED
    assert final.attempt == 2
    assert final.worker == fresh.worker_id
    assert final.result["started"] == second.result["started"]  # the second worker's result stands
    stderr = stale.stderr.read().decode()
    assert "token no longer valid" in stderr or "lease lost" in stderr
    assert first.worker != fresh.worker_id


@pytest.mark.usefixtures("sdk")
async def test_worker_whose_heartbeat_is_rejected_stops_its_task(conn, settings, run_worker):
    async with run_worker(Worker([sleep_task], settings=settings)) as worker:
        task_id = await sleep_task.enqueue(In(sleep_s=60))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        assert task_id in worker.attempts
        # Someone else owns the row now (a new token): this worker's writes must all fail.
        await conn.execute(
            "UPDATE fronta.tasks SET token = gen_random_uuid() WHERE id = %s", (task_id,)
        )
        await wait_until(
            lambda: _gone(worker, task_id), timeout=settings.heartbeat_s + settings.grace_s + 5
        )
    row = await get(conn, task_id)
    assert row.state is State.RUNNING  # untouched: the outcome was discarded, not written
    assert row.attempt == 1


@pytest.mark.usefixtures("sdk")
async def test_listener_reconnects_after_its_backend_is_terminated(conn, dsn, run_worker):
    slow_beats = Settings(dsn=dsn, **{**FAST, "heartbeat_s": 30.0, "lease_s": 60.0})
    async with run_worker(Worker([sleep_task], settings=slow_beats)):
        await wait_until(lambda: _listening(conn), timeout=10)
        old = await _listener_pids(conn)
        await conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE datname = current_database() AND query LIKE 'LISTEN%'"
        )
        # A terminated backend can linger in pg_stat_activity: wait for a *new* listener.
        await wait_until(lambda: _fresh_listener(conn, old), timeout=10)
        task_id = await sleep_task.enqueue(In(sleep_s=60))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING), timeout=10)
        await store.request_cancel(conn, task_id)
        # Only NOTIFY can deliver this in time (heartbeats are 30 s apart).
        await wait_until(lambda: _state(conn, task_id, State.CANCELLED), timeout=5)


async def _listener_pids(conn) -> set[int]:
    cur = await conn.execute(
        "SELECT pid FROM pg_stat_activity WHERE datname = current_database()"
        " AND query LIKE 'LISTEN%' AND state = 'idle'"
    )
    return {row[0] for row in await cur.fetchall()}


async def _listening(conn):
    return bool(await _listener_pids(conn))


async def _fresh_listener(conn, old: set[int]):
    return bool(await _listener_pids(conn) - old)


async def _reaped(conn, task_id):
    return (await get(conn, task_id)).failures >= 1


async def _state(conn, task_id, state):
    return (await get(conn, task_id)).state is state


async def _gone(worker, task_id):
    return task_id not in worker.attempts


@pytest.mark.usefixtures("sdk")
async def test_heartbeats_end_when_the_attempt_settles_even_if_a_cancellation_is_lost(
    conn, dsn, run_worker, monkeypatch
):
    """psycopg-pool < 3.2.8 could swallow the controller's cancellation inside `connection()`;
    the heartbeat loop must still end by itself, or a finished attempt keeps beating (and the
    worker cannot stop) until the pool closes."""
    real_heartbeat = store.heartbeat

    async def heartbeat_that_loses_cancellation(conn, task_id, token, lease_s):
        with contextlib.suppress(asyncio.CancelledError):  # the controller's cancel lands
            await asyncio.sleep(30)  # here and is lost, as in the buggy library
        return await real_heartbeat(conn, task_id, token, lease_s)

    monkeypatch.setattr(store, "heartbeat", heartbeat_that_loses_cancellation)
    quick = Settings(dsn=dsn, **{**FAST, "heartbeat_s": 0.05, "lease_s": 60.0})
    started = time.monotonic()
    async with run_worker(Worker([sleep_task], settings=quick)):
        task_id = await sleep_task.enqueue(In(sleep_s=0.2))
        await wait_until(lambda: _state(conn, task_id, State.SUCCEEDED), timeout=10)
    assert time.monotonic() - started < 10, "the worker could not stop: heartbeats kept running"


@task("slow_to_stop", input=In, attempt_timeout=30)
async def slow_to_stop_task(_ctx, inp):
    """Winds down for half a second after cancellation, well within the grace period."""
    try:
        await asyncio.sleep(inp.sleep_s)
    except asyncio.CancelledError:
        await asyncio.sleep(0.5)
        raise


@pytest.mark.usefixtures("sdk")
async def test_a_lost_lease_stops_the_heartbeats(conn, dsn, run_worker, monkeypatch):
    """After the store reports the lease lost, the attempt is stopped and never beaten again,
    however long its handler takes to wind down."""
    calls = 0

    async def heartbeat_lost(*_args):
        nonlocal calls
        calls += 1
        return store.Heartbeat.LOST

    monkeypatch.setattr(store, "heartbeat", heartbeat_lost)
    quick = Settings(dsn=dsn, **{**FAST, "heartbeat_s": 0.05, "lease_s": 60.0})
    async with run_worker(Worker([slow_to_stop_task], settings=quick)):
        task_id = await slow_to_stop_task.enqueue(In(sleep_s=30))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING), timeout=10)
        await wait_until(lambda: _beats_at_least(lambda: calls, 1), timeout=10)
        await asyncio.sleep(1.0)  # the handler winds down; twenty intervals pass
    assert calls == 1


async def _beats_at_least(count, n):
    return count() >= n
