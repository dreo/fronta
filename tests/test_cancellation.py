"""Cancelling `Worker.run()` (and losing a background loop) still owns every attempt: each is
settled through the shutdown protocol or explicitly abandoned before the pools close, and no
runner, heartbeat, listener, reaper, purger, tick task or watchdog thread outlives the worker."""

from __future__ import annotations

import asyncio
import sys
import threading

import psycopg
import pytest

from fronta import State, Worker, sandbox, store
from fronta import worker as worker_module
from fronta.model import NewTask
from fronta.worker import EXIT_LOOP_FAILED
from tests.conftest import leftover_sandboxes, wait_until
from tests.workers import In, long_proc, sleep_task

requires_linux = pytest.mark.skipif(
    sys.platform != "linux", reason="sandbox process management requires Linux"
)
WORKER_TASKS = ("attempt-", "heartbeat-", "run-", "stop-", "tick", "listener", "reaper", "purger")


def live_worker_tasks() -> list[str]:
    return [t.get_name() for t in asyncio.all_tasks() if t.get_name().startswith(WORKER_TASKS)]


async def _row(conn, task_id):
    row = await store.get_task(conn, task_id)
    assert row is not None
    return row


async def _state(conn, task_id, state):
    return (await _row(conn, task_id)).state is state


async def _threads_back_to(count):
    return threading.active_count() <= count


async def _started(worker: Worker) -> asyncio.Task[int]:
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.started.wait(), 15)
    return run


async def _cancelled(run: asyncio.Task[int], timeout: float) -> None:
    run.cancel()
    await asyncio.wait({run}, timeout=timeout)
    assert run.done()
    assert run.cancelled()


@pytest.mark.usefixtures("sdk")
async def test_cancelling_run_settles_a_running_attempt_before_the_pools_close(conn, settings):
    threads = threading.active_count()
    worker = Worker([sleep_task], settings=settings)
    run = await _started(worker)
    task_id = await sleep_task.enqueue(In(sleep_s=60))
    await wait_until(lambda: _state(conn, task_id, State.RUNNING))
    await _cancelled(run, timeout=30)
    row = await _row(conn, task_id)
    assert row.state is State.QUEUED  # released through the shutdown protocol, without a charge
    assert row.failures == 0
    assert row.token is None
    assert worker.attempts == {}
    assert live_worker_tasks() == []
    assert worker._pool is None
    await wait_until(lambda: _threads_back_to(threads), timeout=5)


@pytest.mark.usefixtures("sdk")
async def test_cancelling_run_during_a_blocked_claim_leaves_nothing_behind(settings, monkeypatch):
    gate = asyncio.Event()

    async def blocked(*_args, **_kwargs):
        await gate.wait()

    monkeypatch.setattr(store, "claim", blocked)
    worker = Worker([sleep_task], settings=settings)
    run = await _started(worker)
    await asyncio.sleep(0.3)
    await _cancelled(run, timeout=30)
    assert live_worker_tasks() == []
    assert worker.attempts == {}


@pytest.mark.usefixtures("sdk")
async def test_cancelling_run_abandons_a_transition_stuck_on_an_outage(
    conn, settings, monkeypatch, caplog
):
    async def down(*_args, **_kwargs):
        msg = "simulated outage"
        raise psycopg.OperationalError(msg)

    monkeypatch.setattr(store, "succeed", down)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_S", 0.1)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_MAX_S", 0.1)
    worker = Worker([sleep_task], settings=settings)
    run = await _started(worker)
    task_id = await sleep_task.enqueue(In(sleep_s=0.2))
    await wait_until(lambda: _state(conn, task_id, State.RUNNING))
    await asyncio.sleep(1.0)  # the attempt is retrying its final write
    with caplog.at_level("ERROR", logger="fronta.worker"):
        await _cancelled(run, timeout=60)
    assert (await _row(conn, task_id)).state is State.RUNNING  # explicitly left to the reaper
    assert any("abandoned unsettled attempt" in r.message for r in caplog.records)
    assert live_worker_tasks() == []


@pytest.mark.usefixtures("sdk")
async def test_a_repeated_cancellation_still_ends_every_task(conn, settings, monkeypatch):
    async def down(*_args, **_kwargs):
        msg = "simulated outage"
        raise psycopg.OperationalError(msg)

    monkeypatch.setattr(store, "release", down)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_S", 0.1)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_MAX_S", 0.1)
    worker = Worker([sleep_task], settings=settings)
    run = await _started(worker)
    task_id = await sleep_task.enqueue(In(sleep_s=60))
    await wait_until(lambda: _state(conn, task_id, State.RUNNING))
    run.cancel()
    await asyncio.sleep(0.5)  # the first cancellation is settling (the release keeps failing)
    run.cancel()  # the second one cuts the settlement short
    await asyncio.wait({run}, timeout=30)
    assert run.done()
    assert run.cancelled()
    assert live_worker_tasks() == []
    assert worker.attempts == {}
    assert (await _row(conn, task_id)).state is State.RUNNING  # left to the reaper


@pytest.mark.usefixtures("sdk")
async def test_a_failed_background_loop_shuts_the_worker_down_in_order(
    conn, settings, monkeypatch, caplog
):
    original = store.reap
    fail_now = asyncio.Event()

    async def reap(conn_, limit=100):
        if not fail_now.is_set():
            return await original(conn_, limit)
        msg = "simulated reaper bug"
        raise RuntimeError(msg)

    monkeypatch.setattr(store, "reap", reap)
    worker = Worker([sleep_task], settings=settings)
    run = await _started(worker)
    task_id = await sleep_task.enqueue(In(sleep_s=60))
    await wait_until(lambda: _state(conn, task_id, State.RUNNING))
    with caplog.at_level("CRITICAL", logger="fronta.worker"):
        fail_now.set()
        exit_code = await asyncio.wait_for(run, 30)
    assert exit_code == EXIT_LOOP_FAILED
    assert any("background loop reaper died" in r.message for r in caplog.records)
    row = await _row(conn, task_id)
    assert row.state is State.QUEUED  # the graceful shutdown released it
    assert row.failures == 0
    assert live_worker_tasks() == []


@requires_linux
async def test_cancelling_run_kills_a_running_sandbox_and_releases_its_row(conn, settings):
    worker = Worker([long_proc], settings=settings)
    run = await _started(worker)
    task_id = await store.enqueue(conn, NewTask("long_proc", "{}", long_proc.policy))
    await wait_until(lambda: _state(conn, task_id, State.RUNNING))
    await wait_until(lambda: _marked(task_id), timeout=10)
    await _cancelled(run, timeout=60)
    row = await _row(conn, task_id)
    assert row.state is State.QUEUED
    assert row.failures == 0
    assert sandbox.find_marked("FRONTA_TASK_ID", str(task_id)) == []
    assert leftover_sandboxes() == []
    assert live_worker_tasks() == []


async def _marked(task_id):
    return bool(sandbox.find_marked("FRONTA_TASK_ID", str(task_id)))
