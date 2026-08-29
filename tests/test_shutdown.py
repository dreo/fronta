"""Shutdown: graceful signals, immediate second signal, fatal handler, blocked event loop."""

from __future__ import annotations

import asyncio
import signal
import threading
import time
from typing import Any

import psycopg
import pytest

from fronta import Sandbox, State, Worker, process_task, sandbox, store, task
from fronta import worker as worker_module
from fronta.model import NewTask
from fronta.worker import EXIT_FATAL
from tests.conftest import leftover_sandboxes, spawn_worker, wait_until, worker_env
from tests.workers import In, blocker_task, long_proc, sleep_task, stubborn_task


async def get(conn, task_id):
    row = await store.get_task(conn, task_id)
    assert row is not None
    return row


async def is_running(conn, task_id):
    return (await get(conn, task_id)).state is State.RUNNING


@pytest.fixture
def subprocess_worker(settings):
    procs = []

    def start(target, **env):
        proc = spawn_worker(target, worker_env(settings, **env))
        procs.append(proc)
        return proc

    yield start
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
@pytest.mark.usefixtures("sdk")
async def test_graceful_signal_finishes_quick_attempts_and_releases_the_rest(
    conn, settings, subprocess_worker, sig
):
    await store.publish_task_type(conn, sleep_task.spec)
    quick = await sleep_task.enqueue(In(n=1, sleep_s=0.5))
    slow = await sleep_task.enqueue(In(n=2, sleep_s=60))
    proc = subprocess_worker("tests.subworkers:crash_worker")
    await wait_until(lambda: is_running(conn, slow), timeout=20)
    await wait_until(lambda: is_running(conn, quick), timeout=20)
    proc.send_signal(sig)
    assert proc.wait(timeout=settings.grace_s + 20) == 0
    finished = await get(conn, quick)
    assert finished.state is State.SUCCEEDED
    released = await get(conn, slow)
    assert released.state is State.QUEUED
    assert released.failures == 0
    assert released.attempt == 1
    assert released.token is None
    assert released.error is None
    cur = await conn.execute("SELECT run_at <= now() FROM fronta.tasks WHERE id = %s", (slow,))
    assert (await cur.fetchone())[0] is True
    # Nothing was claimed after the signal: the released task stays queued.
    time.sleep(1.0)
    assert (await get(conn, slow)).state is State.QUEUED


@pytest.mark.usefixtures("settings", "sdk")
async def test_second_signal_skips_the_grace_wait(conn, subprocess_worker):
    await store.publish_task_type(conn, sleep_task.spec)
    slow = await sleep_task.enqueue(In(sleep_s=60))
    proc = subprocess_worker("tests.subworkers:crash_worker", FRONTA_GRACE_S="30")
    await wait_until(lambda: is_running(conn, slow), timeout=20)
    started = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    time.sleep(0.5)  # back-to-back signals of the same number coalesce in the kernel
    proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=20) == 0
    assert time.monotonic() - started < 15
    assert (await get(conn, slow)).state is State.QUEUED


@pytest.mark.usefixtures("sdk")
async def test_in_process_shutdown_releases_running_attempts(conn, settings, run_worker):
    async with run_worker(Worker([sleep_task], settings=settings)):
        slow = await sleep_task.enqueue(In(sleep_s=60))
        await wait_until(lambda: is_running(conn, slow))
    released = await get(conn, slow)
    assert released.state is State.QUEUED
    assert released.failures == 0


@pytest.mark.usefixtures("settings", "sdk")
async def test_handler_ignoring_cancellation_is_fatal_after_fencing_and_releasing(
    conn, subprocess_worker
):
    await store.publish_task_type(conn, stubborn_task.spec)
    await store.publish_task_type(conn, sleep_task.spec)
    stubborn = await stubborn_task.enqueue(In())
    bystander = await sleep_task.enqueue(In(sleep_s=60))
    proc = subprocess_worker("tests.subworkers:fatal_worker")
    await wait_until(lambda: is_running(conn, stubborn), timeout=20)
    await wait_until(lambda: is_running(conn, bystander), timeout=20)
    # attempt timeout 1 s + grace 1 s + kill timeout 2 s, then the worker must exit 70.
    assert proc.wait(timeout=60) == EXIT_FATAL
    fenced = await get(conn, stubborn)
    assert fenced.state is State.FAILED  # max_attempts 1: the timeout was recorded
    assert fenced.error["type"] == "AttemptTimeout"
    released = await get(conn, bystander)
    assert released.state is State.QUEUED
    assert released.failures == 0
    stderr = proc.stderr.read().decode()
    assert "ignored cancellation" in stderr


@pytest.mark.usefixtures("sdk")
async def test_blocked_event_loop_trips_the_watchdog(conn, settings, subprocess_worker):
    await store.publish_task_type(conn, blocker_task.spec)
    blocker = await blocker_task.enqueue(In())
    proc = subprocess_worker("tests.subworkers:blocker_worker")
    await wait_until(lambda: is_running(conn, blocker), timeout=20)
    started = time.monotonic()
    assert proc.wait(timeout=settings.lease_s + 15) == EXIT_FATAL
    assert time.monotonic() - started < settings.lease_s + 10
    stderr = proc.stderr.read().decode()
    assert "event loop blocked" in stderr
    # The attempt is left to the reaper of the next worker: nothing heartbeats it anymore.
    row = await get(conn, blocker)
    assert row.state is State.RUNNING
    cur = await conn.execute(
        "SELECT lease_until < now() + interval '2 s' FROM fronta.tasks WHERE id = %s", (blocker,)
    )
    assert (await cur.fetchone())[0] is True


@pytest.mark.usefixtures("sdk")
async def test_a_claim_that_lands_after_the_shutdown_signal_is_released_not_started(
    conn, settings, run_worker, monkeypatch
):
    entered: list[int] = []

    @task("entering", input=In, attempt_timeout=30)
    async def entering(ctx: Any, inp: In) -> None:
        del inp
        entered.append(ctx.task_id)
        await asyncio.sleep(60)

    original = store.claim
    holder: dict[str, Worker] = {}

    async def claim_then_stop(*args, **kwargs):
        row = await original(*args, **kwargs)
        if row is not None:
            holder["worker"].request_shutdown()  # the signal lands while the claim is in flight
        return row

    monkeypatch.setattr(store, "claim", claim_then_stop)
    worker = Worker([entering], settings=settings)
    holder["worker"] = worker
    async with run_worker(worker):
        task_id = await entering.enqueue(In())
        await wait_until(lambda: _released(conn, task_id), timeout=10)
    row = await get(conn, task_id)
    assert row.state is State.QUEUED
    assert row.attempt == 1  # it was claimed ...
    assert row.failures == 0  # ... and given back without a charge
    assert row.token is None
    assert entered == []  # ... and the handler never ran


@pytest.mark.usefixtures("sdk")
async def test_graceful_shutdown_waits_for_an_unresolved_final_transition(
    conn, settings, monkeypatch
):
    """The worker does not exit while a fenced transition still lacks a definitive answer."""
    original = store.succeed
    gate = {"open": False, "calls": 0}

    async def gated(*args, **kwargs):
        gate["calls"] += 1
        if not gate["open"]:
            msg = "simulated outage"
            raise psycopg.OperationalError(msg)
        return await original(*args, **kwargs)

    monkeypatch.setattr(store, "succeed", gated)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_S", 0.1)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_MAX_S", 0.1)
    worker = Worker([sleep_task], settings=settings)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.started.wait(), 15)
    task_id = await sleep_task.enqueue(In(n=1, sleep_s=0.2))
    await wait_until(lambda: _calls_at_least(gate, 3), timeout=10)  # retrying against the outage
    worker.request_shutdown()
    await asyncio.sleep(settings.grace_s + 2 * settings.kill_timeout_s + 1)
    assert not run.done()  # still waiting for the database, not exited
    assert (await get(conn, task_id)).state is State.RUNNING
    gate["open"] = True
    assert await asyncio.wait_for(run, 20) == 0
    row = await get(conn, task_id)
    assert row.state is State.SUCCEEDED
    assert row.result["n"] == 1


@pytest.mark.usefixtures("sdk")
async def test_a_second_signal_abandons_an_unresolved_transition(
    conn, settings, monkeypatch, caplog
):
    async def down(*_args, **_kwargs):
        msg = "simulated outage"
        raise psycopg.OperationalError(msg)

    monkeypatch.setattr(store, "succeed", down)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_S", 0.1)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_MAX_S", 0.1)
    worker = Worker([sleep_task], settings=settings)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.started.wait(), 15)
    task_id = await sleep_task.enqueue(In(sleep_s=0.2))
    await wait_until(lambda: _state_is(conn, task_id, State.RUNNING), timeout=10)
    await asyncio.sleep(1.0)
    with caplog.at_level("ERROR", logger="fronta.worker"):
        worker.request_shutdown()
        worker.request_shutdown()
        assert await asyncio.wait_for(run, 20) == 0
    assert any("abandoned unsettled attempt" in r.message for r in caplog.records)
    assert (await get(conn, task_id)).state is State.RUNNING  # left to the reaper


async def _calls_at_least(gate, n):
    return gate["calls"] >= n


async def _state_is(conn, task_id, state):
    return (await get(conn, task_id)).state is state


async def test_the_watchdog_thread_ends_with_the_worker(settings, run_worker):
    before = threading.active_count()
    async with run_worker(Worker([sleep_task], settings=settings)):
        assert threading.active_count() == before + 1
    await wait_until(lambda: _threads_back_to(before), timeout=5)


async def test_a_failing_scavenger_does_not_stop_the_reaper(
    conn, settings, run_worker, monkeypatch, caplog
):
    def broken() -> int:
        msg = "simulated /proc failure"
        raise OSError(msg)

    monkeypatch.setattr(sandbox, "scavenge_orphans", broken)
    await store.publish_task_type(conn, sleep_task.spec)
    task_id = await store.enqueue(conn, NewTask("sleep", "{}", sleep_task.policy))
    stale = await store.claim(conn, types=["sleep"], worker="ghost", lease_s=0.1, deadline_s=1)
    assert stale is not None
    with caplog.at_level("WARNING", logger="fronta.worker"):
        async with run_worker(Worker([long_proc, sleep_task], settings=settings)):
            await wait_until(lambda: _reaped_and_rerun(conn, task_id), timeout=20)
    assert any("scavenger failed" in record.message for record in caplog.records)


async def _released(conn, task_id):
    row = await get(conn, task_id)
    return row.state is State.QUEUED and row.attempt >= 1


async def _threads_back_to(count):
    return threading.active_count() <= count


async def _reaped_and_rerun(conn, task_id):
    row = await get(conn, task_id)
    return row.failures >= 1 and row.state is State.SUCCEEDED


@pytest.mark.usefixtures("sdk")
async def test_a_second_signal_during_phase_two_settles_every_controller_before_exit(
    conn, settings, monkeypatch, caplog
):
    """The release is stuck on an outage; the second signal must still end every controller."""

    async def down(*_args, **_kwargs):
        msg = "simulated outage"
        raise psycopg.OperationalError(msg)

    monkeypatch.setattr(store, "release", down)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_S", 0.1)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_MAX_S", 0.1)
    worker = Worker([sleep_task], settings=settings)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.started.wait(), 15)
    task_id = await sleep_task.enqueue(In(sleep_s=60))
    await wait_until(lambda: _state_is(conn, task_id, State.RUNNING), timeout=10)
    with caplog.at_level("ERROR", logger="fronta.worker"):
        worker.request_shutdown()  # phase 1: grace; phase 2: stop -> release retries forever
        await asyncio.sleep(settings.grace_s + 1.5)
        assert not run.done()
        started = time.monotonic()
        worker.request_shutdown()  # the second signal lands during phase 2
        assert await asyncio.wait_for(run, 60) == 0
    assert time.monotonic() - started < worker._stop_budget_s() * 2 + 5
    assert worker.attempts == {}
    live = [
        t.get_name()
        for t in asyncio.all_tasks()
        if t.get_name().startswith(("attempt-", "heartbeat-", "run-"))
    ]
    assert live == []
    assert any("abandoned unsettled attempt" in r.message for r in caplog.records)


@pytest.mark.usefixtures("sdk")
async def test_the_release_of_an_unstarted_claim_retries_through_an_outage(
    conn, settings, run_worker, monkeypatch
):
    original_claim = store.claim
    original_release = store.release
    holder: dict[str, Worker] = {}
    outage = {"left": 3}

    async def claim_then_stop(*args, **kwargs):
        row = await original_claim(*args, **kwargs)
        if row is not None:
            holder["worker"].request_shutdown()
        return row

    async def flaky_release(*args, **kwargs):
        if outage["left"] > 0:
            outage["left"] -= 1
            msg = "simulated outage"
            raise psycopg.OperationalError(msg)
        return await original_release(*args, **kwargs)

    monkeypatch.setattr(store, "claim", claim_then_stop)
    monkeypatch.setattr(store, "release", flaky_release)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_S", 0.1)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_MAX_S", 0.1)
    worker = Worker([sleep_task], settings=settings)
    holder["worker"] = worker
    async with run_worker(worker):
        task_id = await sleep_task.enqueue(In(sleep_s=60))
        await wait_until(lambda: _released(conn, task_id), timeout=15)
    assert outage["left"] == 0
    row = await get(conn, task_id)
    assert row.state is State.QUEUED
    assert row.failures == 0


async def test_a_second_signal_during_phase_two_settles_a_process_runner_and_its_sandbox(
    conn, settings, monkeypatch, caplog
):
    """A hostile sandbox plus a release stuck on an outage: the second signal must end it all."""

    async def down(*_args, **_kwargs):
        msg = "simulated outage"
        raise psycopg.OperationalError(msg)

    monkeypatch.setattr(store, "release", down)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_S", 0.1)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_MAX_S", 0.1)
    hostile = process_task(
        "hostile_shutdown",
        ["/bin/sh", "-c", "trap '' TERM; setsid sleep 600 & sleep 600"],
        input=In,
        attempt_timeout=600,
        sandbox=Sandbox(max_pids=20),
    )
    worker = Worker([hostile], settings=settings)
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.started.wait(), 15)
    task_id = await store.enqueue(conn, NewTask("hostile_shutdown", "{}", hostile.policy))
    await wait_until(lambda: _state_is(conn, task_id, State.RUNNING), timeout=10)
    await wait_until(lambda: _has_task_marker(task_id), timeout=10)
    with caplog.at_level("ERROR", logger="fronta.worker"):
        worker.request_shutdown()
        await asyncio.sleep(settings.grace_s + 0.5)  # phase 1 over, phase 2 stops the sandbox
        started = time.monotonic()
        worker.request_shutdown()
        assert await asyncio.wait_for(run, 90) == 0
    assert time.monotonic() - started < worker._stop_budget_s() * 2 + 5
    assert worker.attempts == {}
    live = [
        t.get_name()
        for t in asyncio.all_tasks()
        if t.get_name().startswith(("attempt-", "heartbeat-", "run-"))
    ]
    assert live == []
    assert sandbox.find_marked("FRONTA_TASK_ID", str(task_id)) == []
    assert leftover_sandboxes() == []
    assert any("abandoned unsettled attempt" in r.message for r in caplog.records)


@pytest.mark.usefixtures("sdk")
async def test_an_immediate_shutdown_cuts_a_stuck_unstarted_release_short(
    conn, settings, monkeypatch, caplog
):
    original_claim = store.claim
    holder: dict[str, Worker] = {}

    async def claim_then_stop_twice(*args, **kwargs):
        row = await original_claim(*args, **kwargs)
        if row is not None:
            holder["worker"].request_shutdown()
            holder["worker"].request_shutdown()
        return row

    async def down(*_args, **_kwargs):
        msg = "simulated outage"
        raise psycopg.OperationalError(msg)

    monkeypatch.setattr(store, "claim", claim_then_stop_twice)
    monkeypatch.setattr(store, "release", down)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_S", 5.0)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_MAX_S", 5.0)
    worker = Worker([sleep_task], settings=settings)
    holder["worker"] = worker
    run = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.started.wait(), 15)
    started = time.monotonic()
    task_id = await sleep_task.enqueue(In(sleep_s=60))
    with caplog.at_level("ERROR", logger="fronta.worker"):
        assert await asyncio.wait_for(run, 30) == 0
    assert time.monotonic() - started < 10  # no 5 s retry sleep, no second connection timeout
    assert any("abandoned by an immediate shutdown" in r.message for r in caplog.records)
    assert (await get(conn, task_id)).state is State.RUNNING  # left to the reaper


async def _has_task_marker(task_id):
    return bool(sandbox.find_marked("FRONTA_TASK_ID", str(task_id)))
