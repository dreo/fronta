"""Executor stop paths never block the event loop and stay bounded (portable: no sandbox)."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from uuid import uuid4

from fronta import Backoff, State, process_task
from fronta.executors import ProcessExecution
from fronta.model import TaskRow
from tests.workers import In


class SlowSandbox:
    """Stands in for `SandboxProcess`: a graceful stop that walks a big `/proc` synchronously."""

    sandbox_id = "fake-sandbox"

    def __init__(self, scan_s: float) -> None:
        self.scan_s = scan_s
        self.terminated = 0

    def terminate(self) -> int:
        time.sleep(self.scan_s)
        self.terminated += 1
        return 1


def _row() -> TaskRow:
    now = datetime.now(UTC)
    return TaskRow(
        id=1,
        type="p",
        state=State.RUNNING,
        priority=0,
        key=None,
        concurrency_key=None,
        input={},
        result=None,
        error=None,
        progress=None,
        attempt=1,
        failures=0,
        max_attempts=3,
        attempt_timeout_s=30.0,
        backoff=Backoff(),
        token=uuid4(),
        lease_until=now,
        worker="w",
        cancel_requested_at=None,
        created_at=now,
        run_at=now,
        started_at=now,
        finished_at=None,
    )


def _execution(kill_timeout_s: float) -> ProcessExecution:
    definition = process_task("p", ["/bin/true"], input=In)
    return ProcessExecution(
        definition,
        _row(),
        bwrap_path="bwrap",
        worker="w",
        result_cap=1024,
        error_cap=1024,
        kill_timeout_s=kill_timeout_s,
    )


async def _max_loop_gap(duration_s: float) -> float:
    """Largest pause between event-loop turns over `duration_s` (a stalled loop shows here)."""
    gaps = []
    last = time.monotonic()
    deadline = last + duration_s
    while time.monotonic() < deadline:
        await asyncio.sleep(0.01)
        now = time.monotonic()
        gaps.append(now - last)
        last = now
    return max(gaps)


async def test_a_slow_graceful_stop_runs_off_the_event_loop():
    execution = _execution(kill_timeout_s=5.0)
    proc = SlowSandbox(scan_s=0.8)
    execution._proc = proc
    watching = asyncio.create_task(_max_loop_gap(1.2))
    started = time.monotonic()
    await execution.stop()
    elapsed = time.monotonic() - started
    assert proc.terminated == 1
    assert 0.7 <= elapsed < 3.0  # waited for the signal to be sent ...
    assert await watching < 0.25  # ... without stalling other tasks (the heartbeats among them)


async def test_a_graceful_stop_is_bounded_by_the_kill_timeout(caplog):
    execution = _execution(kill_timeout_s=0.3)
    execution._proc = SlowSandbox(scan_s=2.0)
    with caplog.at_level(logging.WARNING, logger="fronta.executors"):
        started = time.monotonic()
        await execution.stop()
        elapsed = time.monotonic() - started
    assert elapsed < 1.5  # the controller moves on to the hard kill; the thread finishes alone
    assert any("slow" in record.message for record in caplog.records)
    await asyncio.sleep(2.0)  # let the orphaned thread finish before the loop closes


async def test_stop_before_the_spawn_only_records_the_request():
    execution = _execution(kill_timeout_s=1.0)
    await execution.stop()
    assert execution._stopped
    assert execution._proc is None
