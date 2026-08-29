"""Cancellation: queued, running (notify and heartbeat paths), terminal."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from fronta import Settings, State, Worker, store, task
from tests.conftest import FAST, wait_until
from tests.workers import In, sleep_task


async def get(conn, task_id):
    row = await store.get_task(conn, task_id)
    assert row is not None
    return row


@pytest.mark.usefixtures("sdk")
async def test_queued_task_is_cancelled_at_once(conn):
    await store.publish_task_type(conn, sleep_task.spec)
    task_id = await sleep_task.enqueue(In())
    assert await store.request_cancel(conn, task_id) is State.CANCELLED
    row = await get(conn, task_id)
    assert row.state is State.CANCELLED
    assert row.finished_at is not None
    assert row.cancel_requested_at is not None
    assert row.attempt == 0
    assert await store.claim(conn, types=["sleep"], worker="w", lease_s=30, deadline_s=1) is None


@pytest.mark.usefixtures("sdk")
async def test_running_task_is_stopped_within_the_grace_period_and_cancelled(
    conn, settings, run_worker
):
    observed: dict[str, Any] = {}

    @task("observe", input=In, attempt_timeout=30)
    async def observe(ctx: Any, inp: In) -> None:
        del inp
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            observed["cancelled_event"] = ctx.cancelled.is_set()
            raise

    async with run_worker(Worker([observe], settings=settings)):
        task_id = await observe.enqueue(In())
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        assert await store.request_cancel(conn, task_id) is State.RUNNING
        await wait_until(
            lambda: _state(conn, task_id, State.CANCELLED), timeout=settings.grace_s + 3
        )
    row = await get(conn, task_id)
    assert row.failures == 0
    assert row.error is None
    assert row.finished_at is not None
    assert row.token is None
    assert observed == {"cancelled_event": True}


@pytest.mark.usefixtures("sdk")
async def test_cancel_arrives_through_notify_while_heartbeats_are_disabled(conn, dsn, run_worker):
    slow_beats = Settings(dsn=dsn, **{**FAST, "heartbeat_s": 30.0, "lease_s": 60.0})
    async with run_worker(Worker([sleep_task], settings=slow_beats)):
        task_id = await sleep_task.enqueue(In(sleep_s=60))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        await store.request_cancel(conn, task_id)
        await wait_until(lambda: _state(conn, task_id, State.CANCELLED), timeout=5)


@pytest.mark.usefixtures("sdk")
async def test_cancel_arrives_through_the_heartbeat_when_notifications_are_missed(
    conn, settings, run_worker
):
    async with run_worker(Worker([sleep_task], settings=settings)):
        task_id = await sleep_task.enqueue(In(sleep_s=60))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        # Flag the row without NOTIFY: only the heartbeat response can carry the request.
        await conn.execute(
            "UPDATE fronta.tasks SET cancel_requested_at = now() WHERE id = %s", (task_id,)
        )
        await wait_until(
            lambda: _state(conn, task_id, State.CANCELLED), timeout=settings.heartbeat_s + 5
        )


@pytest.mark.usefixtures("sdk")
async def test_cancel_of_a_terminal_task_is_refused(conn, settings, run_worker):
    async with run_worker(Worker([sleep_task], settings=settings)):
        task_id = await sleep_task.enqueue(In())
        await wait_until(lambda: _state(conn, task_id, State.SUCCEEDED))
    assert await store.request_cancel(conn, task_id) is None
    assert await store.request_cancel(conn, 10**9) is None
    row = await get(conn, task_id)
    assert row.state is State.SUCCEEDED
    assert row.cancel_requested_at is None


@pytest.mark.usefixtures("sdk")
async def test_a_result_produced_after_the_stop_does_not_undo_the_cancel(
    conn, settings, run_worker
):
    """Once the worker issued the stop, the recorded cause decides, not a late return value."""

    @task("late", input=In, attempt_timeout=30)
    async def late(ctx: Any, inp: In) -> str:
        del ctx, inp
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            return "late result"  # swallows the cancellation and completes anyway
        return "never"

    async with run_worker(Worker([late], settings=settings)):
        task_id = await late.enqueue(In())
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        assert await store.request_cancel(conn, task_id) is State.RUNNING
        await wait_until(lambda: _terminal(conn, task_id), timeout=settings.grace_s + 5)
    row = await get(conn, task_id)
    assert row.state is State.CANCELLED
    assert row.result is None


async def _state(conn, task_id, state):
    return (await get(conn, task_id)).state is state


async def _terminal(conn, task_id):
    return (await get(conn, task_id)).state in (State.SUCCEEDED, State.CANCELLED, State.FAILED)
