"""Public lifecycle-event subscription for singleton external workflow consumers."""

from __future__ import annotations

import asyncio
from typing import Any

import psycopg
import pytest
from pydantic import BaseModel

from fronta import (
    Backoff,
    Context,
    State,
    TaskEvent,
    TaskNotFound,
    Worker,
    get_task,
    store,
    subscribe_events,
    task,
)
from tests.conftest import wait_until


async def collect_until(events, task_id: int, final: State) -> list[TaskEvent]:
    found = []
    while not found or found[-1].state is not final:
        event = await asyncio.wait_for(anext(events), 10)
        if event.id == task_id:
            found.append(event)
    return found


async def state_is(conn, task_id: int, state: State) -> bool:
    row = await store.get_task(conn, task_id)
    return row is not None and row.state is state


class SourceInput(BaseModel):
    client: str


class FollowInput(BaseModel):
    source_id: int
    client: str
    value: int


@pytest.mark.usefixtures("sdk")
async def test_completion_event_can_drive_one_deduplicated_follow_up(conn, settings, run_worker):
    follow_started = asyncio.Event()
    follow_release = asyncio.Event()
    handled: list[FollowInput] = []

    @task("event_source", input=SourceInput)
    async def source(ctx: Context[Any], inp: SourceInput) -> dict[str, int]:
        del ctx, inp
        return {"value": 42}

    @task("event_follow", input=FollowInput)
    async def follow(ctx: Context[Any], inp: FollowInput) -> None:
        del ctx
        handled.append(inp)
        follow_started.set()
        await follow_release.wait()

    async with (
        run_worker(Worker([source, follow], settings=settings)),
        subscribe_events(settings) as events,
    ):
        source_id = await source.enqueue(SourceInput(client="Y"))
        source_events = await collect_until(events, source_id, State.SUCCEEDED)
        row = await get_task(source_id)
        payload = FollowInput(
            source_id=source_id,
            client=row.input["client"],
            value=row.result["value"],
        )
        key = f"workflow:{source_id}:event_follow"
        follow_id = await follow.enqueue(payload, key=key)
        await asyncio.wait_for(follow_started.wait(), 10)
        assert await follow.enqueue(payload, key=key) == follow_id
        follow_release.set()
        await wait_until(lambda: state_is(conn, follow_id, State.SUCCEEDED), timeout=10)

    assert [event.state for event in source_events] == [
        State.QUEUED,
        State.RUNNING,
        State.SUCCEEDED,
    ]
    assert row.type == "event_source"
    assert row.input == {"client": "Y"}
    assert handled == [payload]


@pytest.mark.usefixtures("sdk")
async def test_retry_broadcasts_each_state_transition(settings, run_worker):
    attempts = 0

    @task(
        "event_retry",
        input=SourceInput,
        max_attempts=2,
        backoff=Backoff(0.0, 2.0, 0.0),
    )
    async def retry(ctx: Context[Any], inp: SourceInput) -> int:
        nonlocal attempts
        del ctx, inp
        attempts += 1
        if attempts == 1:
            msg = "retry once"
            raise RuntimeError(msg)
        return attempts

    async with (
        run_worker(Worker([retry], settings=settings)),
        subscribe_events(settings) as events,
    ):
        task_id = await retry.enqueue(SourceInput(client="Y"))
        found = await collect_until(events, task_id, State.SUCCEEDED)

    assert [event.state for event in found] == [
        State.QUEUED,
        State.RUNNING,
        State.QUEUED,
        State.RUNNING,
        State.SUCCEEDED,
    ]


@pytest.mark.usefixtures("sdk")
async def test_final_failure_broadcasts_terminal_state(settings, run_worker):
    @task("event_failure", input=SourceInput, max_attempts=1)
    async def failure(ctx: Context[Any], inp: SourceInput) -> None:
        del ctx, inp
        msg = "permanent failure"
        raise RuntimeError(msg)

    async with (
        run_worker(Worker([failure], settings=settings)),
        subscribe_events(settings) as events,
    ):
        task_id = await failure.enqueue(SourceInput(client="Y"))
        found = await collect_until(events, task_id, State.FAILED)

    assert [event.state for event in found] == [State.QUEUED, State.RUNNING, State.FAILED]


@pytest.mark.usefixtures("sdk")
async def test_running_cancellation_broadcasts_only_the_final_state(conn, settings, run_worker):
    started = asyncio.Event()

    @task("event_cancel", input=SourceInput, max_attempts=1)
    async def cancellable(ctx: Context[Any], inp: SourceInput) -> None:
        del ctx, inp
        started.set()
        await asyncio.Event().wait()

    async with (
        run_worker(Worker([cancellable], settings=settings)),
        subscribe_events(settings) as events,
    ):
        task_id = await cancellable.enqueue(SourceInput(client="Y"))
        before = await collect_until(events, task_id, State.RUNNING)
        await asyncio.wait_for(started.wait(), 10)
        assert await store.request_cancel(conn, task_id) is State.RUNNING
        after = await collect_until(events, task_id, State.CANCELLED)

    assert [event.state for event in before + after] == [
        State.QUEUED,
        State.RUNNING,
        State.CANCELLED,
    ]


@pytest.mark.usefixtures("sdk")
async def test_queued_cancellation_broadcasts_immediately(conn, settings):
    @task("event_queued_cancel", input=SourceInput)
    async def queued(ctx: Context[Any], inp: SourceInput) -> None:
        del ctx, inp

    await store.publish_task_type(conn, queued.spec)
    async with subscribe_events(settings) as events:
        task_id = await queued.enqueue(SourceInput(client="Y"), conn=conn)
        assert await store.request_cancel(conn, task_id) is State.CANCELLED
        found = await collect_until(events, task_id, State.CANCELLED)

    assert [event.state for event in found] == [State.QUEUED, State.CANCELLED]


@pytest.mark.usefixtures("sdk")
async def test_breaking_iteration_closes_subscription_without_hanging(conn, settings):
    @task("event_early_break", input=SourceInput)
    async def boundary(ctx: Context[Any], inp: SourceInput) -> None:
        del ctx, inp

    await store.publish_task_type(conn, boundary.spec)

    async def consume_one() -> None:
        async with subscribe_events(settings) as events:
            task_id = await boundary.enqueue(SourceInput(client="Y"), conn=conn)
            async for event in events:
                assert event == TaskEvent(task_id, "event_early_break", State.QUEUED)
                break

    await asyncio.wait_for(consume_one(), 5)


@pytest.mark.usefixtures("sdk")
async def test_rolled_back_and_deduplicated_enqueues_do_not_broadcast(conn, dsn, settings):
    @task("event_boundary", input=SourceInput)
    async def boundary(ctx: Context[Any], inp: SourceInput) -> None:
        del ctx, inp

    await store.publish_task_type(conn, boundary.spec)
    async with subscribe_events(settings) as events:
        async with await psycopg.AsyncConnection.connect(dsn) as caller:
            rolled_back = await boundary.enqueue(SourceInput(client="rolled-back"), conn=caller)
            await caller.rollback()
        with pytest.raises(TaskNotFound):
            await get_task(rolled_back)

        task_id = await boundary.enqueue(SourceInput(client="Y"), conn=conn, key="same")
        assert (
            await boundary.enqueue(SourceInput(client="ignored"), conn=conn, key="same") == task_id
        )
        event = await asyncio.wait_for(anext(events), 10)
        assert event == TaskEvent(task_id, "event_boundary", State.QUEUED)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(events), 0.25)


@pytest.mark.usefixtures("sdk")
async def test_autocommit_enqueue_rolls_back_when_event_emission_fails(conn, monkeypatch):
    @task("event_atomic", input=SourceInput)
    async def atomic(ctx: Context[Any], inp: SourceInput) -> None:
        del ctx, inp

    await store.publish_task_type(conn, atomic.spec)

    async def broken_event(*_args):
        msg = "simulated notification failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(store, "_event", broken_event)
    with pytest.raises(RuntimeError, match="notification failure"):
        await atomic.enqueue(SourceInput(client="Y"), conn=conn)
    cur = await conn.execute("SELECT count(*) FROM fronta.tasks")
    assert (await cur.fetchone())[0] == 0
