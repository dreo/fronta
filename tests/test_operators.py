"""Pause, requeue, stats and task metadata through the SDK."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from fronta import (
    NotRequeueable,
    PayloadTooLarge,
    State,
    TaskNotFound,
    UnknownTaskType,
    Worker,
    get_task,
    pause,
    requeue,
    resume,
    stats,
    store,
    subscribe,
    task,
)
from fronta.model import Completion
from tests.conftest import wait_until
from tests.workers import In, fail_task, sleep_task


@pytest.mark.usefixtures("sdk")
async def test_pause_survives_publish_and_other_types_drain(conn, settings, run_worker):
    await store.publish_task_type(conn, sleep_task.spec)
    await pause("sleep")
    sleeper = await sleep_task.enqueue(In())
    failed = await fail_task.enqueue(In())
    async with run_worker(Worker([sleep_task, fail_task], settings=settings)):

        async def other_started():
            return (await get_task(failed, conn=conn)).attempt > 0

        await wait_until(other_started)
        assert (await get_task(sleeper, conn=conn)).state is State.QUEUED
        assert (await store.get_task_type(conn, "sleep")).paused
        await resume("sleep")

        async def succeeded():
            return (await get_task(sleeper, conn=conn)).state is State.SUCCEEDED

        await wait_until(succeeded, timeout=2)
    with pytest.raises(UnknownTaskType):
        await pause("unknown")


@pytest.mark.usefixtures("sdk")
@pytest.mark.parametrize("terminal", [State.FAILED, State.CANCELLED])
async def test_requeue_resets_failures_and_emits_a_new_transition(
    conn, settings, run_worker, terminal
):
    await store.publish_task_type(conn, sleep_task.spec)
    task_id = await sleep_task.enqueue(In(), metadata={"trace": "abc"})
    rows = await store.claim(
        conn, types=["sleep"], worker="operator", lease_s=30, deadline_s=1, count=1
    )
    if terminal is State.CANCELLED:
        await store.request_cancel(conn, task_id)
    await store.complete(
        conn,
        [
            Completion(
                task_id,
                rows[0].token,
                "fail" if terminal is State.CANCELLED else "fail_final",
                '{"reason":"test"}',
            )
        ],
    )
    async with subscribe("requeue", settings=settings, states=[State.QUEUED]) as feed:
        await requeue(task_id)
        batch = await asyncio.wait_for(anext(feed), 2)
        assert [(e.id, e.attempt) for e in batch.events] == [(task_id, 1)]
        await batch.ack()
    row = await get_task(task_id, conn=conn)
    assert row.failures == 0
    assert row.error == {"reason": "test"}
    assert row.cancel_requested_at is None
    assert row.metadata == {"trace": "abc"}
    async with run_worker(Worker([sleep_task], settings=settings)):

        async def succeeded():
            return (await get_task(task_id, conn=conn)).state is State.SUCCEEDED

        await wait_until(succeeded)
    assert (await get_task(task_id, conn=conn)).attempt == 2
    with pytest.raises(NotRequeueable):
        await requeue(task_id)
    with pytest.raises(TaskNotFound):
        await requeue(999999)


@pytest.mark.usefixtures("sdk")
async def test_requeue_refuses_active_tasks_and_dedupe_conflicts(conn):
    first = await sleep_task.enqueue(In(), key="key")
    with pytest.raises(NotRequeueable):
        await requeue(first)
    await store.request_cancel(conn, first)
    await sleep_task.enqueue(In(), key="key")
    with pytest.raises(NotRequeueable, match="active duplicate"):
        await requeue(first)
    assert (await get_task(first, conn=conn)).state is State.CANCELLED


@pytest.mark.usefixtures("sdk")
async def test_requeue_refuses_a_running_task(conn):
    await store.publish_task_type(conn, sleep_task.spec)
    task_id = await sleep_task.enqueue(In())
    rows = await store.claim(conn, types=["sleep"], worker="w", lease_s=30, deadline_s=1, count=1)
    with pytest.raises(NotRequeueable, match="running"):
        await requeue(task_id)
    row = await get_task(task_id, conn=conn)
    assert row.state is State.RUNNING
    assert row.token == rows[0].token


@pytest.mark.usefixtures("sdk")
async def test_stats_match_seeded_tasks_and_subscription_backlog(conn, settings):
    await store.publish_task_type(conn, sleep_task.spec)
    async with subscribe("stats", settings=settings, states=[State.QUEUED]):
        for _ in range(4):
            await sleep_task.enqueue(In())
        await sleep_task.enqueue(In(), run_at=datetime.now(UTC) + timedelta(hours=1))
        await store.claim(conn, types=["sleep"], worker="stats", lease_s=30, deadline_s=1, count=2)
        result = await stats()
    assert result["types"][0] | {"oldest_due_age_s": 0} == {
        "type": "sleep",
        "queued_due": 2,
        "queued_scheduled": 1,
        "running": 2,
        "oldest_due_age_s": 0,
    }
    assert result["types"][0]["oldest_due_age_s"] >= 0
    assert result["subscriptions"][0]["backlog"] == 5
    assert result["subscriptions"][0]["oldest_event_age_s"] >= 0


@pytest.mark.usefixtures("sdk")
async def test_metadata_reaches_context_and_is_not_propagated(conn, settings, run_worker):
    seen = []

    @task("metadata", input=In)
    async def handler(ctx, inp):
        seen.append(ctx.metadata)
        if inp.n == 0:
            await ctx.enqueue(handler, In(n=1))

    async with run_worker(Worker([handler], settings=settings)):
        task_id = await handler.enqueue(In(n=0), metadata={"trace": "value"})

        async def done():
            return len(seen) == 2

        await wait_until(done)
    assert seen == [{"trace": "value"}, None]
    assert (await get_task(task_id, conn=conn)).metadata == {"trace": "value"}
    with pytest.raises(PayloadTooLarge, match="metadata"):
        await handler.enqueue(In(), metadata={"value": "x" * settings.progress_cap})
