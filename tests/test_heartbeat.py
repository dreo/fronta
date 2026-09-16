"""Lease renewal under contention: healthy attempts keep their leases when the ordinary pool is
saturated, and cancellation still reaches them promptly."""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from fronta import Context, Settings, State, Worker, runtime, store, task
from fronta.model import NewTask
from fronta.worker import Attempt, Cause
from tests.conftest import FAST, wait_until
from tests.workers import In, sleep_task

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

CANCELLED_AT: list[float] = []


@task("pool_hog", input=In, attempt_timeout=30)
async def pool_hog(ctx: Context[Worker[Any]], inp: In) -> str:
    """Occupies every connection of the worker's ordinary pool for `sleep_s` seconds."""
    worker = ctx.state

    async def occupy() -> None:
        async with worker.pool.connection() as conn:
            await conn.execute("SELECT pg_sleep(%s)", (inp.sleep_s,))

    await asyncio.gather(*(occupy() for _ in range(worker.settings.pool_size)))
    return "hogged"


@task("observed", input=In, attempt_timeout=30)
async def observed(ctx: Context[Any], inp: In) -> None:
    del ctx
    try:
        await asyncio.sleep(inp.sleep_s)
    except asyncio.CancelledError:
        CANCELLED_AT.append(time.monotonic())
        raise


@contextlib.asynccontextmanager
async def lifespan(worker: Worker[Any]) -> AsyncIterator[Worker[Any]]:
    yield worker


async def _row(conn, task_id):
    row = await store.get_task(conn, task_id)
    assert row is not None
    return row


async def _state(conn, task_id, state):
    return (await _row(conn, task_id)).state is state


@pytest.mark.usefixtures("sdk")
async def test_healthy_attempts_keep_their_leases_while_the_pool_is_saturated(
    conn, dsn, run_worker
):
    """The pool is busy for three leases; nothing gets reaped and nothing runs twice."""
    settings = Settings(
        dsn=dsn, **{**FAST, "pool_size": 2, "concurrency": 4, "statement_timeout_s": 10.0}
    )
    async with run_worker(Worker([pool_hog, sleep_task], lifespan=lifespan, settings=settings)):
        sleeper = await sleep_task.enqueue(In(n=1, sleep_s=3.0))
        await wait_until(lambda: _state(conn, sleeper, State.RUNNING))
        hog = await pool_hog.enqueue(In(sleep_s=3.0))
        await wait_until(lambda: _state(conn, hog, State.SUCCEEDED), timeout=30)
        await wait_until(lambda: _state(conn, sleeper, State.SUCCEEDED), timeout=30)
    for task_id in (sleeper, hog):
        row = await _row(conn, task_id)
        assert row.failures == 0, row.error
        assert row.attempt == 1


@pytest.mark.usefixtures("sdk")
async def test_cancellation_reaches_a_handler_while_the_pool_is_saturated(conn, dsn, run_worker):
    CANCELLED_AT.clear()
    settings = Settings(
        dsn=dsn, **{**FAST, "pool_size": 2, "concurrency": 4, "statement_timeout_s": 10.0}
    )
    async with run_worker(Worker([pool_hog, observed], lifespan=lifespan, settings=settings)):
        victim = await observed.enqueue(In(sleep_s=60))
        await wait_until(lambda: _state(conn, victim, State.RUNNING))
        hog = await pool_hog.enqueue(In(sleep_s=3.0))
        await wait_until(lambda: _state(conn, hog, State.RUNNING))
        await asyncio.sleep(0.3)  # the hog now holds every ordinary connection
        requested = time.monotonic()
        assert await store.request_cancel(conn, victim) is State.RUNNING
        await wait_until(lambda: _state(conn, victim, State.CANCELLED), timeout=30)
    assert len(CANCELLED_AT) == 1
    assert CANCELLED_AT[0] - requested < settings.grace_s  # delivered, not queued behind the pool
    row = await _row(conn, victim)
    assert row.failures == 0
    assert (await _row(conn, hog)).failures == 0


@pytest.mark.usefixtures("sdk")
async def test_renewals_use_their_own_connection(conn, settings, run_worker):
    async with run_worker(Worker([sleep_task], settings=settings)) as worker:
        task_id = await sleep_task.enqueue(In(sleep_s=1.5))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        await asyncio.sleep(0.5)  # a few renewals
        cur = await conn.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
            " AND application_name = 'fronta-renewal'"
        )
        assert (await cur.fetchone())[0] == 1
        assert worker.renewal_pool.max_size == 1
        await wait_until(lambda: _state(conn, task_id, State.SUCCEEDED))
    assert (await _row(conn, task_id)).failures == 0


async def test_renewal_batches_fence_each_row_and_report_cancellation(conn):

    await store.publish_task_type(conn, sleep_task.spec)
    for _ in range(3):
        await store.enqueue(conn, NewTask("sleep", "{}", sleep_task.policy))
    rows = await store.claim(
        conn, types=["sleep"], worker="batch", lease_s=30, deadline_s=1, count=3
    )
    await store.request_cancel(conn, rows[1].id)
    result = await store.heartbeat(
        conn, [(rows[0].id, rows[0].token), (rows[1].id, rows[1].token), (rows[2].id, uuid4())], 30
    )
    assert result[rows[0].id] is None
    assert result[rows[1].id] is not None
    assert rows[2].id not in result


async def test_expired_attempt_does_not_consume_healthy_renewal_budget(conn, settings, monkeypatch):
    await store.publish_task_type(conn, sleep_task.spec)
    for _ in range(2):
        await store.enqueue(conn, NewTask("sleep", "{}", sleep_task.policy))
    rows = await store.claim(conn, types=["sleep"], worker="w", lease_s=30, deadline_s=1, count=2)
    worker = Worker([sleep_task], settings=settings)
    expired, healthy = [Attempt(worker, row) for row in rows]
    expired._renewed_at -= settings.lease_s * 2
    original = store.heartbeat
    seen = []

    async def delayed(c, renewals, lease_s):
        seen.extend(i for i, _ in renewals)
        await asyncio.sleep(0.02)  # exceeds the poisoned chunk's former 1 ms budget
        return await original(c, renewals, lease_s)

    monkeypatch.setattr(store, "heartbeat", delayed)
    worker._renewal_pool = runtime.make_pool(settings, max_size=1)
    await runtime.open_ready(worker.renewal_pool, 5)
    try:
        assert await worker._renew_attempts([expired, healthy])
    finally:
        await worker.renewal_pool.close()
    assert expired.cause is Cause.LOST
    assert healthy.cause is None
    assert seen == [healthy.row.id]


async def test_renewal_loop_splits_more_than_one_thousand_attempts(conn, settings, monkeypatch):
    await store.publish_task_type(conn, sleep_task.spec)
    async with conn.transaction():
        for _ in range(1001):
            await store.enqueue(conn, NewTask("sleep", "{}", sleep_task.policy))
    rows = []
    while len(rows) < 1001:
        rows.extend(
            await store.claim(
                conn, types=["sleep"], worker="w", lease_s=30, deadline_s=1, count=256
            )
        )
    await store.request_cancel(conn, rows[-1].id)
    worker = Worker(
        [sleep_task], settings=settings.model_copy(update={"heartbeat_s": 10, "lease_s": 30})
    )
    worker.attempts = {
        row.id: Attempt(worker, row, claimed_at=time.monotonic() - 10) for row in rows
    }
    original = store.heartbeat
    batches = []

    async def observed(c, renewals, lease_s):
        result = await original(c, renewals, lease_s)
        batches.append(len(renewals))
        if sum(batches) == 1001:
            worker._background_stop.set()
        return result

    monkeypatch.setattr(store, "heartbeat", observed)
    worker._renewal_pool = runtime.make_pool(worker.settings, max_size=1)
    await runtime.open_ready(worker.renewal_pool, 5)
    try:
        await asyncio.wait_for(worker._renewals(), 10)
    finally:
        await worker.renewal_pool.close()
    assert batches == [1000, 1]
    assert worker.attempts[rows[-1].id].cause is Cause.CANCEL
    assert all(a.cause is not Cause.LOST for a in worker.attempts.values())


async def test_many_running_tasks_share_renewal_statements(conn, settings, run_worker, monkeypatch):

    observed = []
    original = store.heartbeat

    async def heartbeat(c, renewals, lease_s):
        observed.append(len(renewals))
        return await original(c, renewals, lease_s)

    monkeypatch.setattr(store, "heartbeat", heartbeat)
    await store.publish_task_type(conn, sleep_task.spec)
    ids = [
        await store.enqueue(conn, NewTask("sleep", '{"sleep_s":1.5}', sleep_task.policy))
        for _ in range(120)
    ]
    async with run_worker(
        Worker([sleep_task], settings=settings.model_copy(update={"concurrency": 120}))
    ):

        async def done():
            rows = await (
                await conn.execute("SELECT count(*) FROM fronta.tasks WHERE state='succeeded'")
            ).fetchone()
            return rows[0] == len(ids)

        await wait_until(done)
    assert max(observed) == 120
    assert sum(observed) > 120
    assert (
        await (
            await conn.execute("SELECT max(failures), max(attempt) FROM fronta.tasks")
        ).fetchone()
    ) == (0, 1)


@pytest.mark.usefixtures("sdk")
async def test_unconfirmed_renewals_stop_all_attempts_by_lease_end(
    conn, settings, run_worker, monkeypatch
):

    stopped, began = [], []

    @task("lease_cascade", input=In)
    async def sleeper(ctx, inp):
        del inp
        began.append(ctx.task_id)
        try:
            await asyncio.sleep(60)
        finally:
            stopped.append(ctx.task_id)

    async def stall(*_args):
        await asyncio.sleep(60)

    monkeypatch.setattr(store, "heartbeat", stall)
    await store.publish_task_type(conn, sleeper.spec)
    ids = [
        await store.enqueue(conn, NewTask(sleeper.name, "{}", sleeper.policy)) for _ in range(40)
    ]
    started = time.monotonic()
    worker = Worker(
        [sleeper], settings=settings.model_copy(update={"concurrency": 40, "reaper_interval_s": 60})
    )
    async with run_worker(worker):

        async def done():
            return len(stopped) == len(ids)

        await wait_until(done, timeout=3)
        assert time.monotonic() - started < settings.lease_s + settings.heartbeat_s + 0.5
        assert sorted(stopped) == sorted(began) == ids
    assert not worker.attempts
