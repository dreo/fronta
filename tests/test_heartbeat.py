"""Lease renewal under contention: healthy attempts keep their leases when the ordinary pool is
saturated, and cancellation still reaches them promptly."""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any

import pytest

from fronta import Context, Settings, State, Worker, store, task
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
