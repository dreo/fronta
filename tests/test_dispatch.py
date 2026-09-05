"""Claims never start with a consumed lease: lease timestamps come from the moment the row is
written, lock waits are bounded, every claimed row is dispatched as soon as its own claim returns,
and a claim that reached the worker late renews or steps aside before anything runs."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import psycopg
import pytest

from fronta import Backoff, Settings, State, Worker, store, task
from fronta.model import NewTask, Policy
from tests.conftest import FAST, wait_until
from tests.workers import In, limited_task, sleep_task

LOCK_TYPE_ROW = "SELECT 1 FROM fronta.task_types WHERE name = 'limited' FOR UPDATE"


async def _row(conn, task_id):
    row = await store.get_task(conn, task_id)
    assert row is not None
    return row


async def _state(conn, task_id, state):
    return (await _row(conn, task_id)).state is state


async def _remaining_lease(conn, task_id) -> float:
    cur = await conn.execute(
        "SELECT extract(epoch FROM lease_until - clock_timestamp())"
        " FROM fronta.tasks WHERE id = %s",
        (task_id,),
    )
    return float((await cur.fetchone())[0])


async def test_a_claim_delayed_by_the_type_lock_carries_a_full_lease(conn, dsn):
    await store.publish_task_type(conn, limited_task.spec)
    await store.enqueue(conn, NewTask("limited", "{}", Policy()))
    async with await psycopg.AsyncConnection.connect(dsn) as holder:
        await holder.execute(LOCK_TYPE_ROW)
        claiming = asyncio.create_task(
            store.claim(
                conn, types=["limited"], worker="w", lease_s=2.0, deadline_s=10, lock_timeout_s=10
            )
        )
        await asyncio.sleep(1.5)  # most of a lease passes while the claim waits for the lock
        assert not claiming.done()
        await holder.rollback()
        row = await claiming
    assert row is not None
    assert await _remaining_lease(conn, row.id) > 1.5  # stamped when written, not at BEGIN


async def test_a_heartbeat_delayed_by_a_row_lock_carries_a_full_lease(conn, dsn):
    await store.publish_task_type(conn, sleep_task.spec)
    task_id = await store.enqueue(conn, NewTask("sleep", "{}", Policy()))
    row = await store.claim(conn, types=["sleep"], worker="w", lease_s=2.0, deadline_s=5)
    assert row is not None
    async with await psycopg.AsyncConnection.connect(dsn) as holder:
        await holder.execute("SELECT 1 FROM fronta.tasks WHERE id = %s FOR UPDATE", (task_id,))
        beating = asyncio.create_task(store.heartbeat(conn, task_id, row.token, 2.0))
        await asyncio.sleep(1.5)
        assert not beating.done()
        await holder.rollback()
        assert await beating is store.Heartbeat.ALIVE
    assert await _remaining_lease(conn, task_id) > 1.5


async def test_a_claim_gives_up_a_lock_wait_past_its_bound(conn, dsn):
    await store.publish_task_type(conn, limited_task.spec)
    await store.enqueue(conn, NewTask("limited", "{}", Policy()))
    async with await psycopg.AsyncConnection.connect(dsn) as holder:
        await holder.execute(LOCK_TYPE_ROW)
        started = time.monotonic()
        row = await store.claim(
            conn, types=["limited"], worker="w", lease_s=30, deadline_s=10, lock_timeout_s=0.3
        )
        elapsed = time.monotonic() - started
        assert row is None
        assert elapsed < 2.0  # neither the statement timeout nor the deadline was needed
        await holder.rollback()
    row = await store.claim(conn, types=["limited"], worker="w", lease_s=30, deadline_s=10)
    assert row is not None
    assert row.attempt == 1  # the abandoned round left no trace


@pytest.mark.usefixtures("sdk")
async def test_a_worker_behind_a_long_type_lock_runs_the_task_once_with_a_fresh_lease(
    conn, dsn, settings, run_worker
):
    """The lock outlives the lease (1 s): claims give up at half a lease and retry; the eventual
    claim runs the task exactly once while the worker's own reaper keeps running."""
    async with run_worker(Worker([limited_task], settings=settings)):
        async with await psycopg.AsyncConnection.connect(dsn) as holder:
            await holder.execute(LOCK_TYPE_ROW)
            task_id = await limited_task.enqueue(In(n=1, sleep_s=0.3))
            await asyncio.sleep(2.5 * settings.lease_s)
            assert (await _row(conn, task_id)).state is State.QUEUED  # nothing started stale
            await holder.rollback()
        await wait_until(lambda: _state(conn, task_id, State.SUCCEEDED), timeout=15)
    row = await _row(conn, task_id)
    assert row.attempt == 1
    assert row.failures == 0
    assert row.result["n"] == 1


@pytest.mark.usefixtures("sdk")
async def test_a_claimed_row_starts_before_slower_sibling_claims_return(
    conn, dsn, run_worker, monkeypatch
):
    """Two parallel claims: the second stalls longer than a lease; the first's row must not wait."""
    original = store.claim
    calls = {"n": 0}

    async def claim(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            await asyncio.sleep(3.0)
            return None
        return await original(*args, **kwargs)

    monkeypatch.setattr(store, "claim", claim)
    await store.publish_task_type(conn, sleep_task.spec)
    task_id = await sleep_task.enqueue(In(n=2, sleep_s=0.3))
    settings = Settings(dsn=dsn, **{**FAST, "pool_size": 3})  # two claims in parallel
    async with run_worker(Worker([sleep_task], settings=settings)):
        await wait_until(lambda: _state(conn, task_id, State.SUCCEEDED), timeout=10)
    row = await _row(conn, task_id)
    assert row.failures == 0  # never reaped: its attempt ran while the sibling was still stalled
    assert row.attempt == 1


@pytest.mark.usefixtures("sdk")
async def test_a_claim_returned_after_its_lease_expired_does_not_run(
    conn, dsn, run_worker, monkeypatch
):
    """The row reaches the worker after the reaper requeued it: the stale attempt renews first,
    learns the lease is gone and never runs the handler; the requeued row runs once."""
    runs: list[int] = []

    @task("late_dispatch", input=In, attempt_timeout=30, backoff=Backoff(0.1, 2.0, 0.2))
    async def late(ctx: Any, inp: In) -> int:
        runs.append(ctx.attempt)
        return inp.n

    original = store.claim
    delayed = {"done": False}

    async def claim(*args, **kwargs):
        row = await original(*args, **kwargs)
        if row is not None and not delayed["done"]:
            delayed["done"] = True
            await asyncio.sleep(3.0)  # the 1 s lease expires and the reaper acts meanwhile
        return row

    monkeypatch.setattr(store, "claim", claim)
    settings = Settings(dsn=dsn, **{**FAST, "pool_size": 2})  # one claim at a time
    async with run_worker(Worker([late], settings=settings)):
        task_id = await late.enqueue(In(n=5))
        await wait_until(lambda: _state(conn, task_id, State.SUCCEEDED), timeout=20)
    row = await _row(conn, task_id)
    assert row.failures == 1  # the stale claim was reaped ...
    assert row.attempt == 2  # ... and the row claimed again
    assert runs == [2]  # only the fresh attempt ever ran the handler
    assert row.result == 5
