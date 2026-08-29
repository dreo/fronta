"""Claim: order, accepted/published types, exclusivity under concurrency, tokens, rollback."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from fronta import Settings, State, Worker, store
from fronta.model import NewTask, Policy
from tests import workers
from tests.conftest import FAST, running_all, wait_until
from tests.workers import In, fail_task, sleep_task


async def publish(conn, *definitions):
    for definition in definitions:
        await store.publish_task_type(conn, definition.spec)


async def claim(conn, *types):
    return await store.claim(conn, types=list(types), worker="w", lease_s=30, deadline_s=1)


@pytest.mark.usefixtures("sdk")
async def test_claim_order_is_priority_desc_run_at_asc_id_asc(conn):
    await publish(conn, sleep_task)
    past = datetime.now(UTC) - timedelta(seconds=10)
    earlier = past - timedelta(seconds=5)
    a = await sleep_task.enqueue(In(n=1), priority=0, run_at=past)
    b = await sleep_task.enqueue(In(n=2), priority=5, run_at=past)
    c = await sleep_task.enqueue(In(n=3), priority=5, run_at=earlier)
    d = await sleep_task.enqueue(In(n=4), priority=5, run_at=past)
    e = await sleep_task.enqueue(In(n=5), priority=-1)
    order = [(await claim(conn, "sleep")).id for _ in range(5)]
    assert order == [c, b, d, a, e]
    assert await claim(conn, "sleep") is None


@pytest.mark.usefixtures("sdk")
async def test_only_accepted_types_are_claimed(conn):
    await publish(conn, sleep_task, fail_task)
    await sleep_task.enqueue(In())
    fail_id = await fail_task.enqueue(In())
    row = await claim(conn, "fail")
    assert row is not None
    assert row.id == fail_id
    assert await claim(conn, "fail") is None


async def test_only_published_types_are_claimed(conn):
    await store.enqueue(conn, NewTask("unpublished", "{}", Policy()))
    assert await claim(conn, "unpublished") is None
    await store.publish_task_type(conn, sleep_task.spec)
    await store.enqueue(conn, NewTask("sleep", "{}", Policy()))
    assert (await claim(conn, "unpublished", "sleep")).type == "sleep"


@pytest.mark.usefixtures("sdk")
async def test_claim_issues_a_fresh_token_and_increments_attempt(conn):
    await publish(conn, sleep_task)
    task_id = await sleep_task.enqueue(In())
    first = await claim(conn, "sleep")
    assert first.state is State.RUNNING
    assert first.attempt == 1
    assert first.token is not None
    assert first.lease_until is not None
    assert first.worker == "w"
    assert await store.release(conn, task_id, first.token) is State.QUEUED
    second = await claim(conn, "sleep")
    assert second.attempt == 2
    assert second.token != first.token


@pytest.mark.usefixtures("sdk")
async def test_a_rolled_back_claim_leaves_no_trace(conn, dsn):
    await publish(conn, sleep_task)
    task_id = await sleep_task.enqueue(In())
    async with await psycopg.AsyncConnection.connect(dsn) as other, other.transaction():
        row = await claim(other, "sleep")
        assert row is not None
        raise psycopg.Rollback
    fresh = await store.get_task(conn, task_id)
    assert fresh.state is State.QUEUED
    assert fresh.attempt == 0
    assert fresh.token is None


@pytest.mark.usefixtures("sdk")
async def test_a_task_locked_by_another_claim_is_skipped_not_waited_for(conn, dsn):
    await publish(conn, sleep_task)
    first = await sleep_task.enqueue(In(n=1))
    second = await sleep_task.enqueue(In(n=2))
    async with await psycopg.AsyncConnection.connect(dsn) as holder:
        await holder.execute("SELECT id FROM fronta.tasks WHERE id = %s FOR UPDATE", (first,))
        row = await asyncio.wait_for(claim(conn, "sleep"), 2)
        assert row.id == second


async def test_twenty_workers_hundred_jobs_all_succeed_exactly_once_without_overlap(conn, dsn):
    workers.INTERVALS.clear()
    settings = Settings(dsn=dsn, **{**FAST, "pool_size": 2, "concurrency": 3})
    fleet = [Worker([sleep_task], settings=settings) for _ in range(20)]
    await publish(conn, sleep_task)
    ids = [
        await store.enqueue(
            conn, NewTask("sleep", f'{{"n": {i}, "sleep_s": 0.05}}', sleep_task.policy)
        )
        for i in range(100)
    ]
    async with running_all(fleet):
        await wait_until(lambda: _all_in(conn, ids, State.SUCCEEDED), timeout=60)
    cur = await conn.execute(
        "SELECT count(*) FILTER (WHERE attempt = 1 AND failures = 0) FROM fronta.tasks"
    )
    assert (await cur.fetchone())[0] == 100
    seen = [i.task_id for i in workers.INTERVALS]
    assert sorted(seen) == sorted(ids)  # every job ran in exactly one handler, exactly once
    for row_id in ids:
        row = await store.get_task(conn, row_id)
        assert row.result["n"] == row.input["n"]


async def _all_in(conn, ids, state):
    cur = await conn.execute(
        "SELECT count(*) FROM fronta.tasks WHERE id = ANY(%s) AND state = %s", (ids, state.value)
    )
    return (await cur.fetchone())[0] == len(ids)
