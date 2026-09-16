"""Claims never start with a consumed lease: lease timestamps come from the moment the row is
written, lock waits are bounded, every claimed row is dispatched as soon as its own claim returns,
and a claim that reached the worker late renews or steps aside before anything runs."""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from fronta import Backoff, Settings, State, Worker, store, task
from fronta import worker as worker_module
from fronta.model import Completion, NewTask, Policy
from fronta.worker import _Completions
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


async def test_a_claim_skips_a_locked_type_and_later_returns_a_full_lease(conn, dsn):
    await store.publish_task_type(conn, limited_task.spec)
    await store.enqueue(conn, NewTask("limited", "{}", Policy()))
    async with await psycopg.AsyncConnection.connect(dsn) as holder:
        await holder.execute(LOCK_TYPE_ROW)
        assert (
            await asyncio.wait_for(
                store.claim(
                    conn, types=["limited"], worker="w", lease_s=2, deadline_s=10, count=16
                ),
                0.5,
            )
            == []
        )
    rows = await store.claim(
        conn, types=["limited"], worker="w", lease_s=2, deadline_s=10, count=16
    )
    assert len(rows) == 1
    assert await _remaining_lease(conn, rows[0].id) > 1.5


async def test_a_heartbeat_delayed_by_a_row_lock_carries_a_full_lease(conn, dsn):
    await store.publish_task_type(conn, sleep_task.spec)
    task_id = await store.enqueue(conn, NewTask("sleep", "{}", Policy()))
    row = (
        await store.claim(conn, types=["sleep"], worker="w", lease_s=2.0, deadline_s=5, count=1)
        or [None]
    )[0]
    assert row is not None
    async with await psycopg.AsyncConnection.connect(dsn) as holder:
        await holder.execute("SELECT 1 FROM fronta.tasks WHERE id = %s FOR UPDATE", (task_id,))
        beating = asyncio.create_task(store.heartbeat(conn, [(task_id, row.token)], 2.0))
        await asyncio.sleep(1.5)
        assert not beating.done()
        await holder.rollback()
        assert await beating == {row.id: None}
    assert await _remaining_lease(conn, task_id) > 1.5


@pytest.mark.parametrize("operation", ["complete", "orphans"])
async def test_completion_locks_follow_renewal_id_order(conn, dsn, operation):
    await store.publish_task_type(conn, sleep_task.spec)
    for _ in range(2):
        await store.enqueue(conn, NewTask("sleep", "{}", Policy()))
    rows = sorted(
        await store.claim(conn, types=["sleep"], worker="w", lease_s=30, deadline_s=1, count=2),
        key=lambda row: row.id,
    )
    low, high = rows
    async with await psycopg.AsyncConnection.connect(dsn) as holder:
        await holder.execute("SELECT 1 FROM fronta.tasks WHERE id = %s FOR UPDATE", (low.id,))
        write = asyncio.create_task(
            store.complete(conn, [Completion(r.id, r.token, "succeed", "null") for r in rows[::-1]])
            if operation == "complete"
            else store.release_orphans(conn, "w", [])
        )
        try:

            async def waiting():
                row = await (
                    await holder.execute(
                        "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = %s",
                        (conn.info.backend_pid,),
                    )
                ).fetchone()
                return row[0]

            await wait_until(waiting)
            # A reverse-order writer already owns high here and would deadlock against a
            # renewal that owns low. The ordered writer waits for low before touching high.
            await holder.execute(
                "SELECT 1 FROM fronta.tasks WHERE id = %s FOR UPDATE NOWAIT", (high.id,)
            )
        finally:
            await holder.rollback()
            await asyncio.wait_for(write, 5)
    expected = State.SUCCEEDED if operation == "complete" else State.QUEUED
    for row in rows:
        assert (await _row(conn, row.id)).state is expected


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
            return []
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
        if row and not delayed["done"]:
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


# Folded batch regressions


async def batch_claims(conn, count=16, types=None):
    return await store.claim(
        conn, types=types or ["sleep"], worker="batch", lease_s=30, deadline_s=1, count=count
    )


async def batch_seed(conn, definition=sleep_task, count=16):
    await store.publish_task_type(conn, definition.spec)
    return [
        await store.enqueue(conn, NewTask(definition.name, "{}", Policy(max_attempts=2)))
        for _ in range(count)
    ]


async def batch_all_done(conn, ids):
    rows = await (
        await conn.execute(
            "SELECT count(*) FROM fronta.tasks WHERE id=ANY(%s) AND state='succeeded'", (ids,)
        )
    ).fetchone()
    return rows[0] == len(ids)


async def test_mixed_completions_fence_every_row_and_retry_idempotently(conn):
    ids = await batch_seed(conn, count=10)
    rows = await batch_claims(conn)
    for i in (3, 5, 6):
        await store.request_cancel(conn, ids[i])
    (await store.complete(conn, [Completion(ids[9], rows[9].token, "succeed", '"already"')])).get(
        ids[9]
    )
    kinds = [
        "succeed",
        "fail",
        "fail_final",
        "cancel",
        "release",
        "release",
        "fail",
        "cancel",
        "succeed",
        "succeed",
    ]
    outcomes = [
        Completion(r.id, r.token if i != 8 else uuid4(), kinds[i], '{"value":1}')
        for i, r in enumerate(rows)
    ]
    applied = await store.complete(conn, outcomes)
    assert applied == dict(
        zip(
            ids[:7],
            [
                State.SUCCEEDED,
                State.QUEUED,
                State.FAILED,
                State.CANCELLED,
                State.QUEUED,
                State.CANCELLED,
                State.CANCELLED,
            ],
            strict=True,
        )
    )
    assert await store.complete(conn, outcomes) == {}
    fresh = [await store.get_task(conn, i) for i in ids]
    assert [r.failures for r in fresh] == [0, 1, 1, 0, 0, 0, 1, 0, 0, 0]
    assert fresh[0].result == {"value": 1}
    assert fresh[9].result == "already"
    assert fresh[7].state is fresh[8].state is State.RUNNING


async def test_batch_obeys_caller_rollback(conn, dsn):
    ids = await batch_seed(conn, count=4)
    rows = await batch_claims(conn)
    async with await psycopg.AsyncConnection.connect(dsn) as caller, caller.transaction():
        assert (
            len(
                await store.complete(
                    caller, [Completion(r.id, r.token, "succeed", "1") for r in rows]
                )
            )
            == 4
        )
        raise psycopg.Rollback
    assert all([(await store.get_task(conn, i)).state is State.RUNNING for i in ids])


@pytest.mark.parametrize("response_lost", [False, True])
async def test_worker_batch_retries_outage_and_uncertain_commit(
    conn, dsn, monkeypatch, run_worker, response_lost
):
    original = store.complete
    attempted = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def flaky(c, batch):
        nonlocal calls
        calls += 1
        if calls == 1:
            if response_lost:
                await original(c, batch)
            attempted.set()
            await release.wait()
            raise psycopg.OperationalError("injected lost connection")
        return await original(c, batch)

    monkeypatch.setattr(store, "complete", flaky)
    settings = Settings(dsn=dsn, **FAST)
    ids = await batch_seed(conn, count=5)
    worker = Worker([sleep_task], settings=settings)
    async with run_worker(worker):
        await asyncio.wait_for(attempted.wait(), 5)
        await asyncio.sleep(1.2)
        assert worker.attempts  # slots and attempt controllers remain owned until acknowledgment
        if not response_lost:
            for i in ids:
                row = await store.get_task(conn, i)
                assert row.state is State.RUNNING
                assert row.failures == 0  # renewed while waiting for the completion connection
        release.set()
        await wait_until(lambda: batch_all_done(conn, ids), timeout=10)
    assert calls >= 2
    assert [(await store.get_task(conn, i)).attempt for i in ids] == [1] * len(ids)


async def test_batched_retry_events_and_attempts(conn, dsn, run_worker):
    seen = Counter()

    @task("retry_batch", input=In, max_attempts=2, backoff=Backoff(0, 2, 0))
    async def retry(ctx, inp):
        seen[ctx.task_id] += 1
        if ctx.attempt == 1:
            raise ValueError("retry")
        return inp.n

    await store.publish_task_type(conn, retry.spec)
    ids = [await store.enqueue(conn, NewTask(retry.name, "{}", retry.policy)) for _ in range(100)]
    settings = Settings(dsn=dsn, **FAST)
    async with run_worker(Worker([retry], settings=settings)):
        await wait_until(lambda: batch_all_done(conn, ids), timeout=20)
    assert seen == dict.fromkeys(ids, 2)


async def test_batch_cap_never_reserves_more_than_free_worker_slots(conn, dsn, run_worker):
    active = peak = 0
    worker = None

    @task("slots", input=In)
    async def handler(ctx, inp):
        nonlocal active, peak
        del ctx, inp
        active += 1
        peak = max(peak, active)
        assert len(worker.attempts) <= 5
        try:
            await asyncio.sleep(0.01)
        finally:
            active -= 1

    await store.publish_task_type(conn, handler.spec)
    ids = [await store.enqueue(conn, NewTask("slots", "{}", handler.policy)) for _ in range(100)]
    worker = Worker(
        [handler],
        settings=Settings(dsn=dsn, **FAST),
    )
    async with run_worker(worker):
        await wait_until(lambda: batch_all_done(conn, ids), timeout=10)
    assert peak == 5
    assert not worker.attempts
    assert not [t for t in asyncio.all_tasks() if t.get_name() == "fronta-completions"]


async def test_sparse_completion_writes_immediately_and_coalesces_inflight_siblings():

    entered, release = asyncio.Event(), asyncio.Event()
    writes = []

    async def write(batch):
        writes.append([c.id for c in batch])
        if len(writes) == 1:
            entered.set()
            await release.wait()
        return {c.id: State.SUCCEEDED for c in batch}

    completions = _Completions(write)
    first = asyncio.create_task(completions.complete(Completion(1, uuid4(), "succeed", "null")))
    await asyncio.wait_for(entered.wait(), 0.5)
    siblings = [
        asyncio.create_task(completions.complete(Completion(i, uuid4(), "succeed", "null")))
        for i in range(2, 10)
    ]
    await asyncio.sleep(0)
    assert not first.done()
    release.set()
    assert await asyncio.gather(first, *siblings) == [State.SUCCEEDED] * 9
    assert writes == [[1], list(range(2, 10))]
    await completions.close()


@pytest.mark.parametrize("jitter", [0.8, 1, 1.2])
async def test_poll_backoff_resets_on_dispatch_and_wake(settings, monkeypatch, jitter):

    worker = Worker([sleep_task], settings=settings.model_copy(update={"poll_interval_s": 0.2}))
    waits = []
    claims = 0

    async def claim(_types, _count):
        nonlocal claims
        claims += 1
        return claims == 4

    async def wait(coro, timeout):
        coro.close()
        waits.append(timeout)
        if len(waits) == 6:
            return True  # a hint resets the backoff
        if len(waits) == 9:
            worker._stopping.set()
        raise TimeoutError

    monkeypatch.setattr(worker, "_claim_batch_and_dispatch", claim)
    monkeypatch.setattr(worker_module.random, "uniform", lambda _lo, _hi: jitter)
    monkeypatch.setattr(worker_module.asyncio, "wait_for", wait)
    await worker._claim_loop()
    assert waits == [min(0.2, t * jitter) for t in [0.05, 0.1, 0.2, 0.05, 0.1, 0.2, 0.05, 0.1, 0.2]]
