"""Stress: backlogs, bursts, thousands of quick tasks, limits under load, big rows and tables."""

from __future__ import annotations

import asyncio
import statistics
import threading
import time
from itertools import pairwise
from pathlib import Path
from typing import Any

import httpx
import pytest_asyncio
from psycopg import sql

from fronta import Settings, State, Worker, runtime, store, task
from fronta.model import TaskFilter
from fronta.server import create_app
from tests import workers
from tests.conftest import FAST, running_all, wait_until
from tests.test_limits import max_overlap
from tests.test_server import TOKEN, free_port, serve
from tests.workers import In, Out, sleep_task

BULK = """
INSERT INTO fronta.tasks (type, state, input, priority, run_at, max_attempts, attempt_timeout_s,
    backoff_base_s, backoff_factor, backoff_cap_s, finished_at)
SELECT %(type)s, %(state)s, '{}', (random() * %(priorities)s)::int,
       now() + make_interval(secs => %(offset_s)s + (random() * %(spread_s)s)::int),
       3, 30, 1, 2, 3600,
       CASE WHEN %(state)s IN ('succeeded', 'failed', 'cancelled')
            THEN now() - make_interval(secs => %(age_s)s) END
FROM generate_series(1, %(n)s)
"""


async def bulk(conn, task_type, n, **shape):
    """Insert `n` rows of `task_type`; `shape` overrides the row shape."""
    params = {
        "type": task_type,
        "n": n,
        "state": "queued",
        "priorities": 10,
        "offset_s": -3600,
        "spread_s": 3600,
        "age_s": 0,
        **shape,
    }
    await conn.execute(BULK, params)


async def count(conn, state=None):
    cur = await conn.execute(
        "SELECT count(*) FROM fronta.tasks WHERE %(s)s::text IS NULL OR state = %(s)s", {"s": state}
    )
    return (await cur.fetchone())[0]


async def timed_claims(conn, types, n):
    durations = []
    for _ in range(n):
        started = time.perf_counter()
        row = await store.claim(conn, types=types, worker="w", lease_s=30, deadline_s=5)
        durations.append(time.perf_counter() - started)
        assert row is not None
    return durations


def fds() -> int:
    return len(list(Path("/proc/self/fd").iterdir()))


# ---------------------------------------------------------------- long backlogs


async def test_claims_stay_fast_on_a_sixty_thousand_row_backlog(conn):
    await store.publish_task_type(conn, sleep_task.spec)
    await store.publish_task_type(conn, workers.limited_task.spec)
    await bulk(conn, "sleep", 40_000)
    await bulk(conn, "limited", 20_000)
    await conn.execute("ANALYZE fronta.tasks")
    cur = await conn.execute(
        sql.SQL("EXPLAIN ") + store._CANDIDATE, {"types": ["sleep", "limited"], "skip": []}
    )
    plan = "\n".join(row[0] for row in await cur.fetchall())
    assert "tasks_queue_idx" in plan  # walked in claim order ...
    assert "Sort" not in plan  # ... never sorted
    durations = await timed_claims(conn, ["sleep", "limited"], 50)
    assert statistics.median(durations) < 0.05, statistics.median(durations)
    assert max(durations) < 0.5, max(durations)


async def test_claims_stay_fast_behind_thirty_thousand_higher_priority_tasks_due_later(conn):
    """Scheduled work at a higher priority sits ahead of due work in the index; it is skipped."""
    await store.publish_task_type(conn, sleep_task.spec)
    await bulk(conn, "sleep", 30_000, priorities=0, offset_s=3600, spread_s=3600)  # future, prio 10
    await conn.execute("UPDATE fronta.tasks SET priority = 10")
    await bulk(conn, "sleep", 1_000, priorities=0)  # due, priority 0
    await conn.execute("ANALYZE fronta.tasks")
    durations = await timed_claims(conn, ["sleep"], 30)
    assert statistics.median(durations) < 0.05, statistics.median(durations)
    cur = await conn.execute("SELECT count(*) FROM fronta.tasks WHERE state = 'running'")
    assert (await cur.fetchone())[0] == 30
    cur = await conn.execute(
        "SELECT count(*) FROM fronta.tasks WHERE state = 'running' AND priority = 10"
    )
    assert (await cur.fetchone())[0] == 0  # only due work was claimed


async def test_a_backlog_drains_in_priority_order(conn, dsn):
    """Ten thousand queued rows, three priorities: the fleet finishes high before low."""
    await store.publish_task_type(conn, sleep_task.spec)
    await bulk(conn, "sleep", 10_000, priorities=3)
    settings = Settings(dsn=dsn, **{**FAST, "pool_size": 3, "concurrency": 10})
    fleet = [Worker([sleep_task], settings=settings) for _ in range(4)]
    started = time.monotonic()
    async with running_all(fleet):
        await wait_until(lambda: _count_is(conn, "succeeded", 10_000), timeout=240)
    elapsed = time.monotonic() - started
    cur = await conn.execute(
        "SELECT priority, max(started_at) FROM fronta.tasks GROUP BY 1 ORDER BY 1 DESC"
    )
    windows = await cur.fetchall()
    # Every task of a higher priority started before the *bulk* of a lower one had started: the
    # best-effort order allows only the concurrency window (40 slots) to overlap.
    for (hi, hi_last), (lo, _) in pairwise(windows):
        cur = await conn.execute(
            "SELECT count(*) FROM fronta.tasks WHERE priority = %s AND started_at < %s",
            (lo, hi_last),
        )
        assert (await cur.fetchone())[0] <= 40, (hi, lo)
    assert elapsed < 200, elapsed


# ---------------------------------------------------------------- bursts


async def test_two_thousand_concurrent_sdk_enqueues_are_exact_and_fast(conn, dsn):
    await store.publish_task_type(conn, sleep_task.spec)
    runtime.configure(Settings(dsn=dsn, **{**FAST, "pool_size": 8}))
    try:
        started = time.perf_counter()
        ids = await asyncio.gather(
            *(
                sleep_task.enqueue(In(n=i), key=f"k{i % 50}" if i < 500 else None)
                for i in range(2000)
            )
        )
        elapsed = time.perf_counter() - started
    finally:
        await runtime.close_pool()
    assert len(set(ids)) == 1550  # 1500 keyless rows + 50 keys deduped under the burst
    assert await count(conn) == 1550
    cur = await conn.execute("SELECT count(DISTINCT key) FROM fronta.tasks WHERE key IS NOT NULL")
    assert (await cur.fetchone())[0] == 50
    assert 2000 / elapsed > 100, elapsed  # measured ~470/s on a laptop Docker Postgres


@pytest_asyncio.fixture
async def stress_server(dsn):
    port = free_port()
    settings = Settings(dsn=dsn, **{**FAST, "server_token": TOKEN, "server_port": port})
    async for base in serve(create_app(settings), port):
        yield base


@pytest_asyncio.fixture
async def stress_api(stress_server):
    async with httpx.AsyncClient(
        base_url=stress_server + "/api/v1",
        headers={"Authorization": f"Bearer {TOKEN}"},
        timeout=30,
        # uvicorn closes idle keep-alive connections after 5 s and httpx's default expiry is
        # also 5 s, so a connection reused right at that boundary dies mid-request
        # (`ReadError`; measured 1 in 100 at a 4.9 s idle gap, and CI's slower phases hit it).
        limits=httpx.Limits(max_connections=100, keepalive_expiry=1.0),
    ) as client:
        yield client


async def test_two_hundred_concurrent_rest_clients_are_consistent(stress_api, conn):
    """Bursts of enqueues (sharing keys), reads, lists and cancels through the API at once."""
    await store.publish_task_type(conn, sleep_task.spec)
    keys = [f"k{i % 20}" for i in range(120)] + [None] * 80  # 20 keys, 80 keyless

    async def enqueue(key):
        body: dict[str, Any] = {"type": "sleep", "input": {"n": 1}}
        if key is not None:
            body["key"] = key
        return await stress_api.post("/tasks", json=body)

    started = time.perf_counter()
    responses = await asyncio.gather(*(enqueue(k) for k in keys))
    elapsed = time.perf_counter() - started
    assert {r.status_code for r in responses} == {201}
    ids = [r.json()["id"] for r in responses]
    by_key: dict[str, set[int]] = {}
    for key, task_id in zip(keys, ids, strict=True):
        if key is not None:
            by_key.setdefault(key, set()).add(task_id)
    assert all(len(v) == 1 for v in by_key.values())  # one row per key under the burst
    assert len(set(ids)) == 100  # 20 keyed + 80 keyless rows
    assert elapsed < 20, elapsed
    reads = await asyncio.gather(
        *(stress_api.get(f"/tasks/{i}") for i in set(ids)),
        *(stress_api.get("/tasks", params={"state": "queued"}) for _ in range(50)),
    )
    assert {r.status_code for r in reads} == {200}
    cancels = await asyncio.gather(*(stress_api.post(f"/tasks/{i}/cancel") for i in ids))
    assert {r.status_code for r in cancels} <= {200, 409}
    assert await count(conn, "cancelled") == 100


# ---------------------------------------------------------------- many quick tasks


async def test_two_thousand_quick_tasks_through_a_fleet_exactly_once_without_leaks(conn, dsn):
    """Sustained load: every task succeeds exactly once; tasks, threads, descriptors and
    database connections are back at their baselines after the fleet stops."""
    await store.publish_task_type(conn, sleep_task.spec)
    await bulk(conn, "sleep", 2_000, priorities=1)
    baseline_threads = threading.active_count()
    baseline_fds = fds()
    settings = Settings(dsn=dsn, **{**FAST, "pool_size": 3, "concurrency": 10})
    fleet = [Worker([sleep_task], settings=settings) for _ in range(4)]
    started = time.monotonic()
    async with running_all(fleet):
        await wait_until(lambda: _count_is(conn, "succeeded", 2_000), timeout=180)
    elapsed = time.monotonic() - started
    cur = await conn.execute(
        "SELECT count(*) FILTER (WHERE attempt = 1 AND failures = 0) FROM fronta.tasks"
    )
    assert (await cur.fetchone())[0] == 2_000  # exactly once, no retries
    assert 2_000 / elapsed > 40, elapsed  # measured ~110 tasks/s on a laptop Docker Postgres
    live = [
        t.get_name()
        for t in asyncio.all_tasks()
        if t.get_name().startswith(
            ("attempt-", "heartbeat-", "run-", "tick", "listener", "reaper", "purger")
        )
    ]
    assert live == []
    deadline = time.monotonic() + 5
    while threading.active_count() > baseline_threads and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    assert threading.active_count() <= baseline_threads, [t.name for t in threading.enumerate()]
    try:
        await wait_until(lambda: _own_connections_only(conn), timeout=30)
    except AssertionError:
        cur = await conn.execute(
            "SELECT application_name, state, backend_start, left(query, 80) FROM pg_stat_activity"
            " WHERE datname = current_database() AND application_name LIKE 'fronta-%'"
        )
        msg = f"lingering connections: {await cur.fetchall()}"
        raise AssertionError(msg) from None
    assert fds() <= baseline_fds + 2


async def test_limits_hold_under_a_burst_of_six_hundred_keyed_tasks(conn, dsn):
    """600 tasks over 30 keys, per-key 1 and type 8, six workers: measured in the handlers."""
    workers.INTERVALS.clear()

    @task(
        "keyed_burst",
        input=In,
        output=Out,
        attempt_timeout=30,
        max_concurrency=8,
        max_concurrency_per_key=1,
    )
    async def keyed_burst(ctx: Any, inp: In) -> Out:
        return await workers._timed_sleep(ctx, inp)

    await store.publish_task_type(conn, keyed_burst.spec)
    async with conn.cursor() as cur:
        await cur.executemany(
            "INSERT INTO fronta.tasks (type, state, input, concurrency_key, max_attempts,"
            " attempt_timeout_s, backoff_base_s, backoff_factor, backoff_cap_s)"
            " VALUES ('keyed_burst', 'queued', %s, %s, 3, 30, 1, 2, 3600)",
            [
                (f'{{"n": {i}, "sleep_s": 0.02, "key": "k{i % 30}"}}', f"k{i % 30}")
                for i in range(600)
            ],
        )
    settings = Settings(dsn=dsn, **{**FAST, "pool_size": 3, "concurrency": 5})
    fleet = [Worker([keyed_burst], settings=settings) for _ in range(6)]
    started = time.monotonic()
    async with running_all(fleet):
        await wait_until(lambda: _count_is(conn, "succeeded", 600), timeout=180)
    elapsed = time.monotonic() - started
    assert len(workers.INTERVALS) == 600
    assert max_overlap(workers.INTERVALS) <= 8
    for key in {f"k{i}" for i in range(30)}:
        assert max_overlap(workers.INTERVALS, key) == 1
    assert max_overlap(workers.INTERVALS) >= 4  # the limit was used, not just respected
    assert elapsed < 150, elapsed


async def test_cap_sized_inputs_and_results_under_concurrency(conn, dsn):
    """Twenty tasks carrying ~900 KiB inputs and returning ~900 KiB results at the same time."""

    @task("bulky", input=In, attempt_timeout=60)
    async def bulky(ctx: Any, inp: In) -> dict[str, Any]:
        del ctx
        return {"echo": inp.key, "n": inp.n}

    await store.publish_task_type(conn, bulky.spec)
    pad = "x" * (900 * 1024)
    async with conn.cursor() as cur:
        await cur.executemany(
            "INSERT INTO fronta.tasks (type, state, input, max_attempts, attempt_timeout_s,"
            " backoff_base_s, backoff_factor, backoff_cap_s)"
            " VALUES ('bulky', 'queued', %s::jsonb, 3, 60, 1, 2, 3600)",
            [(f'{{"n": {i}, "key": "{pad}"}}',) for i in range(20)],
        )
    settings = Settings(dsn=dsn, **{**FAST, "pool_size": 3, "concurrency": 10})
    async with running_all([Worker([bulky], settings=settings) for _ in range(2)]):
        await wait_until(lambda: _count_is(conn, "succeeded", 20), timeout=120)
    cur = await conn.execute(
        "SELECT count(*) FROM fronta.tasks WHERE length(result->>'echo') = %s AND attempt = 1",
        (len(pad),),
    )
    assert (await cur.fetchone())[0] == 20


# ---------------------------------------------------------------- big tables


async def test_listing_and_pagination_stay_fast_on_a_hundred_thousand_row_table(conn, stress_api):
    await store.publish_task_type(conn, sleep_task.spec)
    await bulk(conn, "sleep", 30_000, state="succeeded", age_s=100)
    await bulk(conn, "sleep", 30_000, state="failed", age_s=100)
    await bulk(conn, "sleep", 30_000, state="queued")
    await bulk(conn, "limited", 10_000, state="queued")
    await conn.execute(
        "UPDATE fronta.tasks SET key = 'the-one' WHERE id = (SELECT max(id) FROM fronta.tasks)"
    )
    await conn.execute("ANALYZE fronta.tasks")
    queries = {
        "newest": TaskFilter(limit=200),
        "state": TaskFilter(state=State.FAILED, limit=200),
        "type+state": TaskFilter(type="limited", state=State.QUEUED, limit=200),
        "deep page": TaskFilter(state=State.SUCCEEDED, before=5_000, limit=200),
        "key": TaskFilter(key="the-one", limit=200),
    }
    for name, flt in queries.items():
        started = time.perf_counter()
        rows = await store.list_tasks(conn, flt)
        elapsed = time.perf_counter() - started
        assert rows, name
        assert elapsed < 0.1, (name, elapsed)
    started = time.perf_counter()
    page = await stress_api.get("/tasks", params={"state": "queued", "limit": 200})
    assert page.status_code == 200
    assert len(page.json()["items"]) == 200
    assert time.perf_counter() - started < 0.5


async def test_purging_a_hundred_thousand_old_rows_in_batches(conn):
    await store.publish_task_type(conn, sleep_task.spec)
    await bulk(conn, "sleep", 100_000, state="succeeded", age_s=8 * 86400)
    await bulk(conn, "sleep", 1_000, state="succeeded", age_s=60)
    await bulk(conn, "sleep", 1_000, state="queued")
    started = time.monotonic()
    batches = []
    while True:
        batch_started = time.perf_counter()
        deleted = await store.purge_tasks(conn, retention_s=7 * 86400, batch=1000)
        batches.append(time.perf_counter() - batch_started)
        if deleted < 1000:
            break
    assert sum(1 for _ in batches) == 101
    assert max(batches) < 1.0, max(batches)  # every batch is a short transaction
    assert time.monotonic() - started < 60
    assert await count(conn) == 2_000


async def _count_is(conn, state, expected):
    return await count(conn, state) == expected


async def _own_connections_only(conn):
    """No worker, listener, server or SDK connection of ours is left on the database."""
    cur = await conn.execute(
        "SELECT count(*) FROM pg_stat_activity"
        " WHERE datname = current_database() AND application_name LIKE 'fronta-%'"
    )
    return (await cur.fetchone())[0] == 0
