"""Retention purge: only old terminal rows, in batches, safe under concurrency and locks."""

from __future__ import annotations

import asyncio

import psycopg

from fronta import Settings, State, Worker, store
from tests.conftest import FAST, wait_until
from tests.workers import sleep_task

INSERT = """
INSERT INTO fronta.tasks (type, state, input, max_attempts, attempt_timeout_s, backoff_base_s,
    backoff_factor, backoff_cap_s, finished_at)
SELECT 'sleep', %(state)s, '{}', 1, 1, 1, 2, 10, now() - make_interval(secs => %(age_s)s)
FROM generate_series(1, %(n)s)
"""


async def insert(conn, state, age_s, n):
    await conn.execute(INSERT, {"state": state, "age_s": age_s, "n": n})


async def count(conn, state=None):
    cur = await conn.execute(
        "SELECT count(*) FROM fronta.tasks WHERE %(state)s::text IS NULL OR state = %(state)s",
        {"state": state},
    )
    return (await cur.fetchone())[0]


async def test_only_terminal_rows_older_than_the_retention_are_deleted(conn):
    await insert(conn, "succeeded", 100, 3)
    await insert(conn, "failed", 100, 2)
    await insert(conn, "cancelled", 100, 1)
    await insert(conn, "succeeded", 10, 4)  # recent
    await insert(conn, "queued", 100, 2)  # never
    await insert(conn, "running", 100, 2)  # never
    assert await store.purge_tasks(conn, retention_s=50, batch=1000) == 6
    assert await store.purge_tasks(conn, retention_s=50, batch=1000) == 0
    assert await count(conn) == 8
    assert await count(conn, "queued") == 2
    assert await count(conn, "running") == 2
    assert await count(conn, "succeeded") == 4


async def test_batches_delete_the_oldest_first_and_never_more_than_the_batch(conn):
    await insert(conn, "succeeded", 300, 5)
    await insert(conn, "succeeded", 200, 5)
    await insert(conn, "succeeded", 100, 5)
    assert await store.purge_tasks(conn, retention_s=50, batch=6) == 6
    cur = await conn.execute(
        "SELECT max(extract(epoch FROM now() - finished_at)) FROM fronta.tasks"
    )
    assert 195 < (await cur.fetchone())[0] < 205  # the 300 s rows and one 200 s row are gone
    assert await store.purge_tasks(conn, retention_s=50, batch=6) == 6
    assert await store.purge_tasks(conn, retention_s=50, batch=6) == 3
    assert await count(conn) == 0


async def test_concurrent_purges_with_forced_lock_overlap_delete_each_row_exactly_once(conn, dsn):
    await insert(conn, "succeeded", 100, 200)
    async with (
        await psycopg.AsyncConnection.connect(dsn) as holder,
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as a,
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as b,
    ):
        await holder.execute("SELECT id FROM fronta.tasks ORDER BY id LIMIT 50 FOR UPDATE")
        first = await asyncio.gather(store.purge_tasks(a, 50, 60), store.purge_tasks(b, 50, 60))
        second = await asyncio.gather(store.purge_tasks(a, 50, 60), store.purge_tasks(b, 50, 60))
        assert sum(first) == 120
        assert sum(first + second) == 150  # the 50 locked rows were skipped, nothing twice
        assert await count(conn) == 50
        await holder.rollback()
    assert await store.purge_tasks(conn, 50, 60) == 50
    assert await count(conn) == 0


async def test_the_worker_purges_on_its_interval(conn, dsn, run_worker):
    await insert(conn, "succeeded", 100, 2500)
    await insert(conn, "succeeded", 10, 3)
    settings = Settings(
        dsn=dsn, **{**FAST, "retention_s": 50.0, "purge_interval_s": 0.5, "purge_batch": 1000}
    )
    async with run_worker(Worker([sleep_task], settings=settings)):
        await wait_until(lambda: _count_is(conn, 3), timeout=15)
    assert await count(conn, State.SUCCEEDED.value) == 3


async def _count_is(conn, expected):
    return await count(conn) == expected
