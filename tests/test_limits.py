"""Concurrency limits: exact under 20 workers, per key, atomic, no starvation, live changes."""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import replace
from typing import Any

import psycopg
import pytest
from psycopg import sql

from fronta import Settings, State, Worker, runtime, store, task
from fronta.model import Completion, NewTask, Policy
from tests import workers
from tests.conftest import FAST, max_overlap, running_all, wait_until
from tests.workers import In, Out, limited_task, sleep_task


async def all_done(conn, ids):
    cur = await conn.execute(
        "SELECT count(*) FROM fronta.tasks WHERE id = ANY(%s) AND state = 'succeeded'", (ids,)
    )
    return (await cur.fetchone())[0] == len(ids)


async def test_type_limit_holds_under_twenty_workers(conn, dsn):
    workers.INTERVALS.clear()
    settings = Settings(dsn=dsn, **{**FAST, "pool_size": 2, "concurrency": 3})
    fleet = [Worker([limited_task], settings=settings) for _ in range(20)]
    await store.publish_task_type(conn, limited_task.spec)
    ids = [
        await store.enqueue(
            conn, NewTask("limited", f'{{"n": {i}, "sleep_s": 0.1}}', limited_task.policy)
        )
        for i in range(30)
    ]
    async with running_all(fleet):
        await wait_until(lambda: all_done(conn, ids), timeout=90)
    assert len(workers.INTERVALS) == 30
    assert max_overlap(workers.INTERVALS) <= 2
    assert max_overlap(workers.INTERVALS) == 2  # the limit is used, not just respected


async def test_per_key_limit_holds_and_other_keys_run_concurrently(conn, dsn):
    workers.INTERVALS.clear()

    @task("keyed", input=In, output=Out, attempt_timeout=30, max_concurrency_per_key=1)
    async def keyed(ctx: Any, inp: In) -> Out:
        return await workers._timed_sleep(ctx, inp)

    settings = Settings(dsn=dsn, **{**FAST, "pool_size": 2, "concurrency": 4})
    fleet = [Worker([keyed], settings=settings) for _ in range(5)]
    await store.publish_task_type(conn, keyed.spec)
    ids = []
    for i in range(12):
        key = f"k{i % 3}"
        ids.append(
            await store.enqueue(
                conn,
                NewTask(
                    "keyed",
                    f'{{"n": {i}, "sleep_s": 0.2, "key": "{key}"}}',
                    keyed.policy,
                    concurrency_key=key,
                ),
            )
        )
    async with running_all(fleet):
        await wait_until(lambda: all_done(conn, ids), timeout=90)
    for key in ("k0", "k1", "k2"):
        assert max_overlap(workers.INTERVALS, key) == 1
    assert max_overlap(workers.INTERVALS) >= 2  # different keys did overlap


async def test_both_limits_are_acquired_atomically(conn, dsn):
    workers.INTERVALS.clear()
    settings = Settings(dsn=dsn, **{**FAST, "pool_size": 2, "concurrency": 5})
    fleet = [Worker([limited_task], settings=settings) for _ in range(4)]
    await store.publish_task_type(conn, limited_task.spec)
    ids = []
    for i in range(12):
        key = f"k{i % 4}"
        ids.append(
            await store.enqueue(
                conn,
                NewTask(
                    "limited",
                    f'{{"n": {i}, "sleep_s": 0.15, "key": "{key}"}}',
                    limited_task.policy,
                    concurrency_key=key,
                ),
            )
        )
    async with running_all(fleet):
        await wait_until(lambda: all_done(conn, ids), timeout=90)
    assert max_overlap(workers.INTERVALS) <= 2
    for key in ("k0", "k1", "k2", "k3"):
        assert max_overlap(workers.INTERVALS, key) <= 1


async def test_a_saturated_type_does_not_starve_other_types_or_lower_priorities(
    conn, settings, run_worker
):
    await store.publish_task_type(conn, limited_task.spec)
    await store.publish_task_type(conn, sleep_task.spec)
    blockers = [
        await store.enqueue(
            conn, NewTask("limited", '{"sleep_s": 4}', limited_task.policy, priority=10)
        )
        for _ in range(6)
    ]
    quick = [
        await store.enqueue(conn, NewTask("sleep", '{"sleep_s": 0}', sleep_task.policy, priority=0))
        for _ in range(3)
    ]
    async with run_worker(Worker([limited_task, sleep_task], settings=settings)):
        await wait_until(
            lambda: all_done(conn, quick), timeout=3
        )  # long before the blockers finish
        cur = await conn.execute(
            "SELECT count(*) FROM fronta.tasks WHERE type = 'limited' AND state = 'succeeded'"
        )
        assert (await cur.fetchone())[0] == 0
        await wait_until(lambda: all_done(conn, blockers), timeout=60)


async def test_limits_are_the_published_value_not_the_workers_own(conn, dsn, run_worker):
    """Two workers declare different limits; the last publisher's value binds both."""
    workers.INTERVALS.clear()

    @task("shared", input=In, output=Out, attempt_timeout=30, max_concurrency=3)
    async def generous(ctx: Any, inp: In) -> Out:
        return await workers._timed_sleep(ctx, inp)

    @task("shared", input=In, output=Out, attempt_timeout=30, max_concurrency=1)
    async def strict(ctx: Any, inp: In) -> Out:
        return await workers._timed_sleep(ctx, inp)

    settings = Settings(dsn=dsn, **{**FAST, "pool_size": 2})
    ids = []
    async with (
        run_worker(Worker([generous], settings=settings)),
        run_worker(Worker([strict], settings=settings)),
    ):
        for i in range(6):
            ids.append(
                await store.enqueue(
                    conn, NewTask("shared", f'{{"n": {i}, "sleep_s": 0.15}}', strict.policy)
                )
            )
        await wait_until(lambda: all_done(conn, ids), timeout=60)
    assert max_overlap(workers.INTERVALS) == 1


async def test_shrinking_a_limit_admits_nothing_until_running_drops_below_it(conn):
    await store.publish_task_type(conn, _spec(limited_task, max_concurrency=3))
    for _ in range(5):
        await store.enqueue(conn, NewTask("limited", "{}", Policy()))
    running = [await _claim(conn) for _ in range(3)]
    assert all(running)
    assert await _claim(conn) is None
    await store.publish_task_type(conn, _spec(limited_task, max_concurrency=1))
    assert await _claim(conn) is None
    assert (
        await store.complete(conn, [Completion(running[0].id, running[0].token, "succeed", "1")])
    ).get(running[0].id)
    assert await _claim(conn) is None  # 2 still running > 1
    assert (
        await store.complete(conn, [Completion(running[1].id, running[1].token, "succeed", "1")])
    ).get(running[1].id)
    assert await _claim(conn) is None  # 1 running == 1
    assert (
        await store.complete(conn, [Completion(running[2].id, running[2].token, "succeed", "1")])
    ).get(running[2].id)
    assert await _claim(conn) is not None
    assert await _claim(conn) is None


async def test_enabling_a_limit_counts_tasks_already_running(conn):
    await store.publish_task_type(conn, _spec(limited_task, max_concurrency=None))
    for _ in range(3):
        await store.enqueue(conn, NewTask("limited", "{}", Policy()))
    assert await _claim(conn)
    assert await _claim(conn)
    await store.publish_task_type(conn, _spec(limited_task, max_concurrency=2))
    assert await _claim(conn) is None


async def test_reaping_a_holder_frees_its_share(conn):
    await store.publish_task_type(conn, _spec(limited_task, max_concurrency=1))
    for _ in range(2):
        await store.enqueue(conn, NewTask("limited", "{}", Policy(max_attempts=5)))
    holder = await _claim(conn)
    assert await _claim(conn) is None
    await conn.execute(
        "UPDATE fronta.tasks SET lease_until = now() - interval '1 second' WHERE id = %s",
        (holder.id,),
    )
    assert await store.reap(conn)
    replacement = await _claim(conn)
    assert replacement is not None
    assert replacement.state is State.RUNNING


def _spec(definition, **overrides):
    spec = definition.spec
    policy = Policy(**{**_policy_dict(spec.policy), **overrides})
    return type(spec)(spec.name, spec.executor, spec.input_schema, spec.output_schema, policy)


def _policy_dict(policy):
    return {
        "max_attempts": policy.max_attempts,
        "attempt_timeout_s": policy.attempt_timeout_s,
        "backoff": policy.backoff,
        "max_concurrency": policy.max_concurrency,
        "max_concurrency_per_key": policy.max_concurrency_per_key,
    }


async def _claim(conn):
    return (
        await store.claim(conn, types=["limited"], worker="w", lease_s=30, deadline_s=1, count=1)
        or [None]
    )[0]


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


async def test_concurrent_batches_enforce_type_limit(conn, dsn):
    await batch_seed(conn, limited_task, 64)
    pool = runtime.make_pool(Settings(dsn=dsn, pool_size=16))
    await runtime.open_ready(pool, 5)

    async def claim():
        async with pool.connection() as c:
            return await batch_claims(c, types=["limited"])

    try:
        batches = await asyncio.gather(*(claim() for _ in range(16)))
    finally:
        await pool.close()
    assert sum(map(len, batches)) == 2
    assert len({r.id for b in batches for r in b}) == 2


async def test_batch_per_key_limits_include_its_uncommitted_reservations(conn):
    @task("sleep", input=In, max_concurrency_per_key=1)
    async def keyed(ctx, inp):
        del ctx
        return inp.n

    await store.publish_task_type(conn, keyed.spec)
    for key in ["hot"] * 20 + ["cold"] * 3 + [None] * 4:
        await store.enqueue(conn, NewTask("sleep", "{}", Policy(), concurrency_key=key))
    batch = await batch_claims(conn)
    assert Counter(r.concurrency_key for r in batch) == {"hot": 1, "cold": 1, None: 4}
    assert await batch_claims(conn) == []


async def test_busy_type_and_rejected_key_do_not_skip_another_types_candidates(conn, dsn):
    for name in ("a", "b"):

        @task(name, input=In, max_concurrency_per_key=1)
        async def keyed(ctx, inp):
            del ctx
            return inp.n

        await store.publish_task_type(conn, keyed.spec)
    ids = [
        await store.enqueue(conn, NewTask(typ, "{}", Policy(), concurrency_key=key))
        for typ, key in [("a", "hot"), ("a", "hot"), ("b", "hot"), ("b", "cold")]
    ]
    async with await psycopg.AsyncConnection.connect(dsn) as holder:
        assert [r.id for r in await batch_claims(holder, count=1, types=["a"])] == [ids[0]]
        assert [r.id for r in await batch_claims(conn, types=["a", "b"])] == ids[2:]
    assert await batch_claims(conn, types=["a", "b"]) == []


async def test_batched_fleet_limits_and_slot_caps(conn, dsn):
    workers.INTERVALS.clear()
    await store.publish_task_type(conn, limited_task.spec)
    ids = [
        await store.enqueue(conn, NewTask("limited", '{"sleep_s":0.04}', limited_task.policy))
        for _ in range(80)
    ]
    settings = Settings(dsn=dsn, **FAST)
    fleet = [Worker([limited_task], settings=settings) for _ in range(8)]
    async with running_all(fleet):
        await wait_until(lambda: batch_all_done(conn, ids), timeout=30)
    assert len(workers.INTERVALS) == 80
    assert max_overlap(workers.INTERVALS) == 2


async def test_publication_waits_for_unlimited_admissions_then_recounts(conn, dsn):
    await batch_seed(conn, count=5)
    async with (
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as admission,
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as publisher,
    ):
        async with admission.transaction():
            assert len(await batch_claims(admission, count=2)) == 2
            limited = replace(sleep_task.spec, policy=Policy(max_concurrency=1))
            publishing = asyncio.create_task(store.publish_task_type(publisher, limited))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(publishing), 0.05)
        await asyncio.wait_for(publishing, 5)
    assert (
        await batch_claims(conn) == []
    )  # the newly enabled limit includes both earlier admissions


async def test_raw_policy_update_waits_for_unlimited_admission(conn, dsn):
    await batch_seed(conn, count=5)
    async with (
        await psycopg.AsyncConnection.connect(dsn) as admission,
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as publisher,
    ):
        assert len(await batch_claims(admission, count=2)) == 2
        update = asyncio.create_task(
            publisher.execute("UPDATE fronta.task_types SET max_concurrency=1 WHERE name='sleep'")
        )
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(update), 0.05)
        await admission.commit()
        await asyncio.wait_for(update, 2)
    assert await batch_claims(conn) == []


async def test_pool_pins_read_committed_over_a_repeatable_read_database(conn, dsn):

    await batch_seed(conn, limited_task, 64)
    name = (await (await conn.execute("SELECT current_database()")).fetchone())[0]
    statement = sql.SQL("ALTER DATABASE {} SET default_transaction_isolation = {}")
    await conn.execute(statement.format(sql.Identifier(name), sql.Literal("repeatable read")))
    pool = runtime.make_pool(Settings(dsn=dsn, pool_size=20))

    async def claim():
        async with pool.connection() as c:
            level = await (await c.execute("SHOW transaction_isolation")).fetchone()
            assert level[0] == "read committed"
            return await batch_claims(c, types=["limited"])

    try:
        await runtime.open_ready(pool, 5)
        batches = await asyncio.gather(*(claim() for _ in range(20)))
        assert sum(map(len, batches)) == 2
    finally:
        await pool.close()
        await conn.execute(statement.format(sql.Identifier(name), sql.Literal("read committed")))


async def test_twenty_multi_type_workers_hold_type_and_key_limits(conn, settings):
    observed = []
    active = Counter()
    definitions = []
    for name in ("batch_a", "batch_b", "batch_c"):

        @task(name, input=In, max_concurrency=3, max_concurrency_per_key=1)
        async def handler(ctx, inp):
            typ = ctx._attempt.row.type
            active[typ] += 1
            active[typ, inp.key] += 1
            assert active[typ] <= 3
            assert active[typ, inp.key] <= 1
            try:
                await asyncio.sleep(0.015)
                observed.append(ctx.task_id)
            finally:
                active[typ] -= 1
                active[typ, inp.key] -= 1

        definitions.append(handler)
        await store.publish_task_type(conn, handler.spec)
    ids = [
        await store.enqueue(
            conn,
            NewTask(
                definitions[i % 3].name,
                f'{{"key":"k{i % 5}"}}',
                Policy(),
                concurrency_key=f"k{i % 5}",
            ),
        )
        for i in range(150)
    ]
    fleet = [
        Worker(
            definitions, settings=settings.model_copy(update={"concurrency": 16, "pool_size": 2})
        )
        for _ in range(20)
    ]
    async with running_all(fleet):
        await wait_until(lambda: batch_all_done(conn, ids), timeout=30)
    assert sorted(observed) == ids
    assert not any(active.values())
    assert (
        await (
            await conn.execute("SELECT max(attempt), max(failures) FROM fronta.tasks")
        ).fetchone()
    ) == (1, 0)
