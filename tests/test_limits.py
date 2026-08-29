"""Concurrency limits: exact under 20 workers, per key, atomic, no starvation, live changes."""

from __future__ import annotations

from typing import Any

from fronta import Settings, State, Worker, store, task
from fronta.model import NewTask, Policy
from tests import workers
from tests.conftest import FAST, running_all, wait_until
from tests.workers import In, Out, limited_task, sleep_task


def max_overlap(intervals, key=None):
    """Largest number of intervals (optionally of one key) running at the same instant."""
    points = []
    for i in intervals:
        if key is None or i.key == key:
            points.append((i.start, 1))
            points.append((i.end, -1))
    points.sort(key=lambda p: (p[0], p[1]))
    current = peak = 0
    for _, delta in points:
        current += delta
        peak = max(peak, current)
    return peak


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
    assert await store.succeed(conn, running[0].id, running[0].token, "1")
    assert await _claim(conn) is None  # 2 still running > 1
    assert await store.succeed(conn, running[1].id, running[1].token, "1")
    assert await _claim(conn) is None  # 1 running == 1
    assert await store.succeed(conn, running[2].id, running[2].token, "1")
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
    return await store.claim(conn, types=["limited"], worker="w", lease_s=30, deadline_s=1)
