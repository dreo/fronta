"""Deterministic interleavings at the store level: the first committed transition wins."""

import asyncio
from uuid import uuid4

import psycopg

from fronta import Backoff, State, store, subscribe
from fronta.model import Completion, NewTask, Policy
from tests.workers import sleep_task


async def setup(conn, n=1, **policy):
    """Publish, enqueue `n` tasks (no retry delay unless given) and claim them all."""
    await store.publish_task_type(conn, sleep_task.spec)
    policy.setdefault("backoff", Backoff(0.0, 2.0, 0.0))
    ids = [await store.enqueue(conn, NewTask("sleep", "{}", Policy(**policy))) for _ in range(n)]
    rows = [
        (
            await store.claim(conn, types=["sleep"], worker="w", lease_s=30, deadline_s=1, count=1)
            or [None]
        )[0]
        for _ in ids
    ]
    assert all(r is not None for r in rows)
    return rows


async def expire(conn, *ids):
    await conn.execute(
        "UPDATE fronta.tasks SET lease_until = now() - interval '1 second' WHERE id = ANY(%s)",
        (list(ids),),
    )


async def test_completion_before_reap_stands_and_reap_finds_nothing(conn):
    (row,) = await setup(conn)
    assert (await store.complete(conn, [Completion(row.id, row.token, "succeed", "1")])).get(row.id)
    await expire(conn, row.id)  # a lease column left behind must not matter for a terminal row
    assert await store.reap(conn) == []
    final = await store.get_task(conn, row.id)
    assert final.state is State.SUCCEEDED
    assert final.result == 1


async def test_reap_before_completion_rejects_the_stale_completion(conn):
    (row,) = await setup(conn)
    await expire(conn, row.id)
    assert await store.reap(conn) == [(row.id, "sleep", State.QUEUED)]
    assert not (await store.complete(conn, [Completion(row.id, row.token, "succeed", "1")])).get(
        row.id
    )
    assert (await store.complete(conn, [Completion(row.id, row.token, "fail", "{}")])).get(
        row.id
    ) is None
    assert (await store.complete(conn, [Completion(row.id, row.token, "release")])).get(
        row.id
    ) is None
    assert not (await store.complete(conn, [Completion(row.id, row.token, "cancel")])).get(row.id)
    assert await store.heartbeat(conn, [(row.id, row.token)], 30) == {}
    assert not await store.set_progress(conn, row.id, row.token, "1")
    final = await store.get_task(conn, row.id)
    assert final.state is State.QUEUED
    assert final.failures == 1
    assert final.result is None
    assert final.error["type"] == "LeaseExpired"
    assert final.token is None


async def test_stale_writer_cannot_touch_the_replacement_attempt(conn):
    (first,) = await setup(conn)
    await expire(conn, first.id)
    await store.reap(conn)
    second = (
        await store.claim(conn, types=["sleep"], worker="w2", lease_s=30, deadline_s=1, count=1)
        or [None]
    )[0]
    assert second.id == first.id
    assert second.attempt == 2
    assert second.token != first.token
    assert not (
        await store.complete(conn, [Completion(first.id, first.token, "succeed", '"stale"')])
    ).get(first.id)
    assert (await store.complete(conn, [Completion(first.id, first.token, "fail", "{}")])).get(
        first.id
    ) is None
    assert await store.heartbeat(conn, [(first.id, first.token)], 30) == {}
    mid = await store.get_task(conn, first.id)
    assert mid.state is State.RUNNING
    assert mid.attempt == 2
    assert mid.worker == "w2"
    assert (
        await store.complete(conn, [Completion(second.id, second.token, "succeed", '"fresh"')])
    ).get(second.id)
    assert (await store.get_task(conn, first.id)).result == "fresh"


async def test_cancel_request_then_completion_succeeds(conn):
    (row,) = await setup(conn)
    assert await store.request_cancel(conn, row.id) is State.RUNNING
    assert (await store.heartbeat(conn, [(row.id, row.token)], 30))[row.id] is not None
    assert (await store.complete(conn, [Completion(row.id, row.token, "succeed", "1")])).get(row.id)
    final = await store.get_task(conn, row.id)
    assert final.state is State.SUCCEEDED
    assert final.cancel_requested_at is not None


async def test_completion_then_cancel_request_is_refused(conn):
    (row,) = await setup(conn)
    assert (await store.complete(conn, [Completion(row.id, row.token, "succeed", "1")])).get(row.id)
    assert await store.request_cancel(conn, row.id) is None
    assert (await store.get_task(conn, row.id)).state is State.SUCCEEDED


async def test_pending_cancel_turns_retry_and_release_into_cancelled_but_not_final_failure(conn):
    retry, release, final = await setup(conn, 3, max_attempts=5)
    for row in (retry, release, final):
        assert await store.request_cancel(conn, row.id) is State.RUNNING
    assert (await store.complete(conn, [Completion(retry.id, retry.token, "fail", "{}")])).get(
        retry.id
    ) is State.CANCELLED
    assert (await store.complete(conn, [Completion(release.id, release.token, "release")])).get(
        release.id
    ) is State.CANCELLED
    assert (
        await store.complete(conn, [Completion(final.id, final.token, "fail_final", "{}")])
    ).get(final.id) is State.FAILED
    for row, expected in (
        (retry, State.CANCELLED),
        (release, State.CANCELLED),
        (final, State.FAILED),
    ):
        fresh = await store.get_task(conn, row.id)
        assert fresh.state is expected
        assert fresh.finished_at is not None
        assert fresh.token is None


async def test_cancel_ack_requires_a_pending_request(conn):
    (row,) = await setup(conn)
    assert not (await store.complete(conn, [Completion(row.id, row.token, "cancel")])).get(row.id)
    assert (await store.get_task(conn, row.id)).state is State.RUNNING
    await store.request_cancel(conn, row.id)
    assert (await store.complete(conn, [Completion(row.id, row.token, "cancel")])).get(row.id)
    final = await store.get_task(conn, row.id)
    assert final.state is State.CANCELLED
    assert final.failures == 0


async def test_reap_with_a_pending_cancel_cancels_without_charging(conn):
    (row,) = await setup(conn)
    await store.request_cancel(conn, row.id)
    await expire(conn, row.id)
    assert await store.reap(conn) == [(row.id, "sleep", State.CANCELLED)]
    final = await store.get_task(conn, row.id)
    assert final.failures == 0
    assert final.error is None


async def test_reap_exhausting_the_budget_fails_the_task(conn):
    (row,) = await setup(conn, max_attempts=1)
    await expire(conn, row.id)
    assert await store.reap(conn) == [(row.id, "sleep", State.FAILED)]
    final = await store.get_task(conn, row.id)
    assert final.finished_at is not None
    assert final.error["type"] == "LeaseExpired"


async def test_two_concurrent_reapers_charge_exactly_one_failure(conn, dsn):
    rows = await setup(conn, 30, max_attempts=5)
    await expire(conn, *(r.id for r in rows))
    async with (
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as a,
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as b,
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as c,
    ):
        results = await asyncio.gather(
            store.reap(a, limit=7), store.reap(b, limit=7), store.reap(c)
        )
    reaped = [r for batch in results for r in batch]
    assert sorted(r[0] for r in reaped) == sorted(r.id for r in rows)  # each exactly once
    cur = await conn.execute(
        "SELECT count(*) FROM fronta.tasks WHERE failures = 1 AND state = 'queued'"
    )
    assert (await cur.fetchone())[0] == 30


async def test_retry_delay_is_computed_by_the_database_from_the_snapshot(conn):
    rows = await setup(conn, 20, max_attempts=3, backoff=Backoff(1.0, 2.0, 10.0))
    # PostgreSQL's now() is fixed at the outer transaction start, so sequential test work cannot
    # erode the earliest delay before it is measured on a busy runner.
    async with conn.transaction():
        for row in rows:
            assert (await store.complete(conn, [Completion(row.id, row.token, "fail", "{}")])).get(
                row.id
            ) is State.QUEUED
        cur = await conn.execute(
            "SELECT extract(epoch FROM run_at - now()) FROM fronta.tasks WHERE state = 'queued'"
        )
        delays = [float(r[0]) for r in await cur.fetchall()]
    low, high = rows[0].backoff.delay_bounds(1)
    assert all(low <= d <= high for d in delays)
    assert len({round(d, 3) for d in delays}) > 1  # jitter is real


async def test_only_applied_transitions_publish_feed_rows(conn, settings):

    async with subscribe("ordering", states=list(State), settings=settings):
        (row,) = await setup(conn)
        assert not (await store.complete(conn, [Completion(row.id, uuid4(), "succeed", "1")])).get(
            row.id
        )
        assert (await store.complete(conn, [Completion(row.id, row.token, "succeed", "1")])).get(
            row.id
        )
        rows = await (
            await conn.execute(
                "SELECT state FROM fronta.events WHERE subscription='ordering' ORDER BY seq"
            )
        ).fetchall()
        assert [r[0] for r in rows] == ["queued", "running", "succeeded"]
