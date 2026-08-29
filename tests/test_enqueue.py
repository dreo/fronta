"""Enqueue: ids, caps, run_at, policy snapshot, caller transactions, dedupe, notifications."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from fronta import PayloadTooLarge, State, Worker, store
from fronta.model import NewTask, Policy, TaskTypeSpec
from tests.conftest import wait_until
from tests.workers import In, sleep_task


async def publish(conn, *definitions):
    for definition in definitions:
        await store.publish_task_type(conn, definition.spec)


@pytest.mark.usefixtures("sdk")
async def test_ids_are_monotonic(conn):
    await publish(conn, sleep_task)
    ids = [await sleep_task.enqueue(In(n=i)) for i in range(5)]
    assert ids == sorted(ids)
    assert len(set(ids)) == 5


@pytest.mark.usefixtures("sdk")
async def test_enqueue_stores_the_json_input_and_the_policy_snapshot(conn):
    await publish(conn, sleep_task)
    task_id = await sleep_task.enqueue(In(n=7, key="k"), priority=3, concurrency_key="ck")
    row = await store.get_task(conn, task_id)
    assert row is not None
    assert row.state is State.QUEUED
    assert row.input == {"n": 7, "sleep_s": 0.0, "key": "k"}
    assert row.priority == 3
    assert row.concurrency_key == "ck"
    assert row.max_attempts == sleep_task.policy.max_attempts
    assert row.attempt_timeout_s == 30.0
    assert row.backoff == sleep_task.policy.backoff
    assert row.attempt == 0
    assert row.failures == 0
    assert row.token is None


@pytest.mark.usefixtures("sdk")
async def test_dict_input_is_validated_against_the_model(conn):
    await publish(conn, sleep_task)
    task_id = await sleep_task.enqueue({"n": 1})
    assert (await store.get_task(conn, task_id)).input["n"] == 1
    with pytest.raises(ValueError, match="validation error"):
        await sleep_task.enqueue({"n": "not a number"})


async def test_over_cap_payload_is_rejected_at_enqueue(conn, sdk):
    await publish(conn, sleep_task)
    with pytest.raises(PayloadTooLarge, match="payload is"):
        await sleep_task.enqueue(In(key="x" * (sdk.payload_cap + 1)))


@pytest.mark.usefixtures("sdk")
async def test_nul_in_the_input_is_rejected_as_unstorable(conn):
    await publish(conn, sleep_task)
    with pytest.raises(ValueError, match="NUL"):
        await sleep_task.enqueue(In(key="a\x00b"))


@pytest.mark.usefixtures("sdk")
async def test_naive_run_at_is_rejected(conn):
    await publish(conn, sleep_task)
    with pytest.raises(ValueError, match="timezone"):
        await sleep_task.enqueue(In(), run_at=datetime(2030, 1, 1))  # noqa: DTZ001


@pytest.mark.usefixtures("sdk")
async def test_oversized_key_and_type_are_rejected_before_the_insert(conn):
    await publish(conn, sleep_task)
    with pytest.raises(ValueError, match="key must be"):
        await sleep_task.enqueue(In(), key="k" * 1025)
    with pytest.raises(ValueError, match="concurrency_key must be"):
        await sleep_task.enqueue(In(), concurrency_key="")
    with pytest.raises(ValueError, match="name must be"):
        await store.enqueue(conn, NewTask("t" * 256, "{}", Policy()))


@pytest.mark.usefixtures("sdk")
async def test_future_run_at_is_not_claimed_before_it_is_due(conn, settings, run_worker):
    async with run_worker(Worker([sleep_task], settings=settings)):
        soon = datetime.now(UTC) + timedelta(seconds=1.5)
        task_id = await sleep_task.enqueue(In(n=1), run_at=soon)
        await asyncio.sleep(0.8)
        assert (await store.get_task(conn, task_id)).state is State.QUEUED
        await wait_until(
            lambda: _is(conn, task_id, State.SUCCEEDED), timeout=settings.poll_interval_s + 5
        )
    row = await store.get_task(conn, task_id)
    assert row.started_at >= soon - timedelta(milliseconds=50)


@pytest.mark.usefixtures("sdk")
async def test_policy_snapshot_survives_a_definition_change(conn):
    await publish(conn, sleep_task)
    old = await sleep_task.enqueue(In())
    spec = sleep_task.spec
    changed = TaskTypeSpec(
        spec.name, spec.executor, spec.input_schema, spec.output_schema, Policy(max_attempts=9)
    )
    await store.publish_task_type(conn, changed)
    published = await store.get_task_type(conn, "sleep")
    new = await store.enqueue(conn, NewTask("sleep", "{}", published.policy))  # the server path
    assert (await store.get_task(conn, new)).max_attempts == 9
    assert (await store.get_task(conn, old)).max_attempts == sleep_task.policy.max_attempts


@pytest.mark.usefixtures("sdk")
async def test_conn_joins_the_callers_transaction_and_never_commits_it(conn, dsn):
    await publish(conn, sleep_task)
    async with await psycopg.AsyncConnection.connect(dsn) as caller:
        task_id = await sleep_task.enqueue(In(n=1), conn=caller)
        # Not visible to others while the caller's transaction is open...
        assert await store.get_task(conn, task_id) is None
        await caller.rollback()
    assert await store.get_task(conn, task_id) is None
    async with await psycopg.AsyncConnection.connect(dsn) as caller:
        task_id = await sleep_task.enqueue(In(n=2), conn=caller)
        await caller.commit()
    assert (await store.get_task(conn, task_id)).state is State.QUEUED


@pytest.mark.usefixtures("sdk")
async def test_wake_notification_fires_at_the_callers_commit_with_the_type(conn, dsn):
    await publish(conn, sleep_task)
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as listener:
        await listener.execute("LISTEN fronta_wake")
        async with await psycopg.AsyncConnection.connect(dsn) as caller:
            await sleep_task.enqueue(In(), conn=caller)
            notices = [n async for n in listener.notifies(timeout=0.5)]
            assert notices == []  # nothing before the commit
            await caller.commit()
        notices = [n async for n in listener.notifies(timeout=2, stop_after=1)]
    assert [n.payload for n in notices] == ["sleep"]


@pytest.mark.usefixtures("sdk")
async def test_dedupe_twenty_concurrent_enqueues_with_one_key_make_one_row(conn):
    await publish(conn, sleep_task)
    ids = await asyncio.gather(*(sleep_task.enqueue(In(n=i), key="same") for i in range(20)))
    assert len(set(ids)) == 1
    cur = await conn.execute("SELECT count(*) FROM fronta.tasks")
    assert (await cur.fetchone())[0] == 1
    stored = (await store.get_task(conn, ids[0])).input
    # A later duplicate returns the same id and never mutates the existing task.
    assert await sleep_task.enqueue(In(n=999), key="same", priority=7) == ids[0]
    row = await store.get_task(conn, ids[0])
    assert row.input == stored
    assert row.priority == 0


@pytest.mark.usefixtures("sdk")
async def test_dedupe_returns_the_existing_id_while_active_and_a_new_one_after_terminal(conn):
    await publish(conn, sleep_task)
    first = await sleep_task.enqueue(In(n=1), key="k")
    assert await sleep_task.enqueue(In(n=2), key="k") == first
    row = await store.claim(conn, types=["sleep"], worker="w", lease_s=30, deadline_s=1)
    assert row is not None
    assert await sleep_task.enqueue(In(n=3), key="k") == first  # running still dedupes
    assert await store.succeed(conn, first, row.token, "null")
    second = await sleep_task.enqueue(In(n=4), key="k")
    assert second != first


@pytest.mark.usefixtures("sdk")
async def test_keys_are_scoped_per_task_type(conn):
    await publish(conn, sleep_task)
    other = await store.enqueue(conn, NewTask("other", "{}", Policy(), key="k"))
    mine = await sleep_task.enqueue(In(), key="k")
    assert mine != other
    assert await sleep_task.enqueue(In(), key="k") == mine


@pytest.mark.usefixtures("sdk")
async def test_enqueue_without_key_never_dedupes(conn):
    await publish(conn, sleep_task)
    ids = {await sleep_task.enqueue(In(n=1)) for _ in range(3)}
    assert len(ids) == 3


async def _is(conn, task_id, state):
    row = await store.get_task(conn, task_id)
    return row is not None and row.state is state
