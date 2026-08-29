"""Lifecycle: results, failures, retries, timeouts, progress, heartbeats, context."""

from __future__ import annotations

import asyncio
import logging
import math
from typing import Any

import psycopg
import pytest

from fronta import ProgressTooLarge, Settings, State, Worker, store, task
from fronta import worker as worker_module
from fronta.model import NewTask
from tests.conftest import FAST, wait_until
from tests.workers import (
    In,
    Out,
    fail_task,
    fanout_task,
    final_task,
    lifespan,
    progress_task,
    sleep_task,
    state_task,
    timeout_task,
)


async def get(conn, task_id):
    row = await store.get_task(conn, task_id)
    assert row is not None
    return row


async def settled(conn, task_id, *states):
    async def check():
        return (await get(conn, task_id)).state in states

    await wait_until(check, timeout=20)
    return await get(conn, task_id)


@pytest.mark.parametrize("value", [{"k": [1, 2]}, [1, "two"], "text", 42, 1.5, True, None])
@pytest.mark.usefixtures("sdk")
async def test_success_stores_every_json_value_kind(conn, settings, run_worker, value):
    @task("any", input=In, attempt_timeout=30)
    async def any_task(ctx: Any, inp: In) -> Any:
        del ctx, inp
        return value

    async with run_worker(Worker([any_task], settings=settings)):
        task_id = await any_task.enqueue(In())
        row = await settled(conn, task_id, State.SUCCEEDED)
    assert row.result == value
    assert row.error is None
    assert row.finished_at is not None
    assert row.token is None
    assert row.lease_until is None


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (math.nan, "Out of range"),
        (math.inf, "Out of range"),
        (object(), "not JSON serializable"),
        ("x" * (1024 * 1024 + 1), "cap"),
        ("nul\x00byte", "NUL"),
    ],
    ids=("nan", "infinity", "object", "over-cap", "nul"),
)
@pytest.mark.usefixtures("sdk")
async def test_unstorable_results_fail_without_retry(conn, settings, run_worker, value, reason):
    @task("untyped", input=In, attempt_timeout=30)
    async def untyped(ctx: Any, inp: In) -> Any:
        del ctx, inp
        return value

    async with run_worker(Worker([untyped], settings=settings)):
        task_id = await untyped.enqueue(In())
        row = await settled(conn, task_id, State.FAILED)
    assert row.attempt == 1
    assert row.failures == 1
    assert row.error["type"] == "ResultSerializationError"
    assert reason in row.error["message"]


@pytest.mark.usefixtures("sdk")
async def test_result_violating_the_output_model_fails_without_retry(conn, settings, run_worker):
    @task("typed", input=In, output=Out, attempt_timeout=30)
    async def typed(ctx: Any, inp: In) -> Any:
        del ctx, inp
        return {"wrong": "shape"}

    async with run_worker(Worker([typed], settings=settings)):
        task_id = await typed.enqueue(In())
        row = await settled(conn, task_id, State.FAILED)
    assert row.attempt == 1
    assert row.error["type"] == "ResultSerializationError"
    assert "validation error" in row.error["message"]


@pytest.mark.usefixtures("sdk")
async def test_exception_retries_with_backoff_then_fails_after_the_budget(
    conn, settings, run_worker
):
    async with run_worker(Worker([fail_task], settings=settings)):
        task_id = await fail_task.enqueue(In(n=7))
        await wait_until(lambda: _failed_at_least(conn, task_id, 1))
        first_retry = await get(conn, task_id)
        assert first_retry.state is State.QUEUED
        assert first_retry.failures == 1
        assert first_retry.attempt == 1
        assert first_retry.error["type"] == "RuntimeError"
        assert "failing on purpose (7)" in first_retry.error["message"]
        assert "raise RuntimeError" in first_retry.error["traceback"]
        delay = (first_retry.run_at - first_retry.started_at).total_seconds()
        low, high = fail_task.policy.backoff.delay_bounds(1)
        assert low - 0.05 <= delay <= high + 0.5  # jittered in [d/2, d] plus attempt duration
        final = await settled(conn, task_id, State.FAILED)
    assert final.attempt == 3
    assert final.failures == 3
    assert final.finished_at is not None
    assert final.token is None


@pytest.mark.usefixtures("sdk")
async def test_exception_messages_with_nul_bytes_are_stored_sanitized(conn, settings, run_worker):
    @task("nul_error", input=In, attempt_timeout=30, max_attempts=1)
    async def nul_error(ctx: Any, inp: In) -> None:
        del ctx, inp
        raise RuntimeError("bad\x00byte")

    async with run_worker(Worker([nul_error], settings=settings)):
        task_id = await nul_error.enqueue(In())
        row = await settled(conn, task_id, State.FAILED)
    assert row.error["type"] == "RuntimeError"
    assert row.error["message"] == "bad\ufffdbyte"


@pytest.mark.usefixtures("sdk")
async def test_non_retryable_error_fails_at_once(conn, settings, run_worker):
    async with run_worker(Worker([final_task], settings=settings)):
        task_id = await final_task.enqueue(In())
        row = await settled(conn, task_id, State.FAILED)
    assert row.attempt == 1
    assert row.failures == 1
    assert row.error["type"] == "NonRetryableError"
    assert row.error["message"] == "do not retry"


async def test_input_that_does_not_match_the_model_fails_at_once(conn, settings, run_worker):
    await store.publish_task_type(conn, sleep_task.spec)
    task_id = await store.enqueue(conn, NewTask("sleep", '{"n": "nope"}', sleep_task.policy))
    async with run_worker(Worker([sleep_task], settings=settings)):
        row = await settled(conn, task_id, State.FAILED)
    assert row.attempt == 1
    assert row.error["type"] == "InputValidationError"
    assert "n" in row.error["message"]


@pytest.mark.usefixtures("sdk")
async def test_error_metadata_is_truncated_to_the_cap(conn, dsn, run_worker):
    small = settings_with(dsn, error_cap=2048)

    @task("huge_error", input=In, max_attempts=1, attempt_timeout=30)
    async def huge(ctx: Any, inp: In) -> None:
        del ctx, inp
        raise RuntimeError("x" * 10000)

    async with run_worker(Worker([huge], settings=small)):
        task_id = await huge.enqueue(In())
        row = await settled(conn, task_id, State.FAILED)
    assert row.error["truncated"] is True
    assert row.error["type"] == "RuntimeError"
    assert len(row.error["message"]) < 10000
    cur = await conn.execute(
        "SELECT octet_length(error::text) FROM fronta.tasks WHERE id = %s", (task_id,)
    )
    assert (await cur.fetchone())[0] <= 2048 + 64  # jsonb text adds spaces after colons


@pytest.mark.usefixtures("sdk")
async def test_progress_is_stored_and_over_cap_progress_raises_in_the_handler(
    conn, dsn, run_worker
):
    tiny = settings_with(dsn, progress_cap=64)
    seen: list[BaseException | None] = []

    @task("big_progress", input=In, attempt_timeout=30, max_attempts=1)
    async def big(ctx: Any, inp: In) -> str:
        del inp
        await ctx.progress({"ok": True})
        try:
            await ctx.progress({"pad": "x" * 100})
        except ProgressTooLarge as exc:
            seen.append(exc)
        return "done"

    async with run_worker(Worker([progress_task, big], settings=tiny)):
        task_id = await progress_task.enqueue(In(n=5))
        row = await settled(conn, task_id, State.SUCCEEDED)
        assert row.progress == {"step": 2, "n": 5}
        big_id = await big.enqueue(In())
        big_row = await settled(conn, big_id, State.SUCCEEDED)
    assert big_row.progress == {"ok": True}
    assert len(seen) == 1


@pytest.mark.usefixtures("sdk")
async def test_attempt_timeout_cancels_the_handler_and_retries(conn, settings, run_worker):
    async with run_worker(Worker([timeout_task], settings=settings)):
        task_id = await timeout_task.enqueue(In())
        await wait_until(lambda: _failed_at_least(conn, task_id, 1))
        row = await get(conn, task_id)
        assert row.failures == 1
        assert row.error["type"] == "AttemptTimeout"
        final = await settled(conn, task_id, State.FAILED)
    assert final.attempt == 2
    assert final.failures == 2


@pytest.mark.usefixtures("sdk")
async def test_heartbeats_extend_the_lease_while_the_handler_runs(conn, settings, run_worker):
    async with run_worker(Worker([sleep_task], settings=settings)):
        task_id = await sleep_task.enqueue(In(sleep_s=2.5))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        leases = []
        for _ in range(8):
            await asyncio.sleep(0.25)
            row = await get(conn, task_id)
            if row.state is State.RUNNING:
                cur = await conn.execute("SELECT now()")
                now = (await cur.fetchone())[0]
                assert row.lease_until > now  # never expired
                leases.append(row.lease_until)
        row = await settled(conn, task_id, State.SUCCEEDED)
    assert len(leases) >= 3
    assert leases[-1] > leases[0]
    assert row.result["n"] == 0


@pytest.mark.usefixtures("sdk")
async def test_heartbeats_continue_through_the_grace_period(conn, dsn, run_worker):
    """A handler that delays its cancellation past the lease must not be reaped meanwhile."""
    slow_grace = settings_with(dsn, grace_s=3.0, lease_s=1.0, heartbeat_s=0.2)

    @task("lingering", input=In, attempt_timeout=0.5, max_attempts=1)
    async def lingering(ctx: Any, inp: In) -> None:
        del ctx, inp
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            await asyncio.sleep(2.0)  # linger inside the grace period, longer than the lease
            raise

    async with run_worker(Worker([lingering], settings=slow_grace)):
        task_id = await lingering.enqueue(In())
        row = await settled(conn, task_id, State.FAILED)
    assert row.error["type"] == "AttemptTimeout"  # decided by the worker, not by the reaper
    assert row.attempt == 1


@pytest.mark.usefixtures("sdk")
async def test_the_final_transition_survives_a_database_disconnect(
    conn, settings, run_worker, caplog
):
    async with run_worker(Worker([sleep_task], settings=settings)) as worker:
        task_id = await sleep_task.enqueue(In(sleep_s=1.5))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        with caplog.at_level(logging.WARNING, logger="fronta.worker"):
            await conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                " WHERE datname = current_database() AND pid <> pg_backend_pid()"
            )
            row = await settled(conn, task_id, State.SUCCEEDED)
        assert worker.started.is_set()
    assert row.attempt == 1
    assert row.failures == 0


@pytest.mark.usefixtures("sdk")
async def test_ctx_enqueue_is_immediate_and_independent_of_the_outcome(conn, settings, run_worker):
    async with run_worker(Worker([fanout_task, sleep_task], settings=settings)):
        parent = await fanout_task.enqueue(In(n=1))
        row = await settled(conn, parent, State.SUCCEEDED)
        assert row.attempt == 2  # first attempt failed after enqueueing the child
        cur = await conn.execute(
            "SELECT count(*) FROM fronta.tasks WHERE type = 'sleep' AND key = %s",
            (f"child-of-{parent}",),
        )
        assert (await cur.fetchone())[0] == 1  # the retry's enqueue deduped on the key
        cur = await conn.execute("SELECT id FROM fronta.tasks WHERE type = 'sleep'")
        child = (await cur.fetchone())[0]
        await settled(conn, child, State.SUCCEEDED)


@pytest.mark.usefixtures("sdk")
async def test_context_exposes_lifespan_state_log_and_correlation(
    conn, settings, run_worker, caplog
):
    async with run_worker(Worker([state_task], lifespan=lifespan, settings=settings)):
        with caplog.at_level(logging.INFO, logger="fronta.worker"):
            task_id = await state_task.enqueue(In())
            row = await settled(conn, task_id, State.SUCCEEDED)
    assert row.result == {"resource": "ready", "cancelled": False}
    line = next(r for r in caplog.records if "using lifespan state" in r.message)
    assert f"task={task_id}" in line.message
    assert "attempt=1" in line.message
    assert line.task_id == task_id  # type: ignore[attr-defined]  # LoggerAdapter extra


@pytest.mark.usefixtures("sdk")
async def test_handler_raising_cancelled_error_itself_is_a_retryable_failure(
    conn, settings, run_worker
):
    @task("self_cancel", input=In, attempt_timeout=30, max_attempts=1)
    async def self_cancel(ctx: Any, inp: In) -> None:
        del ctx, inp
        raise asyncio.CancelledError

    async with run_worker(Worker([self_cancel], settings=settings)):
        task_id = await self_cancel.enqueue(In())
        row = await settled(conn, task_id, State.FAILED)
    assert row.error["type"] == "CancelledError"


def settings_with(dsn, **overrides):
    return Settings(dsn=dsn, **{**FAST, **overrides})


async def _state(conn, task_id, state):
    return (await get(conn, task_id)).state is state


async def _failed_at_least(conn, task_id, failures):
    return (await get(conn, task_id)).failures >= failures


@pytest.mark.usefixtures("sdk")
async def test_the_final_transition_retries_through_a_database_outage(
    conn, settings, run_worker, monkeypatch
):
    """Success is never lost to an outage: the transition retries until the database answers."""
    original = store.succeed
    failures = {"left": 4}

    async def flaky(*args, **kwargs):
        if failures["left"] > 0:
            failures["left"] -= 1
            msg = "simulated outage"
            raise psycopg.OperationalError(msg)
        return await original(*args, **kwargs)

    monkeypatch.setattr(store, "succeed", flaky)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_S", 0.2)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_MAX_S", 0.2)
    async with run_worker(Worker([sleep_task], settings=settings)):
        task_id = await sleep_task.enqueue(In(n=4))
        row = await settled(conn, task_id, State.SUCCEEDED)
    assert failures["left"] == 0
    assert row.attempt == 1
    assert row.failures == 0  # heartbeats kept the lease alive through the retries
    assert row.result["n"] == 4


@pytest.mark.usefixtures("sdk")
async def test_literal_backslash_u0000_in_a_result_is_stored_as_is(conn, settings, run_worker):
    text = "\\u0000 literal"

    @task("literal", input=In, attempt_timeout=30)
    async def literal(ctx: Any, inp: In) -> str:
        del ctx, inp
        return text

    async with run_worker(Worker([literal], settings=settings)):
        task_id = await literal.enqueue(In())
        row = await settled(conn, task_id, State.SUCCEEDED)
    assert row.result == text


@pytest.mark.usefixtures("sdk")
async def test_non_string_keys_fail_without_retry_and_tuples_become_arrays(
    conn, settings, run_worker
):
    @task("intkeys", input=In, attempt_timeout=30)
    async def intkeys(ctx: Any, inp: In) -> Any:
        del ctx, inp
        return {1: "x"}

    @task("tuples", input=In, attempt_timeout=30)
    async def tuples(ctx: Any, inp: In) -> Any:
        del ctx, inp
        return (1, "two")

    async with run_worker(Worker([intkeys, tuples], settings=settings)):
        bad = await intkeys.enqueue(In())
        good = await tuples.enqueue(In())
        bad_row = await settled(conn, bad, State.FAILED)
        good_row = await settled(conn, good, State.SUCCEEDED)
    assert bad_row.attempt == 1
    assert bad_row.error["type"] == "ResultSerializationError"
    assert "keys must be strings" in bad_row.error["message"]
    assert good_row.result == [1, "two"]
