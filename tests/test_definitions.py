"""Definitions: decorators, policy validation, publishing, fingerprints."""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import timedelta
from typing import Any

import pytest
from click.testing import CliRunner
from pydantic import BaseModel, ValidationError

from fronta import Backoff, Policy, Sandbox, Settings, Worker, process_task, runtime, store, task
from fronta.cli import main
from fronta.model import Executor, TaskTypeSpec
from tests.conftest import fronta_cli
from tests.workers import In, Out, limited_task, sleep_task


def test_task_decorator_builds_a_definition_with_defaults():
    assert sleep_task.name == "sleep"
    assert sleep_task.executor is Executor.ASYNCIO
    assert sleep_task.input_model is In
    assert sleep_task.output_model is Out
    assert sleep_task.policy == Policy(attempt_timeout_s=30)
    assert sleep_task.policy.max_attempts == 3
    assert sleep_task.policy.backoff == Backoff(1.0, 2.0, 3600.0)


def test_timedelta_timeouts_are_normalized_to_seconds():
    definition = task("t", input=In, attempt_timeout=timedelta(minutes=2))(sleep_task.handler)  # type: ignore[arg-type]
    assert definition.policy.attempt_timeout_s == 120.0


def test_process_task_carries_argv_and_sandbox():
    definition = process_task("p", ["/bin/true"], input=In, sandbox=Sandbox(max_pids=4))
    assert definition.executor is Executor.PROCESS
    assert definition.argv == ("/bin/true",)
    assert definition.sandbox.max_pids == 4
    assert definition.handler is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"attempt_timeout_s": 0},
        {"attempt_timeout_s": float("inf")},
        {"attempt_timeout_s": 31 * 86400},
        {"max_concurrency": 0},
        {"max_concurrency_per_key": -1},
    ],
)
def test_invalid_policy_numbers_are_rejected(kwargs):
    with pytest.raises(ValueError, match="must be"):
        Policy(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"base_s": -1},
        {"factor": 0.5},
        {"factor": 11},
        {"cap_s": float("nan")},
        {"base_s": 10, "cap_s": 5},
        {"base_s": 40 * 86400, "cap_s": 40 * 86400},
    ],
)
def test_invalid_backoff_numbers_are_rejected(kwargs):
    with pytest.raises(ValueError, match="backoff"):
        Backoff(**kwargs)


def test_backoff_delay_bounds_follow_the_formula_and_the_cap():
    backoff = Backoff(base_s=1.0, factor=2.0, cap_s=5.0)
    assert backoff.delay_bounds(1) == (0.5, 1.0)
    assert backoff.delay_bounds(3) == (2.0, 4.0)
    assert backoff.delay_bounds(4) == (2.5, 5.0)
    assert backoff.delay_bounds(1000) == (2.5, 5.0)


def test_sandbox_rejects_reserved_env_relative_binds_and_non_positive_limits():
    with pytest.raises(ValueError, match="reserved"):
        Sandbox(env={"FRONTA_X": "1"})
    with pytest.raises(ValueError, match="absolute"):
        Sandbox(ro_binds=("usr",))
    with pytest.raises(ValueError, match="positive"):
        Sandbox(memory_bytes=0)


def test_fingerprint_is_stable_and_changes_with_the_published_fields():
    spec = sleep_task.spec
    assert spec.fingerprint == sleep_task.spec.fingerprint
    assert len(spec.fingerprint) == 64
    other = TaskTypeSpec(spec.name, spec.executor, spec.input_schema, spec.output_schema, Policy())
    assert other.fingerprint != spec.fingerprint


def test_input_schema_is_the_pydantic_validation_schema():
    schema = sleep_task.spec.input_schema
    assert schema["type"] == "object"
    assert set(schema["properties"]) == {"n", "sleep_s", "key"}


def test_worker_rejects_empty_and_duplicate_definitions():
    with pytest.raises(Exception, match="at least one"):
        Worker([])
    with pytest.raises(Exception, match="duplicate"):
        Worker([sleep_task, sleep_task])


def test_building_a_worker_needs_no_environment(monkeypatch):
    monkeypatch.delenv("FRONTA_DSN", raising=False)
    monkeypatch.setattr(runtime, "_settings", None)
    worker = Worker([sleep_task])  # a task module imports anywhere, e.g. under pytest or mypy
    with pytest.raises(ValidationError):
        _ = worker.settings  # FRONTA_* is read when the worker starts


async def test_worker_start_publishes_schemas_policy_and_limits(conn, settings, run_worker):
    async with run_worker(Worker([sleep_task, limited_task], settings=settings)):
        rows = {row.name: row for row in await store.get_task_types(conn)}
    assert set(rows) == {"sleep", "limited"}
    assert rows["sleep"].executor is Executor.ASYNCIO
    assert rows["sleep"].input_schema == sleep_task.spec.input_schema
    assert rows["sleep"].output_schema == sleep_task.spec.output_schema
    assert rows["sleep"].fingerprint == sleep_task.spec.fingerprint
    assert rows["limited"].policy.max_concurrency == 2
    assert rows["limited"].policy.max_concurrency_per_key == 1


async def test_same_name_different_fingerprint_last_writer_wins_with_a_warning(
    conn, settings, run_worker, caplog
):
    class Other(BaseModel):
        x: str

    @task("sleep", input=Other, max_attempts=9)
    async def other(ctx: Any, inp: Other) -> None:
        del ctx, inp

    async with run_worker(Worker([sleep_task], settings=settings)):
        pass
    with caplog.at_level(logging.WARNING, logger="fronta.worker"):
        async with run_worker(Worker([other], settings=settings)):
            pass
    row = await store.get_task_type(conn, "sleep")
    assert row is not None
    assert row.fingerprint == other.spec.fingerprint
    assert row.policy.max_attempts == 9
    assert any("last writer wins" in record.message for record in caplog.records)


async def test_republishing_the_same_definition_is_silent(settings, run_worker, caplog):
    async with run_worker(Worker([sleep_task], settings=settings)):
        pass
    with caplog.at_level(logging.WARNING, logger="fronta.worker"):
        async with run_worker(Worker([sleep_task], settings=settings)):
            pass
    assert not [r for r in caplog.records if "last writer wins" in r.message]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"server_token": ""},
        {"error_cap": 1},
        {"payload_cap": 10},
        {"progress_cap": 1},
        {"heartbeat_s": 30, "lease_s": 30},
        {"connect_timeout_s": 0.2},
        {"server_port": 70000},
    ],
)
def test_settings_reject_unsafe_values(kwargs):
    with pytest.raises(ValidationError):
        Settings(dsn="postgresql://x", **kwargs)


def test_server_cli_validates_its_overrides():
    result = CliRunner().invoke(
        main, ["server", "--port", "70000"], env={"FRONTA_DSN": "postgresql://x"}
    )
    assert result.exit_code != 0
    assert "invalid settings" in result.output


def test_worker_cli_reports_an_unreachable_database_cleanly():
    env = {
        **os.environ,
        "FRONTA_DSN": "postgresql://nobody@127.0.0.1:1/none",
        "FRONTA_CONNECT_TIMEOUT_S": "1",
    }
    result = subprocess.run(  # noqa: S603  # our own CLI with fixed arguments
        [fronta_cli(), "worker", "tests.cliworkers:worker"],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 1
    assert "database unavailable" in result.stderr
    assert "Traceback" not in result.stderr
