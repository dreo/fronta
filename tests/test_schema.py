"""`fronta db init` is idempotent and the schema enforces its bounds."""

import psycopg
import pytest
from click.testing import CliRunner

from fronta import store
from fronta.cli import main


async def test_init_is_idempotent(conn):
    await store.init_schema(conn)
    await store.init_schema(conn)
    cur = await conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'fronta' ORDER BY 1"
    )
    assert [r[0] for r in await cur.fetchall()] == ["task_types", "tasks"]


def test_db_init_command_applies_the_schema(dsn):
    result = CliRunner().invoke(main, ["db", "init", "--dsn", dsn])
    assert result.exit_code == 0, result.output
    assert "ready" in result.output


def test_db_init_reports_connection_errors():
    result = CliRunner().invoke(
        main, ["db", "init", "--dsn", "postgresql://nobody@127.0.0.1:1/none?connect_timeout=1"]
    )
    assert result.exit_code != 0
    assert "database error" in result.output


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("type", "x" * 256),
        ("key", ""),
        ("key", "k" * 1025),
        ("concurrency_key", "c" * 1025),
    ],
)
async def test_oversized_names_and_keys_are_rejected_by_constraints(conn, column, value):
    row = {
        "type": "t",
        "key": None,
        "concurrency_key": None,
        "max_attempts": 1,
        "attempt_timeout_s": 1.0,
        "backoff_base_s": 1.0,
        "backoff_factor": 2.0,
        "backoff_cap_s": 10.0,
    }
    row[column] = value
    with pytest.raises(psycopg.errors.CheckViolation):
        await conn.execute(
            "INSERT INTO fronta.tasks (type, state, key, concurrency_key, input, max_attempts,"
            " attempt_timeout_s, backoff_base_s, backoff_factor, backoff_cap_s)"
            " VALUES (%(type)s, 'queued', %(key)s, %(concurrency_key)s, '{}', %(max_attempts)s,"
            " %(attempt_timeout_s)s, %(backoff_base_s)s, %(backoff_factor)s, %(backoff_cap_s)s)",
            row,
        )


@pytest.mark.parametrize(
    "policy",
    [
        {"max_attempts": 0},
        {"attempt_timeout_s": 0},
        {"attempt_timeout_s": 31 * 86400},
        {"backoff_factor": 11},
        {"backoff_base_s": 20.0, "backoff_cap_s": 10.0},
        {"backoff_cap_s": 31 * 86400},
    ],
)
async def test_pathological_policy_numbers_are_rejected_by_constraints(conn, policy):
    row = {
        "max_attempts": 1,
        "attempt_timeout_s": 1.0,
        "backoff_base_s": 1.0,
        "backoff_factor": 2.0,
        "backoff_cap_s": 10.0,
    }
    row.update(policy)
    with pytest.raises(psycopg.errors.CheckViolation):
        await conn.execute(
            "INSERT INTO fronta.tasks (type, state, input, max_attempts, attempt_timeout_s,"
            " backoff_base_s, backoff_factor, backoff_cap_s) VALUES ('t', 'queued', '{}',"
            " %(max_attempts)s, %(attempt_timeout_s)s, %(backoff_base_s)s, %(backoff_factor)s,"
            " %(backoff_cap_s)s)",
            row,
        )
