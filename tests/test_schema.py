"""`fronta db init` is idempotent and the schema enforces its bounds."""

import psycopg
import pytest
from click.testing import CliRunner

from fronta import store
from fronta.cli import main
from fronta.model import NewTask, Policy
from tests.conftest import wait_until
from tests.workers import sleep_task


async def test_init_is_idempotent(conn):
    await store.init_schema(conn)
    await store.init_schema(conn)
    cur = await conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'fronta' ORDER BY 1"
    )
    assert [r[0] for r in await cur.fetchall()] == ["task_types", "tasks"]


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


async def _index_design(conn):
    cur = await conn.execute(
        "SELECT indexname FROM pg_indexes WHERE schemaname = 'fronta' AND tablename = 'tasks'"
    )
    names = {row[0] for row in await cur.fetchall()}
    cur = await conn.execute("SELECT reloptions FROM pg_class WHERE oid = 'fronta.tasks'::regclass")
    return names, (await cur.fetchone())[0] or []


async def test_init_applies_and_upgrades_the_measured_index_design(conn):
    names, options = await _index_design(conn)
    assert "tasks_key_idx" in names  # historical key filtering (measured: 24 ms -> 0.3 ms)
    assert "tasks_lease_idx" not in names  # heartbeats stay heap-only updates
    assert "fillfactor=90" in options
    # An older schema is brought in place by a rerun, without a table rewrite.
    await conn.execute(
        "CREATE INDEX tasks_lease_idx ON fronta.tasks (lease_until) WHERE state = 'running'"
    )
    await conn.execute("DROP INDEX fronta.tasks_key_idx")
    await conn.execute("ALTER TABLE fronta.tasks RESET (fillfactor)")
    await store.init_schema(conn)
    assert await _index_design(conn) == (names, options)


async def test_heartbeats_are_heap_only_updates(conn):
    await store.publish_task_type(conn, sleep_task.spec)
    await store.enqueue(conn, NewTask("sleep", "{}", Policy()))
    row = await store.claim(conn, types=["sleep"], worker="w", lease_s=30, deadline_s=5)
    assert row is not None
    await conn.execute("SELECT pg_stat_reset()")
    for _ in range(50):
        assert await store.heartbeat(conn, row.id, row.token, 30) is store.Heartbeat.ALIVE
    await conn.execute("SELECT pg_stat_force_next_flush()")

    async def counted():
        cur = await conn.execute(
            "SELECT n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables WHERE relname = 'tasks'"
        )
        updates, hot = await cur.fetchone()
        return updates >= 50 and hot >= 45

    await wait_until(counted, timeout=5)
