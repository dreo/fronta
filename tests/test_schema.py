"""`fronta db init` is idempotent and the schema enforces its bounds."""

import asyncio
import re
from importlib import resources

import psycopg
import pytest
from click.testing import CliRunner
from psycopg import sql

from fronta import ConfigurationError, Worker, store
from fronta.cli import main
from fronta.model import NewTask, Policy
from fronta.server.service import Service
from tests.conftest import wait_until
from tests.workers import sleep_task


async def test_init_is_idempotent(conn):
    await store.init_schema(conn)
    await store.init_schema(conn)
    cur = await conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'fronta' ORDER BY 1"
    )
    assert [r[0] for r in await cur.fetchall()] == [
        "events",
        "meta",
        "subscriptions",
        "task_types",
        "tasks",
    ]
    assert await (
        await conn.execute(
            "SELECT data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema='fronta' AND table_name='subscriptions' AND column_name='backfill'"
        )
    ).fetchall() == [("jsonb", "YES")]


async def test_db_init_adds_backfill_without_a_schema_version_change(conn, dsn):
    await conn.execute("ALTER TABLE fronta.subscriptions DROP COLUMN backfill")
    version = await (
        await conn.execute("SELECT value FROM fronta.meta WHERE key='schema_version'")
    ).fetchone()
    try:
        for _ in range(2):
            result = await asyncio.to_thread(CliRunner().invoke, main, ["db", "init", "--dsn", dsn])
            assert result.exit_code == 0, result.output
            assert "ready" in result.output
        assert (
            await (
                await conn.execute("SELECT value FROM fronta.meta WHERE key='schema_version'")
            ).fetchone()
            == version
        )
        assert (
            await (await conn.execute("SELECT backfill FROM fronta.subscriptions")).fetchall() == []
        )
    finally:
        await store.init_schema(conn)


@pytest.mark.parametrize("first_table", ["task_types", "events"])
async def test_init_allows_an_active_writer_to_finish_before_migrating(conn, dsn, first_table):
    async with (
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as writer,
        await psycopg.AsyncConnection.connect(dsn, autocommit=True) as migration,
    ):
        applying = None
        try:
            async with writer.transaction():
                await writer.execute(
                    sql.SQL("SELECT * FROM fronta.{} LIMIT 0").format(sql.Identifier(first_table))
                )
                applying = asyncio.create_task(store.init_schema(migration))

                async def migration_is_waiting():
                    row = await (
                        await conn.execute(
                            "SELECT state='active' AND wait_event IS NOT NULL "
                            "FROM pg_stat_activity WHERE pid=%s",
                            (migration.info.backend_pid,),
                        )
                    ).fetchone()
                    return row[0]

                await wait_until(migration_is_waiting)
                await writer.execute("SET LOCAL lock_timeout='1s'")
                await writer.execute("UPDATE fronta.tasks SET state=state WHERE false")
            await asyncio.wait_for(applying, 5)
            await store.check_schema(conn)
        finally:
            if applying:
                applying.cancel()
                await asyncio.gather(applying, return_exceptions=True)


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
    assert "tasks_type_queue_idx" in names  # direct claim order for a single accepted type
    assert "tasks_lease_idx" not in names  # heartbeats stay heap-only updates
    assert "fillfactor=90" in options
    assert (
        await (await conn.execute("SELECT to_regclass('fronta.events_created_at_idx')")).fetchone()
    )[0]
    # An older schema is brought in place by a rerun, without a table rewrite.
    await conn.execute(
        "CREATE INDEX tasks_lease_idx ON fronta.tasks (lease_until) WHERE state = 'running'"
    )
    await conn.execute("DROP INDEX fronta.tasks_key_idx")
    await conn.execute("DROP INDEX fronta.tasks_type_queue_idx")
    await conn.execute("DROP INDEX fronta.events_created_at_idx")
    await conn.execute("ALTER TABLE fronta.tasks RESET (fillfactor)")
    await store.init_schema(conn)
    assert await _index_design(conn) == (names, options)
    assert (
        await (await conn.execute("SELECT to_regclass('fronta.events_created_at_idx')")).fetchone()
    )[0]


async def test_heartbeats_are_heap_only_updates(conn):
    await store.publish_task_type(conn, sleep_task.spec)
    await store.enqueue(conn, NewTask("sleep", "{}", Policy()))
    row = (
        await store.claim(conn, types=["sleep"], worker="w", lease_s=30, deadline_s=5, count=1)
        or [None]
    )[0]
    assert row is not None
    await conn.execute("SELECT pg_stat_reset()")
    for _ in range(50):
        assert await store.heartbeat(conn, [(row.id, row.token)], 30) == {row.id: None}
    await conn.execute("SELECT pg_stat_force_next_flush()")

    async def counted():
        cur = await conn.execute(
            "SELECT n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables WHERE relname = 'tasks'"
        )
        updates, hot = await cur.fetchone()
        return updates >= 50 and hot >= 45

    await wait_until(counted, timeout=5)


def test_composed_function_embeds_canonical_candidate_verbatim():

    candidate = resources.files("fronta").joinpath("claim_candidate.sql").read_text()
    names = {
        "types": "p_types",
        "skip": "v_skip",
        "skip_keys": "v_skip_keys",
        "skip_types": "v_skip_types",
        "skip_key_types": "v_skip_key_types",
        "count": "(v_cap - cardinality(v_ids))",
    }
    substituted = re.sub(r"%\((\w+)\)s", lambda m: names[m[1]], candidate)
    assert substituted in store.schema_sql()
    assert "{candidate}" not in store.schema_sql()


async def test_schema_version_is_written_and_old_workers_functions_survive_until_pruned(conn):
    previous = f"claim_v{store.SCHEMA_VERSION - 1}"
    await conn.execute(
        sql.SQL("""CREATE FUNCTION fronta.{}(text[],text,float8,float8,int)
        RETURNS SETOF fronta.tasks LANGUAGE sql AS $$
        SELECT * FROM fronta.{}($1,$2,$3,$4,$5) $$""").format(
            sql.Identifier(previous), sql.Identifier(f"claim_v{store.SCHEMA_VERSION}")
        )
    )
    await store.init_schema(conn)
    assert (
        await (
            await conn.execute("SELECT value FROM fronta.meta WHERE key='schema_version'")
        ).fetchone()
    )[0] == str(store.SCHEMA_VERSION)
    await conn.execute(
        sql.SQL("SELECT * FROM fronta.{}('{{}}','old',30,1,1)").format(sql.Identifier(previous))
    )
    await store.init_schema(conn, prune=True)
    names = [
        r[0]
        for r in await (
            await conn.execute(
                "SELECT proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                "WHERE n.nspname='fronta'"
            )
        ).fetchall()
    ]
    assert previous not in names
    assert f"claim_v{store.SCHEMA_VERSION}" in names


async def test_worker_and_server_refuse_an_old_schema_before_publishing(conn, settings):

    previous = store.SCHEMA_VERSION - 1
    await conn.execute(
        "UPDATE fronta.meta SET value=%s WHERE key='schema_version'", (str(previous),)
    )
    service = Service(settings)
    try:
        for start in (Worker([sleep_task], settings=settings).run, service.start):
            with pytest.raises(
                ConfigurationError,
                match=rf"version {previous}.*version {store.SCHEMA_VERSION}.*fronta db init",
            ):
                await start()
        assert await store.get_task_types(conn) == []
        assert service._pool is None
    finally:
        await store.init_schema(conn)


async def test_server_cli_reports_old_schema_without_uvicorn_traceback(conn, dsn, monkeypatch):
    monkeypatch.setenv("FRONTA_DSN", dsn)
    monkeypatch.setenv("FRONTA_SERVER_TOKEN", "test-token")
    await conn.execute("UPDATE fronta.meta SET value='0' WHERE key='schema_version'")
    try:
        result = await asyncio.to_thread(CliRunner().invoke, main, ["server"])
        assert result.exit_code == 1
        assert "fronta db init" in result.output
        assert "Traceback" not in result.output
    finally:
        await store.init_schema(conn)


async def test_db_sql_applies_the_same_catalog_as_init(conn):
    result = CliRunner().invoke(main, ["db", "sql"])
    assert result.exit_code == 0

    async def catalog():
        return await (
            await conn.execute(
                "SELECT c.relname,c.relkind,pg_get_expr(d.adbin,d.adrelid) FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "LEFT JOIN pg_attrdef d ON d.adrelid=c.oid "
                "WHERE n.nspname='fronta' ORDER BY 1,2,3"
            )
        ).fetchall()

    expected = await catalog()
    await conn.execute("DROP SCHEMA fronta CASCADE")
    await conn.execute(result.output)
    assert await catalog() == expected
    await store.check_schema(conn)


async def test_init_does_not_lower_a_newer_schema_version(conn):
    await conn.execute("UPDATE fronta.meta SET value='2' WHERE key='schema_version'")
    try:
        await store.init_schema(conn, prune=True)
        await store.check_schema(conn)
        row = await (
            await conn.execute("SELECT value FROM fronta.meta WHERE key='schema_version'")
        ).fetchone()
        assert row[0] == "2"
    finally:
        await conn.execute("UPDATE fronta.meta SET value='1' WHERE key='schema_version'")
