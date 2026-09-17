"""Queue SQL and its SQL resources, as typed async functions over a psycopg connection.

Transactions: multi-statement operations open a block (a savepoint inside a caller transaction).
Managed transitions are single statements; callers send hints after commit.
`enqueue` is an exception: it only executes statements, so it joins the caller's transaction
and never commits it.
Every worker write is fenced by `state = 'running' AND token = $token`.
"""

from __future__ import annotations

import json
import re
import time
from importlib import resources
from typing import TYPE_CHECKING, Any, TypedDict
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from fronta.errors import (
    ConfigurationError,
    InvalidInput,
    NotRequeueable,
    TaskNotFound,
    UnknownTaskType,
)
from fronta.model import (
    MAX_KEY_BYTES,
    MAX_NAME_BYTES,
    Backoff,
    Executor,
    Policy,
    State,
    TaskRow,
    TaskSummary,
    TaskTypeRow,
)

if TYPE_CHECKING:
    from datetime import datetime, timedelta
    from uuid import UUID

    from fronta.model import Completion, NewTask, TaskFilter, TaskTypeSpec

type Conn = psycopg.AsyncConnection[Any]
"""Any connection; each query sets its own row factory, so the connection row type is moot."""

SCHEMA_VERSION = 1

WAKE_CHANNEL = "fronta_wake"
CANCEL_CHANNEL = "fronta_cancel"


class Backfill(TypedDict):
    generation: str
    since: str | None
    pending: dict[str, int]


_TASK_COLUMNS = (
    "id", "type", "state", "priority", "key", "concurrency_key", "input", "result", "error",
    "progress", "attempt", "failures", "max_attempts", "attempt_timeout_s", "backoff_base_s",
    "backoff_factor", "backoff_cap_s", "token", "lease_until", "worker", "cancel_requested_at",
    "created_at", "run_at", "started_at", "finished_at", "metadata",
)  # fmt: skip

_SUMMARY_COLUMNS = (
    "id", "type", "state", "priority", "key", "concurrency_key", "attempt", "failures",
    "max_attempts", "worker", "cancel_requested_at", "created_at", "run_at", "started_at",
    "finished_at",
)  # fmt: skip

_TASK_TYPE_COLUMNS = (
    "name", "executor", "input_schema", "output_schema", "policy", "fingerprint", "updated_at",
    "paused",
)  # fmt: skip


def _columns(names: tuple[str, ...], prefix: str = "") -> sql.Composed:
    return sql.SQL(", ").join(sql.SQL(prefix) + sql.Identifier(n) for n in names)


_BACKOFF = (
    "now() + make_interval(secs => least({t}backoff_cap_s, {t}backoff_base_s"
    " * power({t}backoff_factor, least({t}failures, 64))) * (0.5 + 0.5 * random()))"
)

_PUBLISHED = """
INSERT INTO fronta.events (subscription, task_id, type, state, attempt)
SELECT s.name, a.id, a.type, a.state, a.attempt
FROM applied a CROSS JOIN LATERAL (
    SELECT s.name FROM fronta.subscriptions s
    WHERE a.state = ANY(s.states) AND (s.types IS NULL OR a.type = ANY(s.types))
    FOR KEY SHARE OF s
) s
"""


def _with_events(query: sql.SQL | sql.Composed, columns: str, *, where: str = "") -> sql.Composed:
    return sql.SQL(
        "WITH applied AS ({query}), published AS ({published}{where}) SELECT {columns} FROM applied"
    ).format(
        query=query,
        published=sql.SQL(_PUBLISHED),
        where=sql.SQL(where),
        columns=sql.SQL(columns),
    )


_ENQUEUE: sql.SQL | sql.Composed = sql.SQL("""
INSERT INTO fronta.tasks (type, state, priority, key, concurrency_key, input, max_attempts,
    attempt_timeout_s, backoff_base_s, backoff_factor, backoff_cap_s, run_at, metadata)
VALUES (%(type)s, 'queued', %(priority)s, %(key)s, %(concurrency_key)s, %(input)s::jsonb,
    %(max_attempts)s, %(attempt_timeout_s)s, %(backoff_base_s)s, %(backoff_factor)s,
    %(backoff_cap_s)s, coalesce(%(run_at)s, now()), %(metadata)s::jsonb)
ON CONFLICT (type, key) WHERE key IS NOT NULL AND state IN ('queued', 'running') DO NOTHING
RETURNING id, type, state, attempt
""")
_ENQUEUE = _with_events(_ENQUEUE, "id")

_FIND_ACTIVE_BY_KEY = sql.SQL("""
SELECT id FROM fronta.tasks
WHERE type = %(type)s AND key = %(key)s AND state IN ('queued', 'running')
""")

# Shared by the server-side claim function and the benchmark EXPLAINs.
_CANDIDATE = sql.SQL(resources.files("fronta").joinpath("claim_candidate.sql").read_text())
_CLAIM = sql.SQL("""
SELECT {cols} FROM fronta.{function}(
    %(types)s::text[], %(worker)s::text, %(lease_s)s::float8, %(deadline_s)s::float8,
    %(count)s::integer)
""").format(cols=_columns(_TASK_COLUMNS), function=sql.Identifier(f"claim_v{SCHEMA_VERSION}"))


def _completion_statement(outcomes: str) -> sql.Composed:
    return sql.SQL(resources.files("fronta").joinpath("complete.sql").read_text()).format(
        outcomes=sql.SQL(outcomes),
        backoff=sql.SQL(_BACKOFF.format(t="t.")),
        published=sql.SQL(_PUBLISHED),
    )


_COMPLETE = _completion_statement("""
    SELECT * FROM unnest(%(ids)s::bigint[], %(tokens)s::uuid[], %(kinds)s::text[], %(data)s::text[])
        AS c(id, token, kind, data)
""")
_ORPHANS = _completion_statement("""
    SELECT id, token, 'release'::text AS kind, NULL::text AS data FROM fronta.tasks
    WHERE worker = %(worker)s AND state = 'running' AND id <> ALL(%(live)s::bigint[])
    ORDER BY id LIMIT 256
""")


# The row is locked first: an UPDATE evaluates its new values before it waits for a row lock, so
# a heartbeat blocked behind another writer would otherwise carry a stamp from before the wait.
_HEARTBEAT = sql.SQL("""
WITH renewals AS (
    SELECT * FROM unnest(%(ids)s::bigint[], %(tokens)s::uuid[]) AS r(id, token)
), locked AS (
    SELECT t.id, t.token FROM fronta.tasks t JOIN renewals r ON t.id = r.id
    WHERE t.state = 'running' AND t.token = r.token ORDER BY t.id FOR UPDATE OF t
)
UPDATE fronta.tasks t
SET lease_until = clock_timestamp() + make_interval(secs => %(lease_s)s)
FROM locked WHERE t.id = locked.id AND t.state = 'running' AND t.token = locked.token
RETURNING t.id, t.cancel_requested_at
""")

_PROGRESS = sql.SQL("""
UPDATE fronta.tasks SET progress = %(progress)s::jsonb
WHERE id = %(id)s AND state = 'running' AND token = %(token)s
RETURNING id
""")

_REQUEST_CANCEL: sql.SQL | sql.Composed = sql.SQL("""
UPDATE fronta.tasks
SET cancel_requested_at = coalesce(cancel_requested_at, now()),
    state = CASE WHEN state = 'queued' THEN 'cancelled' ELSE state END,
    finished_at = CASE WHEN state = 'queued' THEN now() ELSE finished_at END
WHERE id = %(id)s AND state IN ('queued', 'running')
RETURNING id, type, state, attempt
""")
_REQUEST_CANCEL = _with_events(_REQUEST_CANCEL, "type, state", where=" WHERE a.state = 'cancelled'")

_REAP = sql.SQL("""
WITH expired AS (
    SELECT id FROM fronta.tasks WHERE state = 'running' AND lease_until < now()
    ORDER BY lease_until LIMIT %(limit)s FOR UPDATE SKIP LOCKED)
UPDATE fronta.tasks t
SET token = NULL, lease_until = NULL,
    failures = CASE WHEN t.cancel_requested_at IS NULL THEN t.failures + 1 ELSE t.failures END,
    error = CASE WHEN t.cancel_requested_at IS NULL THEN jsonb_build_object(
                'type', 'LeaseExpired',
                'message', 'lease expired: worker ' || coalesce(t.worker, '?')
                           || ' stopped heartbeating',
                'worker', t.worker, 'attempt', t.attempt) ELSE t.error END,
    state = CASE WHEN t.cancel_requested_at IS NOT NULL THEN 'cancelled'
                 WHEN t.failures + 1 < t.max_attempts THEN 'queued' ELSE 'failed' END,
    run_at = CASE WHEN t.cancel_requested_at IS NULL AND t.failures + 1 < t.max_attempts
                  THEN {backoff} ELSE t.run_at END,
    finished_at = CASE WHEN t.cancel_requested_at IS NOT NULL OR t.failures + 1 >= t.max_attempts
                       THEN now() END
FROM expired WHERE t.id = expired.id
RETURNING t.id, t.type, t.state, t.attempt
""").format(backoff=sql.SQL(_BACKOFF.format(t="t.")))
_REAP = _with_events(_REAP, "id, type, state")

_REQUEUE = _with_events(
    sql.SQL("""
UPDATE fronta.tasks SET state = 'queued', run_at = now(), failures = 0,
    cancel_requested_at = NULL, finished_at = NULL, token = NULL, lease_until = NULL
WHERE id = %(id)s AND state IN ('failed', 'cancelled')
RETURNING id, type, state, attempt
"""),
    "type",
)

_STATS = sql.SQL("""
SELECT jsonb_build_object(
    'types', coalesce((SELECT jsonb_agg(to_jsonb(x) ORDER BY x.type) FROM (
        SELECT ty.name AS type,
            count(t.id) FILTER (WHERE t.state='queued' AND t.run_at <= now()) AS queued_due,
            count(t.id) FILTER (WHERE t.state='queued' AND t.run_at > now()) AS queued_scheduled,
            count(t.id) FILTER (WHERE t.state='running') AS running,
            extract(epoch FROM now() - min(t.run_at) FILTER (
                WHERE t.state='queued' AND t.run_at <= now())) AS oldest_due_age_s
        FROM fronta.task_types ty LEFT JOIN fronta.tasks t
            ON t.type=ty.name AND t.state IN ('queued','running') GROUP BY ty.name
    ) x), '[]'::jsonb),
    'subscriptions', coalesce((SELECT jsonb_agg(to_jsonb(x) ORDER BY x.name) FROM (
        SELECT s.name, count(e.seq) AS backlog,
            s.backfill IS NOT NULL AS backfill_pending,
            extract(epoch FROM now()-min(e.created_at)) AS oldest_event_age_s
        FROM fronta.subscriptions s LEFT JOIN fronta.events e ON e.subscription=s.name
        GROUP BY s.name
    ) x), '[]'::jsonb))
""")

_BACKFILL = sql.SQL("""
WITH ins AS (
    INSERT INTO fronta.events (subscription, task_id, type, state, attempt)
    SELECT %(name)s, t.id, t.type, t.state, t.attempt FROM fronta.tasks t
    WHERE t.state = %(state)s AND t.id > %(after)s
      AND (%(types)s::text[] IS NULL OR t.type = ANY(%(types)s::text[]))
      AND (%(since)s::timestamptz IS NULL OR t.finished_at >= %(since)s)
    ORDER BY t.id LIMIT %(limit)s RETURNING task_id)
SELECT count(*), max(task_id) FROM ins
""")

_PURGE_TASKS = sql.SQL("""
DELETE FROM fronta.tasks WHERE id IN (
    SELECT id FROM fronta.tasks
    WHERE state IN ('succeeded', 'failed', 'cancelled')
      AND finished_at < now() - make_interval(secs => %(retention_s)s)
    ORDER BY finished_at LIMIT %(batch)s FOR UPDATE SKIP LOCKED)
""")

_PUBLISH = sql.SQL("""
INSERT INTO fronta.task_types (name, executor, input_schema, output_schema, policy,
    max_concurrency, max_concurrency_per_key, fingerprint, updated_at)
SELECT %(name)s, %(executor)s, %(input_schema)s::jsonb, %(output_schema)s::jsonb,
    %(policy)s::jsonb, %(max_concurrency)s, %(max_concurrency_per_key)s, %(fingerprint)s,
    now()
ON CONFLICT (name) DO UPDATE SET
    executor = EXCLUDED.executor, input_schema = EXCLUDED.input_schema,
    output_schema = EXCLUDED.output_schema, policy = EXCLUDED.policy,
    max_concurrency = EXCLUDED.max_concurrency,
    max_concurrency_per_key = EXCLUDED.max_concurrency_per_key,
    fingerprint = EXCLUDED.fingerprint, updated_at = now()
RETURNING (SELECT t.fingerprint FROM fronta.task_types t WHERE t.name = fronta.task_types.name)
    AS previous_fingerprint
""")

_GET_TASK_TYPES = sql.SQL("SELECT {cols} FROM fronta.task_types ORDER BY name").format(
    cols=_columns(_TASK_TYPE_COLUMNS)
)
_GET_TASK_TYPE = sql.SQL("SELECT {cols} FROM fronta.task_types WHERE name = %(name)s").format(
    cols=_columns(_TASK_TYPE_COLUMNS)
)
_GET_TASK = sql.SQL("SELECT {cols} FROM fronta.tasks WHERE id = %(id)s").format(
    cols=_columns(_TASK_COLUMNS)
)
_LIST_TASKS = sql.SQL(
    "SELECT {cols} FROM fronta.tasks WHERE {conditions} ORDER BY id DESC LIMIT %(limit)s"
)


MIN_PRIORITY = -(2**31)
MAX_PRIORITY = 2**31 - 1
"""`priority` is a PostgreSQL integer."""


def _check_text(value: str, what: str, max_bytes: int) -> None:
    """Text the database will store: 1..max UTF-8 bytes, no NUL, no lone surrogates."""
    if "\x00" in value:
        msg = f"{what} must not contain NUL characters"
        raise InvalidInput(msg)
    try:
        size = len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        msg = f"{what} must be valid Unicode text (lone surrogates cannot be stored)"
        raise InvalidInput(msg) from exc
    if not 1 <= size <= max_bytes:
        msg = f"{what} must be 1..{max_bytes} UTF-8 bytes"
        raise InvalidInput(msg)


def check_name(name: str) -> None:
    _check_text(name, "task type name", MAX_NAME_BYTES)


def check_key(value: str | None, what: str) -> None:
    if value is not None:
        _check_text(value, what, MAX_KEY_BYTES)


def check_priority(value: int) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not (MIN_PRIORITY <= value <= MAX_PRIORITY)
    ):
        msg = f"priority must be an integer in [{MIN_PRIORITY}, {MAX_PRIORITY}], got {value!r}"
        raise InvalidInput(msg)


def _task(rec: dict[str, Any]) -> TaskRow:
    fields = {k: rec[k] for k in _TASK_COLUMNS if not k.startswith("backoff_")}
    fields["state"] = State(rec["state"])
    return TaskRow(
        backoff=Backoff(rec["backoff_base_s"], rec["backoff_factor"], rec["backoff_cap_s"]),
        **fields,
    )


def _summary(rec: dict[str, Any]) -> TaskSummary:
    return TaskSummary(**{**rec, "state": State(rec["state"])})


def _task_type(rec: dict[str, Any]) -> TaskTypeRow:
    return TaskTypeRow(
        name=rec["name"],
        executor=Executor(rec["executor"]),
        input_schema=rec["input_schema"],
        output_schema=rec["output_schema"],
        policy=Policy.from_json(rec["policy"]),
        fingerprint=rec["fingerprint"],
        updated_at=rec["updated_at"],
        paused=rec["paused"],
    )


# ---------------------------------------------------------------------------------------------
# Schema and task types


def candidate_sql() -> str:
    names = {
        "types": "p_types",
        "skip": "v_skip",
        "skip_keys": "v_skip_keys",
        "skip_types": "v_skip_types",
        "skip_key_types": "v_skip_key_types",
        "count": "(v_cap - cardinality(v_ids))",
    }
    return re.sub(r"%\((\w+)\)s", lambda match: names[match[1]], _CANDIDATE.as_string())


def schema_sql() -> str:
    ddl = resources.files("fronta").joinpath("schema.sql").read_text()
    function = resources.files("fronta").joinpath("claim.sql").read_text()
    function = function.replace("{candidate}", candidate_sql()).replace("{published}", _PUBLISHED)
    function = function.replace("{version}", str(SCHEMA_VERSION))
    return (
        ddl + "\n" + function + "\nINSERT INTO fronta.meta (key, value) "
        f"VALUES ('schema_version', '{SCHEMA_VERSION}') "
        "ON CONFLICT (key) DO UPDATE SET value = greatest(\n"
        "fronta.meta.value::int, EXCLUDED.value::int)::text;\n"
    )


async def init_schema(conn: Conn, *, prune: bool = False) -> None:
    async with conn.transaction():
        await conn.execute(sql.SQL(schema_sql()))
        if prune:
            rows = await (
                await conn.execute("""
                SELECT proname, pg_get_function_identity_arguments(p.oid)
                FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = 'fronta'
                  AND (proname ~ '^claim_v[0-9]+$' OR proname = 'claim_tasks')
            """)
            ).fetchall()
            for name, arguments in rows:
                if name == "claim_tasks" or int(name.removeprefix("claim_v")) < SCHEMA_VERSION:
                    await conn.execute(
                        sql.SQL("DROP FUNCTION fronta.{}({})").format(
                            sql.Identifier(name),
                            sql.SQL(arguments),
                        )
                    )


async def check_schema(conn: Conn) -> None:
    try:
        row = await (
            await conn.execute("SELECT value FROM fronta.meta WHERE key = 'schema_version'")
        ).fetchone()
        installed = 0 if row is None else int(row[0])
    except (psycopg.errors.UndefinedTable, ValueError):
        installed = 0
    if installed < SCHEMA_VERSION:
        msg = (
            f"Fronta schema version {installed} is behind required version {SCHEMA_VERSION}; "
            "run `fronta db init` before starting workers or the server"
        )
        raise ConfigurationError(msg)


async def publish_task_type(conn: Conn, spec: TaskTypeSpec) -> str | None:
    """Upsert a definition atomically. Returns the fingerprint it replaced (None when new)."""
    check_name(spec.name)
    params = {
        "name": spec.name,
        "executor": spec.executor.value,
        "input_schema": json.dumps(spec.input_schema),
        "output_schema": None if spec.output_schema is None else json.dumps(spec.output_schema),
        "policy": json.dumps(spec.policy.to_json()),
        "max_concurrency": spec.policy.max_concurrency,
        "max_concurrency_per_key": spec.policy.max_concurrency_per_key,
        "fingerprint": spec.fingerprint,
    }
    cur = await conn.execute(_PUBLISH, params)
    rec = await cur.fetchone()
    return None if rec is None or rec[0] is None else str(rec[0])


async def get_task_types(conn: Conn) -> list[TaskTypeRow]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_GET_TASK_TYPES)
        return [_task_type(rec) for rec in await cur.fetchall()]


async def get_task_type(conn: Conn, name: str) -> TaskTypeRow | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_GET_TASK_TYPE, {"name": name})
        rec = await cur.fetchone()
        return None if rec is None else _task_type(rec)


# ---------------------------------------------------------------------------------------------
# Enqueue


async def enqueue(conn: Conn, task: NewTask, deadline_s: float = 30.0) -> int:
    """Insert a queued task (or return the active task with the same type + key).

    Does not open an explicit transaction. The caller owns the data/notification boundary.
    """
    check_name(task.type)
    check_key(task.key, "key")
    check_key(task.concurrency_key, "concurrency_key")
    check_priority(task.priority)
    if task.run_at is not None and task.run_at.tzinfo is None:
        msg = "run_at must be timezone-aware"
        raise InvalidInput(msg)
    params = {
        "type": task.type,
        "priority": task.priority,
        "key": task.key,
        "concurrency_key": task.concurrency_key,
        "input": task.input_json,
        "max_attempts": task.policy.max_attempts,
        "attempt_timeout_s": task.policy.attempt_timeout_s,
        "backoff_base_s": task.policy.backoff.base_s,
        "backoff_factor": task.policy.backoff.factor,
        "backoff_cap_s": task.policy.backoff.cap_s,
        "run_at": task.run_at,
        "metadata": task.metadata_json,
    }
    deadline = time.monotonic() + deadline_s
    while True:
        cur = await conn.execute(_ENQUEUE, params)
        rec = await cur.fetchone()
        if rec is not None:
            task_id = int(rec[0])
            if (
                not conn.autocommit
                or conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE
            ):
                await conn.execute(
                    "SELECT pg_notify('fronta_wake', %s), pg_notify('fronta_feed', 'queued')",
                    (task.type,),
                )
            return task_id
        # Conflict with an active task of the same key: return its id, untouched.
        cur = await conn.execute(_FIND_ACTIVE_BY_KEY, {"type": task.type, "key": task.key})
        existing = await cur.fetchone()
        if existing is not None:
            return int(existing[0])
        # The conflicting row turned terminal between the two statements: insert again.
        if time.monotonic() > deadline:
            msg = f"enqueue of {task.type!r} with key {task.key!r} kept losing the dedupe race"
            raise TimeoutError(msg)


# ---------------------------------------------------------------------------------------------
# Claim and the fenced writes of a running attempt


async def claim(  # noqa: PLR0913  # distinct claim inputs
    conn: Conn,
    *,
    types: list[str],
    worker: str,
    lease_s: float,
    deadline_s: float,
    count: int,
) -> list[TaskRow]:
    """Claim bounded, globally ordered candidates across accepted types, skipping busy rows."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            _CLAIM,
            {
                "types": types,
                "worker": worker,
                "lease_s": lease_s,
                "deadline_s": deadline_s,
                "count": count,
            },
            binary=True,
        )
        return [_task(rec) for rec in await cur.fetchall()]


async def heartbeat(
    conn: Conn,
    renewals: list[tuple[int, UUID]],
    lease_s: float,
) -> dict[int, datetime | None]:
    if len(renewals) > 1000:  # noqa: PLR2004  # bounded renewal statement
        msg = "heartbeat batches accept at most 1000 ids"
        raise InvalidInput(msg)
    if not renewals:
        return {}
    params = {
        "ids": [i for i, _ in renewals],
        "tokens": [t for _, t in renewals],
        "lease_s": lease_s,
    }
    rows = await (await conn.execute(_HEARTBEAT, params)).fetchall()
    return dict(rows)


async def set_progress(conn: Conn, task_id: int, token: UUID, progress_json: str) -> bool:
    cur = await conn.execute(_PROGRESS, {"id": task_id, "token": token, "progress": progress_json})
    return cur.rowcount == 1


async def complete(conn: Conn, completions: list[Completion]) -> dict[int, State]:
    """Commit a bounded batch; return only ids whose token and transition preconditions applied."""
    if not completions:
        return {}
    if len(completions) > 256 or len({c.id for c in completions}) != len(completions):  # noqa: PLR2004
        msg = "completion batches require at most 256 distinct task ids"
        raise InvalidInput(msg)
    params = {
        "ids": [c.id for c in completions],
        "tokens": [c.token for c in completions],
        "kinds": [c.kind for c in completions],
        "data": [c.data for c in completions],
    }
    records = await (await conn.execute(_COMPLETE, params)).fetchall()
    return {task_id: State(state) for task_id, _typ, state in records}


# ---------------------------------------------------------------------------------------------
# Control plane


async def release_orphans(conn: Conn, worker: str, live: list[int]) -> list[tuple[int, str, State]]:
    rows = await (await conn.execute(_ORPHANS, {"worker": worker, "live": live})).fetchall()
    return [(int(i), str(t), State(s)) for i, t, s in rows]


async def request_cancel(conn: Conn, task_id: int) -> State | None:
    """Cancel a queued task at once or flag a running one. None when unknown or terminal."""
    rec = await (await conn.execute(_REQUEST_CANCEL, {"id": task_id})).fetchone()
    return None if rec is None else State(rec[1])


async def reap(conn: Conn, limit: int = 100) -> list[tuple[int, str, State]]:
    records = await (await conn.execute(_REAP, {"limit": limit})).fetchall()
    return [(int(i), str(t), State(s)) for i, t, s in records]


async def set_paused(conn: Conn, task_type: str, paused: bool) -> None:
    check_name(task_type)
    cur = await conn.execute(
        "UPDATE fronta.task_types SET paused = %s WHERE name = %s", (paused, task_type)
    )
    if cur.rowcount == 0:
        raise UnknownTaskType(f"unknown task type {task_type!r}")


async def requeue(conn: Conn, task_id: int) -> str:
    try:
        row = await (await conn.execute(_REQUEUE, {"id": task_id})).fetchone()
    except psycopg.errors.UniqueViolation as exc:
        raise NotRequeueable(f"task {task_id} has an active duplicate") from exc
    if row is not None:
        return str(row[0])
    task = await get_task(conn, task_id)
    if task is None:
        raise TaskNotFound(f"no task {task_id}")
    raise NotRequeueable(
        f"task {task_id} is {task.state.value}; only failed or cancelled tasks can be requeued"
    )


async def stats(conn: Conn) -> dict[str, Any]:
    row = await (await conn.execute(_STATS)).fetchone()
    assert row is not None  # noqa: S101  # SELECT always returns one JSON object
    return dict(row[0])


async def register_subscription(
    conn: Conn,
    name: str,
    states: list[str],
    types: list[str] | None,
    backfill: bool | float = False,
) -> Backfill | None:
    # Only INSERT installs a marker. Concurrent creators/resumers return the same generation
    # and progress; changing filters never starts another backfill or moves its time window.
    window_s = None if isinstance(backfill, bool) else backfill
    row = await (
        await conn.execute(
            "INSERT INTO fronta.subscriptions (name, states, types, backfill) "
            "VALUES (%s, %s, %s, CASE WHEN %s THEN "
            "jsonb_build_object('generation', %s::text, 'since', "
            "CASE WHEN %s::float8 IS NOT NULL THEN "
            "clock_timestamp() - make_interval(secs => %s) END, 'pending', %s::jsonb) END) "
            "ON CONFLICT (name) DO UPDATE SET states = EXCLUDED.states, types = EXCLUDED.types "
            "RETURNING backfill",
            (
                name,
                states,
                types,
                backfill is not False,
                str(uuid4()),
                window_s,
                window_s,
                json.dumps(dict.fromkeys(states, 0)),
            ),
        )
    ).fetchone()
    assert row is not None  # noqa: S101  # upsert returns the marker
    return (
        None
        if row[0] is None
        else Backfill(
            generation=row[0]["generation"], since=row[0]["since"], pending=row[0]["pending"]
        )
    )


async def backfill_blockers(
    conn: Conn, transactions: list[str] | None = None
) -> list[tuple[str, int, str | None, str | None, timedelta | None]]:
    """Capture owned virtual IDs, or inspect the remaining locks of a fixed captured set."""
    # Virtual IDs exist before snapshots/real XIDs and survive idle-in-transaction. Unlike
    # activity timestamps they need neither monitoring privileges nor track_activities.
    # pg_locks.database is NULL for virtual IDs: activity's public pid/datname scope the DB.
    # The other activity columns are diagnostics only and may be hidden or disabled.
    return await (
        await conn.execute(
            "SELECT l.virtualxid, l.pid, a.usename, a.application_name, "
            "clock_timestamp() - a.xact_start FROM pg_locks l "
            "JOIN pg_stat_activity a ON a.pid = l.pid "
            "WHERE l.locktype = 'virtualxid' AND l.mode = 'ExclusiveLock' AND l.granted "
            "AND l.pid <> pg_backend_pid() AND a.datname = current_database() "
            "AND (%s::text[] IS NULL OR l.virtualxid = ANY(%s::text[]))",
            (transactions, transactions),
        )
    ).fetchall()


async def backfill_chunk(
    conn: Conn, name: str, generation: str, limit: int
) -> tuple[int, bool] | None:
    """Commit events and progress together; None means the registration was removed/replaced."""
    async with conn.transaction():
        row = await (
            await conn.execute(
                "SELECT types, backfill FROM fronta.subscriptions "
                "WHERE name = %s FOR NO KEY UPDATE",
                (name,),
            )
        ).fetchone()
        if row is None:
            return None
        if row[1] is None:
            return 0, False
        types, marker = row
        # A cleared barrier for a deleted registration cannot authorize chunks for a new one
        # with the same name. Check under the row lock before inserting or clearing anything.
        if marker["generation"] != generation:
            return None
        pending = marker["pending"]
        count = 0
        if pending:
            state = min(pending)
            after = pending[state]
            inserted = await (
                await conn.execute(
                    _BACKFILL,
                    {
                        "name": name,
                        "state": state,
                        "after": after,
                        "types": types,
                        "since": None
                        if state in (State.QUEUED, State.RUNNING)
                        else marker["since"],
                        "limit": limit,
                    },
                )
            ).fetchone()
            assert inserted is not None  # noqa: S101  # aggregate always returns a row
            count, last_id = inserted
            if count < limit:
                del pending[state]
            else:
                pending[state] = last_id
        await conn.execute(
            "UPDATE fronta.subscriptions SET backfill = %s::jsonb WHERE name = %s",
            (json.dumps(marker) if pending else None, name),
        )
        return count, bool(pending)


async def purge_tasks(conn: Conn, retention_s: float, batch: int) -> int:
    cur = await conn.execute(_PURGE_TASKS, {"retention_s": retention_s, "batch": batch})
    return cur.rowcount


async def purge_events(conn: Conn, retention_s: float, batch: int) -> list[tuple[str, int]]:
    rows = await (
        await conn.execute(
            """
        WITH deleted AS (
            DELETE FROM fronta.events WHERE (subscription, seq) IN (
                SELECT subscription, seq FROM fronta.events
                WHERE created_at < now() - make_interval(secs => %s)
                LIMIT %s FOR UPDATE SKIP LOCKED)
            RETURNING subscription)
        SELECT subscription, count(*) FROM deleted GROUP BY subscription
    """,
            (retention_s, batch),
        )
    ).fetchall()
    return [(str(name), int(count)) for name, count in rows]


# ---------------------------------------------------------------------------------------------
# Reads


async def get_task(conn: Conn, task_id: int) -> TaskRow | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_GET_TASK, {"id": task_id})
        rec = await cur.fetchone()
        return None if rec is None else _task(rec)


async def list_tasks(conn: Conn, flt: TaskFilter) -> list[TaskSummary]:
    """Newest first, keyset by id: pass the last id seen as `before` for the next page."""
    conditions = [sql.SQL("TRUE")]
    params: dict[str, Any] = {"limit": flt.limit}
    if flt.type is not None:
        conditions.append(sql.SQL("type = %(type)s"))
        params["type"] = flt.type
    if flt.state is not None:
        conditions.append(sql.SQL("state = %(state)s"))
        params["state"] = flt.state.value
    if flt.key is not None:
        conditions.append(sql.SQL("key = %(key)s"))
        params["key"] = flt.key
    if flt.before is not None:
        conditions.append(sql.SQL("id < %(before)s"))
        params["before"] = flt.before
    query = _LIST_TASKS.format(
        cols=_columns(_SUMMARY_COLUMNS), conditions=sql.SQL(" AND ").join(conditions)
    )
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return [_summary(rec) for rec in await cur.fetchall()]
