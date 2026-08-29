"""Every SQL statement of Fronta, as typed async functions over a psycopg connection.

Transactions: functions that must be atomic open their own transaction block on the given
connection (nested inside a caller's transaction that becomes a savepoint). `enqueue` is the
exception: it only executes statements, so it joins the caller's transaction and never commits it.
Every worker write is fenced by `state = 'running' AND token = $token`.
"""

from __future__ import annotations

import json
import time
from enum import StrEnum
from importlib import resources
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

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
    from fronta.model import NewTask, TaskFilter, TaskTypeSpec

type Conn = psycopg.AsyncConnection[Any]
"""Any connection; each query sets its own row factory, so the connection row type is moot."""

WAKE_CHANNEL = "fronta_wake"
CANCEL_CHANNEL = "fronta_cancel"
EVENTS_CHANNEL = "fronta_events"

_TASK_COLUMNS = (
    "id", "type", "state", "priority", "key", "concurrency_key", "input", "result", "error",
    "progress", "attempt", "failures", "max_attempts", "attempt_timeout_s", "backoff_base_s",
    "backoff_factor", "backoff_cap_s", "token", "lease_until", "worker", "cancel_requested_at",
    "created_at", "run_at", "started_at", "finished_at",
)  # fmt: skip

_SUMMARY_COLUMNS = (
    "id", "type", "state", "priority", "key", "concurrency_key", "attempt", "failures",
    "max_attempts", "worker", "cancel_requested_at", "created_at", "run_at", "started_at",
    "finished_at",
)  # fmt: skip

_TASK_TYPE_COLUMNS = (
    "name", "executor", "input_schema", "output_schema", "policy", "fingerprint", "updated_at",
)  # fmt: skip


def _columns(names: tuple[str, ...], prefix: str = "") -> sql.Composed:
    return sql.SQL(", ").join(sql.SQL(prefix) + sql.Identifier(n) for n in names)


_BACKOFF = (
    "now() + make_interval(secs => least({t}backoff_cap_s, {t}backoff_base_s"
    " * power({t}backoff_factor, least({t}failures, 64))) * (0.5 + 0.5 * random()))"
)

_ENQUEUE = sql.SQL("""
INSERT INTO fronta.tasks (type, state, priority, key, concurrency_key, input, max_attempts,
    attempt_timeout_s, backoff_base_s, backoff_factor, backoff_cap_s, run_at)
VALUES (%(type)s, 'queued', %(priority)s, %(key)s, %(concurrency_key)s, %(input)s::jsonb,
    %(max_attempts)s, %(attempt_timeout_s)s, %(backoff_base_s)s, %(backoff_factor)s,
    %(backoff_cap_s)s, coalesce(%(run_at)s, now()))
ON CONFLICT (type, key) WHERE key IS NOT NULL AND state IN ('queued', 'running') DO NOTHING
RETURNING id
""")

_FIND_ACTIVE_BY_KEY = sql.SQL("""
SELECT id FROM fronta.tasks
WHERE type = %(type)s AND key = %(key)s AND state IN ('queued', 'running')
""")

_CANDIDATE = sql.SQL("""
SELECT {cols}
FROM fronta.tasks t
WHERE t.state = 'queued' AND t.run_at <= now() AND t.type = ANY(%(types)s)
  AND t.id <> ALL(%(skip)s)
  AND EXISTS (
    SELECT 1 FROM fronta.task_types ty
    WHERE ty.name = t.type
      AND (ty.max_concurrency IS NULL OR ty.max_concurrency > (
            SELECT count(*) FROM fronta.tasks r WHERE r.type = t.type AND r.state = 'running'))
      AND (t.concurrency_key IS NULL OR ty.max_concurrency_per_key IS NULL
           OR ty.max_concurrency_per_key > (
            SELECT count(*) FROM fronta.tasks r
            WHERE r.type = t.type AND r.concurrency_key = t.concurrency_key
              AND r.state = 'running')))
ORDER BY t.priority DESC, t.run_at, t.id
LIMIT 1 FOR UPDATE OF t SKIP LOCKED
""").format(cols=_columns(_TASK_COLUMNS, "t."))

_READ_LIMITS = sql.SQL(
    "SELECT max_concurrency, max_concurrency_per_key FROM fronta.task_types WHERE name = %(type)s"
)
_SHARE_LIMITS = _READ_LIMITS + sql.SQL(" FOR SHARE")
_LOCK_LIMITS = _READ_LIMITS + sql.SQL(" FOR UPDATE")

_COUNT_RUNNING = sql.SQL("""
SELECT count(*) FILTER (WHERE TRUE) AS by_type,
       count(*) FILTER (WHERE concurrency_key IS NOT DISTINCT FROM %(key)s) AS by_key
FROM fronta.tasks WHERE type = %(type)s AND state = 'running'
""")

_START = sql.SQL("""
UPDATE fronta.tasks
SET state = 'running', attempt = attempt + 1, token = %(token)s,
    lease_until = now() + make_interval(secs => %(lease_s)s), started_at = now(),
    worker = %(worker)s, progress = NULL
WHERE id = %(id)s
RETURNING {cols}
""").format(cols=_columns(_TASK_COLUMNS))

_HEARTBEAT = sql.SQL("""
UPDATE fronta.tasks SET lease_until = now() + make_interval(secs => %(lease_s)s)
WHERE id = %(id)s AND state = 'running' AND token = %(token)s
RETURNING cancel_requested_at
""")

_PROGRESS = sql.SQL("""
UPDATE fronta.tasks SET progress = %(progress)s::jsonb
WHERE id = %(id)s AND state = 'running' AND token = %(token)s
RETURNING id
""")

_SUCCEED = sql.SQL("""
UPDATE fronta.tasks
SET state = 'succeeded', result = %(result)s::jsonb, finished_at = now(),
    token = NULL, lease_until = NULL
WHERE id = %(id)s AND state = 'running' AND token = %(token)s
RETURNING type, state
""")

_FAIL = sql.SQL("""
UPDATE fronta.tasks
SET failures = failures + 1, error = %(error)s::jsonb, token = NULL, lease_until = NULL,
    state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled'
                 WHEN failures + 1 < max_attempts THEN 'queued' ELSE 'failed' END,
    run_at = CASE WHEN cancel_requested_at IS NULL AND failures + 1 < max_attempts
                  THEN {backoff} ELSE run_at END,
    finished_at = CASE WHEN cancel_requested_at IS NOT NULL OR failures + 1 >= max_attempts
                       THEN now() END
WHERE id = %(id)s AND state = 'running' AND token = %(token)s
RETURNING type, state
""").format(backoff=sql.SQL(_BACKOFF.format(t="")))

_FAIL_FINAL = sql.SQL("""
UPDATE fronta.tasks
SET failures = failures + 1, error = %(error)s::jsonb, token = NULL, lease_until = NULL,
    state = 'failed', finished_at = now()
WHERE id = %(id)s AND state = 'running' AND token = %(token)s
RETURNING type, state
""")

_RELEASE = sql.SQL("""
UPDATE fronta.tasks
SET token = NULL, lease_until = NULL, run_at = now(),
    state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'queued' END,
    finished_at = CASE WHEN cancel_requested_at IS NOT NULL THEN now() END
WHERE id = %(id)s AND state = 'running' AND token = %(token)s
RETURNING type, state
""")

_REQUEST_CANCEL = sql.SQL("""
UPDATE fronta.tasks
SET cancel_requested_at = coalesce(cancel_requested_at, now()),
    state = CASE WHEN state = 'queued' THEN 'cancelled' ELSE state END,
    finished_at = CASE WHEN state = 'queued' THEN now() ELSE finished_at END
WHERE id = %(id)s AND state IN ('queued', 'running')
RETURNING type, state
""")

_ACK_CANCEL = sql.SQL("""
UPDATE fronta.tasks
SET state = 'cancelled', finished_at = now(), token = NULL, lease_until = NULL
WHERE id = %(id)s AND state = 'running' AND token = %(token)s AND cancel_requested_at IS NOT NULL
RETURNING type, state
""")

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
RETURNING t.id, t.type, t.state
""").format(backoff=sql.SQL(_BACKOFF.format(t="t.")))

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
VALUES (%(name)s, %(executor)s, %(input_schema)s::jsonb, %(output_schema)s::jsonb,
    %(policy)s::jsonb, %(max_concurrency)s, %(max_concurrency_per_key)s, %(fingerprint)s, now())
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


class Heartbeat(StrEnum):
    ALIVE = "alive"
    CANCEL_REQUESTED = "cancel_requested"
    LOST = "lost"


def new_token() -> UUID:
    return uuid4()


def check_name(name: str) -> None:
    if not 1 <= len(name.encode("utf-8")) <= MAX_NAME_BYTES:
        msg = f"task type name must be 1..{MAX_NAME_BYTES} UTF-8 bytes"
        raise ValueError(msg)


def check_key(value: str | None, what: str) -> None:
    if value is not None and not 1 <= len(value.encode("utf-8")) <= MAX_KEY_BYTES:
        msg = f"{what} must be 1..{MAX_KEY_BYTES} UTF-8 bytes"
        raise ValueError(msg)


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
    )


async def _notify(conn: Conn, channel: str, payload: str) -> None:
    await conn.execute("SELECT pg_notify(%s, %s)", (channel, payload))


async def _event(conn: Conn, task_id: int, task_type: str, state: State) -> None:
    payload = json.dumps(
        {"id": task_id, "type": task_type, "state": state.value}, separators=(",", ":")
    )
    await _notify(conn, EVENTS_CHANNEL, payload)


# ---------------------------------------------------------------------------------------------
# Schema and task types


async def init_schema(conn: Conn) -> None:
    """Apply `schema.sql`; safe to run repeatedly."""
    ddl = resources.files("fronta").joinpath("schema.sql").read_text(encoding="utf-8")
    await conn.execute(sql.SQL(ddl))


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

    Runs inside whatever transaction the connection is in and never commits. NOTIFY fires when
    that transaction commits.
    """
    check_name(task.type)
    check_key(task.key, "key")
    check_key(task.concurrency_key, "concurrency_key")
    if task.run_at is not None and task.run_at.tzinfo is None:
        msg = "run_at must be timezone-aware"
        raise ValueError(msg)
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
    }
    deadline = time.monotonic() + deadline_s
    while True:
        cur = await conn.execute(_ENQUEUE, params)
        rec = await cur.fetchone()
        if rec is not None:
            task_id = int(rec[0])
            await _notify(conn, WAKE_CHANNEL, task.type)
            await _event(conn, task_id, task.type, State.QUEUED)
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


async def claim(
    conn: Conn, *, types: list[str], worker: str, lease_s: float, deadline_s: float
) -> TaskRow | None:
    """Claim one eligible task or return None. Each round is one transaction.

    The candidate query walks the queue index in claim order and stops at the first eligible
    row. Limits are enforced exactly (`_within_limits`): claims of a limited type serialize on its
    `task_types` row and recount the running tasks against the current limits; claims of an
    unlimited type only share-lock the row, so a concurrent publish that enables a limit waits
    for them. A candidate that lost the race is skipped for the rest of this call; the call ends
    when no candidate is left or `deadline_s` passed.
    """
    skip: list[int] = []
    deadline = time.monotonic() + deadline_s
    while True:
        async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_CANDIDATE, {"types": types, "skip": skip})
            rec = await cur.fetchone()
            if rec is None:
                return None
            task_id: int = rec["id"]
            task_type: str = rec["type"]
            if not await _within_limits(conn, task_type, rec["concurrency_key"]):
                skip.append(task_id)
                raise psycopg.Rollback
            await cur.execute(
                _START,
                {"token": new_token(), "lease_s": lease_s, "worker": worker, "id": task_id},
            )
            started = await cur.fetchone()
            if started is None:  # pragma: no cover  # impossible: the row is locked by us
                raise psycopg.Rollback
            await _event(conn, task_id, task_type, State.RUNNING)
            return _task(started)
        if time.monotonic() > deadline:
            return None


async def _within_limits(conn: Conn, task_type: str, key: str | None) -> bool:
    params = {"type": task_type, "key": key}
    limits = await (await conn.execute(_READ_LIMITS, params)).fetchone()
    if limits is None:
        return False  # unpublished meanwhile
    if limits[0] is None and (limits[1] is None or key is None):
        # Unlimited: a share lock keeps a concurrent publish from enabling a limit under us.
        shared = await (await conn.execute(_SHARE_LIMITS, params)).fetchone()
        return shared is not None and shared[0] is None and (shared[1] is None or key is None)
    locked = await (await conn.execute(_LOCK_LIMITS, params)).fetchone()
    if locked is None:
        return False
    type_limit, key_limit = locked
    counts = await (await conn.execute(_COUNT_RUNNING, params)).fetchone()
    by_type, by_key = (0, 0) if counts is None else (int(counts[0]), int(counts[1]))
    if type_limit is not None and by_type >= type_limit:
        return False
    return key is None or key_limit is None or by_key < key_limit


async def heartbeat(conn: Conn, task_id: int, token: UUID, lease_s: float) -> Heartbeat:
    cur = await conn.execute(_HEARTBEAT, {"id": task_id, "token": token, "lease_s": lease_s})
    rec = await cur.fetchone()
    if rec is None:
        return Heartbeat.LOST
    return Heartbeat.ALIVE if rec[0] is None else Heartbeat.CANCEL_REQUESTED


async def set_progress(conn: Conn, task_id: int, token: UUID, progress_json: str) -> bool:
    cur = await conn.execute(_PROGRESS, {"id": task_id, "token": token, "progress": progress_json})
    return cur.rowcount == 1


async def _finish(
    conn: Conn, query: sql.SQL | sql.Composed, params: dict[str, Any]
) -> State | None:
    """Run a fenced transition and emit its event in one transaction."""
    async with conn.transaction():
        cur = await conn.execute(query, params)
        rec = await cur.fetchone()
        if rec is None:
            return None
        task_type, state = str(rec[0]), State(rec[1])
        await _event(conn, params["id"], task_type, state)
        if state is State.QUEUED:
            await _notify(conn, WAKE_CHANNEL, task_type)
        return state


async def succeed(conn: Conn, task_id: int, token: UUID, result_json: str) -> bool:
    state = await _finish(conn, _SUCCEED, {"id": task_id, "token": token, "result": result_json})
    return state is not None


async def fail(
    conn: Conn, task_id: int, token: UUID, error_json: str, *, retry: bool
) -> State | None:
    """Charge a failure. Returns the resulting state, or None when the token check failed."""
    query = _FAIL if retry else _FAIL_FINAL
    return await _finish(conn, query, {"id": task_id, "token": token, "error": error_json})


async def release(conn: Conn, task_id: int, token: UUID) -> State | None:
    """Give the task back (shutdown): queued now, or cancelled when a cancel is pending."""
    return await _finish(conn, _RELEASE, {"id": task_id, "token": token})


async def ack_cancel(conn: Conn, task_id: int, token: UUID) -> bool:
    state = await _finish(conn, _ACK_CANCEL, {"id": task_id, "token": token})
    return state is not None


# ---------------------------------------------------------------------------------------------
# Control plane


async def request_cancel(conn: Conn, task_id: int) -> State | None:
    """Cancel a queued task at once or flag a running one. None when unknown or terminal."""
    async with conn.transaction():
        cur = await conn.execute(_REQUEST_CANCEL, {"id": task_id})
        rec = await cur.fetchone()
        if rec is None:
            return None
        task_type, state = str(rec[0]), State(rec[1])
        if state is State.RUNNING:
            await _notify(conn, CANCEL_CHANNEL, str(task_id))
        else:
            await _event(conn, task_id, task_type, state)
        return state


async def reap(conn: Conn, limit: int = 100) -> list[tuple[int, str, State]]:
    """Requeue, fail or cancel running tasks whose lease expired."""
    async with conn.transaction():
        cur = await conn.execute(_REAP, {"limit": limit})
        rows = [(int(r[0]), str(r[1]), State(r[2])) for r in await cur.fetchall()]
        if rows:
            for task_id, task_type, state in rows:
                await _event(conn, task_id, task_type, state)
            for task_type in {r[1] for r in rows if r[2] is State.QUEUED}:
                await _notify(conn, WAKE_CHANNEL, task_type)
        return rows


async def purge_tasks(conn: Conn, retention_s: float, batch: int) -> int:
    """Delete one batch of terminal tasks older than the retention. Returns the rows deleted."""
    async with conn.transaction():
        cur = await conn.execute(_PURGE_TASKS, {"retention_s": retention_s, "batch": batch})
        return cur.rowcount


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
