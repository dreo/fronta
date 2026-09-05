"""Live task lifecycle events and task lookup for external observers."""

from __future__ import annotations

import contextlib
import json
import math
from typing import TYPE_CHECKING

import psycopg
from psycopg import sql

from fronta import runtime, store
from fronta.errors import TaskNotFound
from fronta.model import State, TaskEvent, TaskRow

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    from fronta.config import Settings


def _decode(payload: str) -> TaskEvent:
    data = json.loads(payload)
    if not isinstance(data, dict):
        msg = "invalid Fronta event payload"
        raise ValueError(msg)
    task_id, task_type, state = data.get("id"), data.get("type"), data.get("state")
    if (
        not isinstance(task_id, int)
        or isinstance(task_id, bool)
        or not isinstance(task_type, str)
        or not isinstance(state, str)
    ):
        msg = "invalid Fronta event payload"
        raise ValueError(msg)
    return TaskEvent(task_id, task_type, State(state))


async def _stream(conn: store.Conn) -> AsyncGenerator[TaskEvent]:
    async with contextlib.aclosing(conn.notifies()) as notices:
        async for notice in notices:
            yield _decode(notice.payload)


@contextlib.asynccontextmanager
async def subscribe_events(
    settings: Settings | None = None,
) -> AsyncIterator[AsyncIterator[TaskEvent]]:
    """Yield a live stream of committed task state transitions.

    The dedicated connection has completed ``LISTEN`` before this context yields. PostgreSQL
    notifications are best-effort and have no replay: reconnect and reconcile task state after a
    disconnect. Passing settings also configures the process-global SDK used by :func:`get_task`.
    """
    if settings is not None:
        runtime.configure(settings)
    current = runtime.get_settings()
    conn = await psycopg.AsyncConnection.connect(
        runtime.dsn_of(current),
        autocommit=True,
        connect_timeout=max(1, math.ceil(current.connect_timeout_s)),
        application_name="fronta-events",
    )
    async with conn:
        query = sql.SQL("LISTEN {}").format(sql.Identifier(store.EVENTS_CHANNEL))
        await conn.execute(query)
        events = _stream(conn)
        try:
            yield events
        finally:
            # psycopg's notification generator holds the connection lock across yields. Release it
            # before AsyncConnection.__aexit__ tries to finish and close the connection.
            await events.aclose()


async def get_task(task_id: int, *, conn: store.Conn | None = None) -> TaskRow:
    """Return a task's latest durable row; raise :class:`TaskNotFound` when it is absent."""
    if conn is None:
        pool = await runtime.open_pool()
        async with pool.connection() as own_conn:
            row = await store.get_task(own_conn, task_id)
    else:
        row = await store.get_task(conn, task_id)
    if row is None:
        msg = f"no task {task_id}"
        raise TaskNotFound(msg)
    return row
