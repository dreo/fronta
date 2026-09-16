"""Durable, delete-on-ack subscriptions to committed task transitions."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import psycopg

from fronta import runtime, store
from fronta.model import State, TaskEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from fronta.config import Settings


class Batch:
    def __init__(self, conn: store.Conn, name: str, events: list[TaskEvent]) -> None:
        self.conn, self.name, self.events = conn, name, events
        self._active = True

    async def ack(self) -> None:
        """Delete this delivery and commit its transaction, including reactions on `conn`."""
        if not self._active:
            msg = "batch is no longer active"
            raise RuntimeError(msg)
        await self.conn.execute(
            "DELETE FROM fronta.events WHERE subscription = %s AND seq = ANY(%s)",
            (self.name, [e.seq for e in self.events]),
        )
        await self.conn.commit()
        self._active = False


class Feed:
    def __init__(self, conn: store.Conn, name: str, settings: Settings, batch_size: int) -> None:
        self.conn, self.name, self.settings, self.batch_size = conn, name, settings, batch_size
        self._batch: Batch | None = None

    def __aiter__(self) -> Feed:
        return self

    async def _rollback(self) -> None:
        if self._batch is not None:
            self._batch._active = False
            self._batch = None
        await self.conn.rollback()

    async def __anext__(self) -> Batch:
        await self._rollback()
        try:
            while True:
                # Notices are level-triggered hints: discard the backlog before checking rows.
                async with contextlib.aclosing(self.conn.notifies(timeout=0)) as pending:
                    async for _ in pending:
                        pass
                registration = await (
                    await self.conn.execute(
                        "SELECT name FROM fronta.subscriptions WHERE name = %s FOR KEY SHARE",
                        (self.name,),
                    )
                ).fetchone()
                if registration is None:
                    raise StopAsyncIteration
                rows = await (
                    await self.conn.execute(
                        "SELECT seq, task_id, type, state, attempt FROM fronta.events "
                        "WHERE subscription = %s ORDER BY seq LIMIT %s FOR UPDATE SKIP LOCKED",
                        (self.name, self.batch_size),
                    )
                ).fetchall()
                if rows:
                    self._batch = Batch(
                        self.conn,
                        self.name,
                        [
                            TaskEvent(seq, task_id, typ, State(state), attempt)
                            for seq, task_id, typ, state, attempt in rows
                        ],
                    )
                    return self._batch
                await self.conn.rollback()
                async with contextlib.aclosing(
                    self.conn.notifies(
                        timeout=self.settings.poll_interval_s,
                        stop_after=1,
                    )
                ) as notices:
                    async for _ in notices:
                        break
        except BaseException:
            await self._rollback()
            raise


@contextlib.asynccontextmanager
async def subscribe(
    name: str,
    *,
    states: Sequence[State | str] = (State.SUCCEEDED, State.FAILED, State.CANCELLED),
    types: Sequence[str] | None = None,
    settings: Settings | None = None,
    batch_size: int = 256,
) -> AsyncIterator[Feed]:
    """Register filters and consume available rows in sequence order, without a cursor."""
    store.check_name(name)
    selected = [State(s).value for s in states]
    for typ in types or ():
        store.check_name(typ)
    if not 1 <= batch_size <= 1000:  # noqa: PLR2004  # bounded delivery
        msg = "feed batch_size must be between 1 and 1000"
        raise ValueError(msg)
    current = settings or runtime.get_settings()
    async with await psycopg.AsyncConnection.connect(
        runtime.dsn_of(current),
        **runtime.connection_kwargs(current, "fronta-feed"),
    ) as conn:
        await conn.execute("LISTEN fronta_feed")
        await conn.execute(
            "INSERT INTO fronta.subscriptions (name, states, types) VALUES (%s, %s, %s) "
            "ON CONFLICT (name) DO UPDATE SET states = EXCLUDED.states, types = EXCLUDED.types",
            (name, selected, None if types is None else list(types)),
        )
        await conn.set_autocommit(False)
        feed = Feed(conn, name, current, batch_size)
        try:
            yield feed
        finally:
            await feed._rollback()


async def unsubscribe(name: str) -> None:
    """Remove a subscription and its backlog; coordinate with transitions already in flight."""
    store.check_name(name)
    pool = await runtime.open_pool()
    async with pool.connection() as conn, conn.transaction():
        # Publishers and deliveries lock this registration before writing/locking events.
        # Only this subscription is blocked; no old reader can insert after its deletion.
        await conn.execute("DELETE FROM fronta.subscriptions WHERE name = %s", (name,))
        await conn.execute("DELETE FROM fronta.events WHERE subscription = %s", (name,))
