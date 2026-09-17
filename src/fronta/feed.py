"""Durable, delete-on-ack subscriptions to committed task transitions."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import time
from datetime import timedelta
from typing import TYPE_CHECKING

import psycopg

from fronta import runtime, store
from fronta.errors import ConfigurationError
from fronta.model import State, TaskEvent

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from uuid import UUID

    from fronta.config import Settings


log = logging.getLogger(__name__)

_BACKFILL_CHUNK = 5_000
_BACKFILL_POLL_S = 0.25
_BACKFILL_WARN_S = 5.0


def _normalize_backfill(backfill: bool | float | timedelta | None) -> bool | float:
    if backfill is None or isinstance(backfill, bool):
        return bool(backfill)
    window = backfill.total_seconds() if isinstance(backfill, timedelta) else backfill
    if not isinstance(window, (int, float)) or not 0 <= window <= sys.float_info.max:
        msg = "backfill must be a boolean or a finite, nonnegative duration in seconds"
        raise ValueError(msg)
    return float(window)


async def _register(
    conn: store.Conn,
    name: str,
    states: list[str],
    types: list[str] | None,
    *,
    backfill: bool | float,
) -> store.Subscription:
    try:
        return await store.register_subscription(conn, name, states, types, backfill)
    except psycopg.errors.UndefinedColumn as exc:
        msg = "Feed subscriptions require a schema upgrade; run `fronta db init`"
        raise ConfigurationError(msg) from exc


async def _wait_for_transactions(conn: store.Conn, name: str, generation: UUID) -> bool:
    # Registration has committed. Every transaction that could have read the old subscription
    # set is either finished or owns one of these locks. Capture once: later transactions see
    # the registration and must not extend this wait. Autocommit releases our own transaction
    # between polls. A resumer captures afresh; no persisted transaction IDs are needed.
    blockers = await store.backfill_blockers(conn)
    transactions = [row[0] for row in blockers]
    warn_at = time.monotonic() + _BACKFILL_WARN_S
    while blockers:
        # An obsolete consumer need not wait for an unrelated old transaction to finish.
        registration = await (
            await conn.execute(
                "SELECT generation FROM fronta.subscriptions WHERE name = %s", (name,)
            )
        ).fetchone()
        if registration is None or registration[0] != generation:
            return False
        if time.monotonic() >= warn_at:
            for vxid, pid, user, application, age in blockers:
                log.warning(
                    "feed backfill waiting name=%s virtualxid=%s pid=%s user=%s "
                    "application=%s transaction_age=%s; finish this transaction before "
                    "awaiting the subscription (including this process's own batches)",
                    name,
                    vxid,
                    pid,
                    user,
                    application,
                    age,
                )
            warn_at = time.monotonic() + _BACKFILL_WARN_S
        await asyncio.sleep(_BACKFILL_POLL_S)
        blockers = await store.backfill_blockers(conn, transactions)
    return True


async def _backfill(conn: store.Conn, name: str, generation: UUID, marker: store.Backfill) -> bool:
    started = time.monotonic()
    log.info(
        "feed backfill start/resume name=%s states=%s since=%s",
        name,
        sorted(marker["pending"]),
        marker["since"],
    )
    rows = chunks = 0
    if await _wait_for_transactions(conn, name, generation):
        while True:
            result = await store.backfill_chunk(conn, name, generation, _BACKFILL_CHUNK)
            if result is None:
                break
            count, pending = result
            rows += count
            chunks += 1
            log.debug("feed backfill chunk name=%s rows=%s pending=%s", name, count, pending)
            if not pending:
                log.info(
                    "feed backfill finished name=%s rows=%s chunks=%s seconds=%.3f",
                    name,
                    rows,
                    chunks,
                    time.monotonic() - started,
                )
                return True
    log.info("feed backfill stopped name=%s: registration removed or replaced", name)
    return False


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
    def __init__(
        self, conn: store.Conn, name: str, generation: UUID, settings: Settings, batch_size: int
    ) -> None:
        self.conn, self.name, self.settings, self.batch_size = conn, name, settings, batch_size
        self._generation = generation
        self._closed = False
        self._batch: Batch | None = None

    def __aiter__(self) -> Feed:
        return self

    async def _rollback(self) -> None:
        if self._batch is not None:
            self._batch._active = False
            self._batch = None
        await self.conn.rollback()

    async def __anext__(self) -> Batch:
        if self._closed:
            raise StopAsyncIteration
        await self._rollback()
        try:
            while True:
                # Notices are level-triggered hints: discard the backlog before checking rows.
                async with contextlib.aclosing(self.conn.notifies(timeout=0)) as pending:
                    async for _ in pending:
                        pass
                registration = await (
                    await self.conn.execute(
                        "SELECT generation FROM fronta.subscriptions WHERE name = %s FOR KEY SHARE",
                        (self.name,),
                    )
                ).fetchone()
                if registration is None or registration[0] != self._generation:
                    self._closed = True
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
async def subscribe(  # noqa: PLR0913  # additive public call arguments
    name: str,
    *,
    states: Sequence[State | str] = (State.SUCCEEDED, State.FAILED, State.CANCELLED),
    types: Sequence[str] | None = None,
    settings: Settings | None = None,
    batch_size: int = 256,
    backfill: bool | float | timedelta | None = None,
) -> AsyncIterator[Feed]:
    """Consume durable events, optionally projecting retained tasks on first registration.

    Backfill accepts True (all retained matches), seconds, or a timedelta. A duration limits
    terminal rows by finished_at; queued/running rows always describe current state. Pending
    backfills resume even when this call omits the argument. Concurrent live events may
    duplicate backfilled (id, attempt, state) snapshots.
    """
    store.check_name(name)
    selected = [State(s).value for s in states]
    for typ in types or ():
        store.check_name(typ)
    if not 1 <= batch_size <= 1000:  # noqa: PLR2004  # bounded delivery
        msg = "feed batch_size must be between 1 and 1000"
        raise ValueError(msg)
    backfill = _normalize_backfill(backfill)
    current = settings or runtime.get_settings()
    async with await psycopg.AsyncConnection.connect(
        runtime.dsn_of(current),
        **runtime.connection_kwargs(current, "fronta-feed"),
    ) as conn:
        await conn.execute("LISTEN fronta_feed")
        registration = await _register(
            conn,
            name,
            selected,
            None if types is None else list(types),
            backfill=backfill,
        )
        generation, marker = registration["generation"], registration["backfill"]
        ready = marker is None or await _backfill(conn, name, generation, marker)
        await conn.set_autocommit(False)
        feed = Feed(conn, name, generation, current, batch_size)
        feed._closed = not ready
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
