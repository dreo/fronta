"""Coalesced wake, cancellation and feed hints, sent outside durable task commits."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import psycopg

if TYPE_CHECKING:
    from collections.abc import Iterable

log = logging.getLogger(__name__)


class Hints:
    def __init__(self, dsn: str, kwargs: dict[str, Any]) -> None:
        self.dsn, self.kwargs = dsn, kwargs
        self._wake: set[str] = set()
        self._cancel: set[int] = set()
        self._feed: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._ready = asyncio.Event()
        self._closing = False

    def wake(self, types: Iterable[str]) -> None:
        self._wake.update(types)
        self._schedule()

    def cancel(self, ids: Iterable[int]) -> None:
        self._cancel.update(ids)
        self._schedule()

    def feed(self, states: Iterable[str]) -> None:
        self._feed.update(states)
        self._schedule()

    def _schedule(self) -> None:
        self._ready.set()
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="fronta-hints")

    async def _send(
        self, conn: psycopg.AsyncConnection[Any], notices: list[tuple[str, str]]
    ) -> None:
        async with conn.transaction():
            await conn.execute("SET LOCAL synchronous_commit = off")
            await conn.execute(
                "SELECT pg_notify(c, CASE WHEN c='fronta_feed' THEN '' ELSE p END) "
                "FROM unnest(%s::text[], %s::text[]) AS n(c, p) "
                "WHERE c<>'fronta_feed' OR EXISTS (SELECT FROM fronta.subscriptions "
                "WHERE states && string_to_array(p, ','))",
                ([c for c, _ in notices], [p for _, p in notices]),
            )

    async def _run(self) -> None:
        conn = None
        delay = 0.1
        try:
            while True:
                await self._ready.wait()
                self._ready.clear()
                if self._closing and not (self._wake or self._cancel or self._feed):
                    return
                wakes, cancels, feed = self._wake, self._cancel, self._feed
                self._wake, self._cancel, self._feed = set(), set(), set()
                notices = [("fronta_wake", t) for t in wakes]
                notices.extend(("fronta_cancel", str(i)) for i in cancels)
                if feed:
                    notices.append(("fronta_feed", ",".join(sorted(feed))))
                try:
                    if conn is None:
                        conn = await psycopg.AsyncConnection.connect(self.dsn, **self.kwargs)
                    await self._send(conn, notices)
                    delay = 0.1
                    if self._closing:
                        self._ready.set()
                except (psycopg.Error, OSError):
                    self._wake.update(wakes)
                    self._cancel.update(cancels)
                    self._feed |= feed
                    self._ready.set()
                    log.warning("hint delivery failed; retrying", exc_info=True)
                    if conn is not None:
                        await conn.close()
                        conn = None
                    await asyncio.sleep(delay)
                    delay = min(1.0, delay * 2)
        finally:
            if conn is not None:
                await conn.close()

    async def close(self, *, immediate: bool = False) -> None:
        self._closing = True
        self._ready.set()
        if self._task is not None:
            if not immediate:
                try:
                    await asyncio.wait_for(asyncio.shield(self._task), 5)
                except TimeoutError:
                    log.warning("hint flush timed out; polling will recover")
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._wake.clear()
        self._cancel.clear()
        self._feed.clear()
