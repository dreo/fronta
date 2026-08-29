"""Process-global runtime for the SDK: settings and the connection pool behind `task.enqueue()`.

`configure()` is optional; the first use builds `Settings()` from the environment. Pools are
bound to the event loop that opened them (psycopg pools own tasks on that loop), so one pool is
kept per running loop and a pool whose loop is gone is discarded.
"""

from __future__ import annotations

import asyncio
import math
import weakref
from typing import Any

from psycopg_pool import AsyncConnectionPool

from fronta.config import Settings

_settings: Settings | None = None
_pools: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop, tuple[Settings, AsyncConnectionPool[Any]]
] = weakref.WeakKeyDictionary()
_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
    weakref.WeakKeyDictionary()
)


def configure(settings: Settings) -> None:
    """Set the process-wide settings (a worker does this with its own settings)."""
    global _settings  # noqa: PLW0603  # the runtime is deliberately process-global
    _settings = settings


def get_settings() -> Settings:
    global _settings  # noqa: PLW0603  # the runtime is deliberately process-global
    if _settings is None:
        _settings = Settings()  # type: ignore[call-arg]  # pydantic-settings reads `dsn` from FRONTA_DSN
    return _settings


def make_pool(
    settings: Settings, *, max_size: int | None = None, application_name: str = "fronta-sdk"
) -> AsyncConnectionPool[Any]:
    """A closed pool with the configured connect and statement timeouts. Open it with `open()`.

    `application_name` shows in `pg_stat_activity`, so operators (and the tests) can tell a
    worker's connections from a server's or an application's.
    """
    statement_timeout_ms = max(1, math.ceil(settings.statement_timeout_s * 1000))
    return AsyncConnectionPool(
        settings.dsn,
        min_size=1,
        max_size=max_size or settings.pool_size,
        open=False,
        timeout=settings.connect_timeout_s,
        kwargs={
            "connect_timeout": max(1, math.ceil(settings.connect_timeout_s)),
            "options": f"-c statement_timeout={statement_timeout_ms}",
            "application_name": application_name,
        },
    )


async def open_ready(pool: AsyncConnectionPool[Any], timeout_s: float) -> None:
    """Open a pool and wait for its first connection; close it again when that fails."""
    await pool.open()
    try:
        await pool.wait(timeout=timeout_s)
    except BaseException:
        await pool.close()
        raise


async def open_pool(settings: Settings | None = None) -> AsyncConnectionPool[Any]:
    """Open (once per event loop and settings) and return the SDK pool; fails fast on a bad DSN."""
    if settings is not None:
        configure(settings)
    current = get_settings()
    loop = asyncio.get_running_loop()
    lock = _locks.get(loop)
    if lock is None:
        lock = _locks[loop] = asyncio.Lock()
    async with lock:  # concurrent first calls must not open two pools
        entry = _pools.get(loop)
        if entry is not None and (entry[0] is not current or entry[1].closed):
            _pools.pop(loop, None)
            await entry[1].close()
            entry = None
        if entry is None:
            pool = make_pool(current)
            await open_ready(pool, current.connect_timeout_s)
            _pools[loop] = (current, pool)
            return pool
        return entry[1]


async def get_pool() -> AsyncConnectionPool[Any]:
    return await open_pool()


async def close_pool() -> None:
    """Close the pool of the current event loop, if any."""
    entry = _pools.pop(asyncio.get_running_loop(), None)
    if entry is not None:
        await entry[1].close()
