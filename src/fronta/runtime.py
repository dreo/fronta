"""Process-global runtime for the SDK: settings and the connection pool behind `task.enqueue()`.

`configure()` is optional; the first use builds `Settings()` from the environment. Pools are
bound to the event loop that opened them (psycopg pools own tasks on that loop), so one pool is
kept per running loop and a pool whose loop is gone is discarded.

Every pool Fronta opens hands out autocommit connections: a single statement is one round trip,
and multi-statement operations open explicit transaction blocks. Claims/outcomes use a single
statement. Coalesced hints use a separate lazy connection per event loop and database.
"""

from __future__ import annotations

import asyncio
import math
import weakref
from typing import TYPE_CHECKING, Any

from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import AsyncConnectionPool

from fronta.config import Settings
from fronta.errors import ConfigurationError
from fronta.hints import Hints

if TYPE_CHECKING:
    from fronta.store import Conn

_settings: Settings | None = None
_pools: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop, tuple[Settings, AsyncConnectionPool[Any]]
] = weakref.WeakKeyDictionary()
_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
    weakref.WeakKeyDictionary()
)


_hints: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[tuple[str, ...], Hints]] = (
    weakref.WeakKeyDictionary()
)


def connection_kwargs(
    settings: Settings,
    application_name: str,
    statement_timeout_s: float | None = None,
) -> dict[str, Any]:
    timeout = settings.statement_timeout_s if statement_timeout_s is None else statement_timeout_s
    return {
        "autocommit": True,
        "connect_timeout": max(1, math.ceil(settings.connect_timeout_s)),
        "options": (
            f"-c statement_timeout={max(1, math.ceil(timeout * 1000))}"
            " -c default_transaction_isolation=read\\ committed"
        ),
        "application_name": application_name,
    }


def hints(settings: Settings | None = None, *, conn: Conn | None = None) -> Hints:
    current = settings or get_settings()
    target = (
        make_conninfo(conn.info.dsn, password=conn.info.password)
        if conn is not None
        else dsn_of(current)
    )
    params = conninfo_to_dict(target)
    params["port"] = params.get("port") or "5432"
    # libpq adds hostaddr when resolving a numeric host; it is not a second target.
    if params.get("hostaddr") == params.get("host"):
        params.pop("hostaddr", None)
    key = tuple(
        str(params.get(k) or "") for k in ("host", "hostaddr", "port", "dbname", "user", "password")
    )
    entries = _hints.setdefault(asyncio.get_running_loop(), {})
    if key not in entries:
        entries[key] = Hints(target, connection_kwargs(current, "fronta-hints", 5))
    return entries[key]


async def close_hints(*, immediate: bool = False) -> None:
    entries = _hints.pop(asyncio.get_running_loop(), {})
    await asyncio.gather(*(h.close(immediate=immediate) for h in entries.values()))


def configure(settings: Settings) -> None:
    """Set the process-wide settings (a worker does this with its own settings)."""
    global _settings  # noqa: PLW0603  # the runtime is deliberately process-global
    _settings = settings


def get_settings() -> Settings:
    global _settings  # noqa: PLW0603  # the runtime is deliberately process-global
    if _settings is None:
        _settings = Settings()  # type: ignore[call-arg]  # mypy misses pydantic Field defaults
    return _settings


def dsn_of(settings: Settings) -> str:
    """The DSN, or a clear error: it is only required where Fronta opens its own connections."""
    if settings.dsn is None:
        msg = "FRONTA_DSN is required to open a database connection"
        raise ConfigurationError(msg)
    return settings.dsn


def make_pool(
    settings: Settings,
    *,
    max_size: int | None = None,
    application_name: str = "fronta-sdk",
    statement_timeout_s: float | None = None,
) -> AsyncConnectionPool[Any]:
    """A closed pool of autocommit connections with the configured timeouts. Open it with `open()`.

    `application_name` shows in `pg_stat_activity`, so operators (and the tests) can tell a
    worker's connections from a server's or an application's. `statement_timeout_s` overrides the
    configured statement timeout (the worker's lease-renewal pool uses its renewal budget).
    """
    return AsyncConnectionPool(
        dsn_of(settings),
        min_size=1,
        max_size=max_size or settings.pool_size,
        open=False,
        timeout=settings.connect_timeout_s,
        kwargs=connection_kwargs(settings, application_name, statement_timeout_s),
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
    """Open (once per event loop and settings) and return the SDK pool; fails fast on a bad DSN.

    The pool's connections are autocommit (see the module docstring).
    """
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


async def close_pool() -> None:
    """Close the pool of the current event loop, if any."""
    await close_hints()
    entry = _pools.pop(asyncio.get_running_loop(), None)
    if entry is not None:
        await entry[1].close()
