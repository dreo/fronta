"""The SDK's process-global runtime: settings, pool per event loop, reconfiguration, failures."""

from __future__ import annotations

import asyncio

import psycopg
import pytest

from fronta import Settings, runtime, store
from tests.conftest import FAST
from tests.workers import In, sleep_task


async def test_pool_is_reused_per_loop_and_replaced_after_reconfiguration(settings):
    runtime.configure(settings)
    first = await runtime.open_pool()
    assert await runtime.open_pool() is first
    changed = Settings(dsn=settings.dsn, **{**FAST, "pool_size": 2})
    runtime.configure(changed)
    second = await runtime.open_pool()
    assert second is not first
    assert first.closed
    assert second.max_size == 2
    await runtime.close_pool()
    assert second.closed
    assert runtime._pools.get(asyncio.get_running_loop()) is None


async def test_open_pool_with_an_unreachable_database_fails_fast_and_leaves_nothing(settings):
    bad = Settings(dsn="postgresql://nobody@127.0.0.1:1/none", **{**FAST, "connect_timeout_s": 1.0})
    with pytest.raises(psycopg.OperationalError):
        await runtime.open_pool(bad)
    assert runtime._pools.get(asyncio.get_running_loop()) is None
    runtime.configure(settings)
    pool = await runtime.open_pool()
    assert not pool.closed
    await runtime.close_pool()


async def test_settings_come_from_the_environment_when_not_configured(monkeypatch, dsn):
    monkeypatch.setenv("FRONTA_DSN", dsn)
    monkeypatch.setenv("FRONTA_CONCURRENCY", "3")
    monkeypatch.setattr(runtime, "_settings", None)
    loaded = runtime.get_settings()
    assert loaded.dsn == dsn
    assert loaded.concurrency == 3


async def test_concurrent_first_opens_share_one_pool(settings):
    runtime.configure(settings)
    pools = await asyncio.gather(*(runtime.open_pool() for _ in range(5)))
    assert len({id(p) for p in pools}) == 1
    assert runtime._pools[asyncio.get_running_loop()][1] is pools[0]
    await runtime.close_pool()


async def test_sdk_pool_connections_are_autocommit_and_enqueue_stays_atomic(
    settings, conn, monkeypatch
):
    runtime.configure(settings)
    pool = await runtime.open_pool()
    async with pool.connection() as c:
        assert c.autocommit  # one statement, one round trip; transactions are explicit
    await store.publish_task_type(conn, sleep_task.spec)

    async def broken_event(*_args):
        msg = "simulated notification failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(store, "_event", broken_event)
    with pytest.raises(RuntimeError, match="notification failure"):
        await sleep_task.enqueue(In())  # the pool path: row and notifications commit together
    cur = await conn.execute("SELECT count(*) FROM fronta.tasks")
    assert (await cur.fetchone())[0] == 0
    await runtime.close_pool()
