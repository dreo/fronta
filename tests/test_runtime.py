"""The SDK's process-global runtime: settings, pool per event loop, reconfiguration, failures."""

from __future__ import annotations

import asyncio

import psycopg
import pytest

from fronta import Settings, runtime
from tests.conftest import FAST


async def test_pool_is_reused_per_loop_and_replaced_after_reconfiguration(settings):
    runtime.configure(settings)
    first = await runtime.open_pool()
    assert await runtime.get_pool() is first
    changed = Settings(dsn=settings.dsn, **{**FAST, "pool_size": 2})
    runtime.configure(changed)
    second = await runtime.get_pool()
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
