"""Test infrastructure.

Database tests need `FRONTA_TEST_DSN`, a maintenance DSN: the session creates its own database
(`fronta_test_<random>`), applies the schema, truncates the tables before every test, and drops the
database at the end. Nothing outside that database is ever touched. Without the DSN the database
tests are skipped with a reason; under `CI` they fail instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg
import pytest
import pytest_asyncio
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from fronta import Settings, Worker, runtime, sandbox, store

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

MAINT_DSN = os.environ.get("FRONTA_TEST_DSN")
REPO = Path(__file__).resolve().parent.parent
FAST = {
    "heartbeat_s": 0.2,
    "lease_s": 1.0,
    "reaper_interval_s": 0.5,
    "poll_interval_s": 0.2,
    "grace_s": 1.0,
    "kill_timeout_s": 2.0,
    "concurrency": 5,
    "statement_timeout_s": 10.0,
    "connect_timeout_s": 5.0,
    "purge_interval_s": 3600.0,
}


def _require_dsn() -> str:
    if MAINT_DSN:
        return MAINT_DSN
    if os.environ.get("CI"):
        pytest.fail("FRONTA_TEST_DSN must be set in CI: database tests cannot be skipped there")
    pytest.skip("FRONTA_TEST_DSN is not set: database tests skipped")


@pytest.fixture(scope="session")
def test_dsn() -> AsyncIterator[str]:
    maint = _require_dsn()
    name = f"fronta_test_{secrets.token_hex(4)}"
    with psycopg.connect(maint, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    dsn = make_conninfo(**{**conninfo_to_dict(maint), "dbname": name})

    async def init() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            await store.init_schema(conn)

    asyncio.run(init())
    try:
        yield dsn  # type: ignore[misc]  # pytest accepts sync generator fixtures
    finally:
        with psycopg.connect(maint, autocommit=True) as conn:
            conn.execute(f'DROP DATABASE "{name}" WITH (FORCE)')


@pytest.fixture
def dsn(test_dsn: str) -> str:
    """The test database with empty tables."""
    with psycopg.connect(test_dsn, autocommit=True) as conn:
        conn.execute("TRUNCATE fronta.tasks, fronta.task_types RESTART IDENTITY")
    return test_dsn


@pytest.fixture
def settings(dsn: str) -> Settings:
    return Settings(dsn=dsn, **FAST)


@pytest_asyncio.fixture
async def conn(dsn: str) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as c:
        yield c


@pytest_asyncio.fixture(autouse=True)
async def _no_sdk_pool_left_behind() -> AsyncIterator[None]:
    """A test that opens the SDK pool must close it (use the `sdk` fixture): leaked pools keep
    idle database connections alive for as long as the interpreter lives."""
    yield
    loop = asyncio.get_running_loop()
    entry = runtime._pools.get(loop)
    if entry is not None and not entry[1].closed:
        await entry[1].close()
        msg = "the test left the SDK pool open on its event loop"
        raise AssertionError(msg)


@pytest_asyncio.fixture
async def sdk(settings: Settings) -> AsyncIterator[Settings]:
    """Process-global runtime bound to the test database (for `task.enqueue()` without conn)."""
    runtime.configure(settings)
    try:
        yield settings
    finally:
        await runtime.close_pool()


@contextlib.asynccontextmanager
async def running(worker: Worker[Any], *, start_timeout: float = 15) -> AsyncIterator[Worker[Any]]:
    """Run a worker in the background for the duration of the block; graceful shutdown after."""
    task = asyncio.create_task(worker.run())
    started = asyncio.create_task(worker.started.wait())
    done, _ = await asyncio.wait(
        {task, started}, timeout=start_timeout, return_when="FIRST_COMPLETED"
    )
    started.cancel()
    if task in done:
        task.result()  # raises the startup error
        msg = "worker exited before it started serving"
        raise AssertionError(msg)
    if not done:
        task.cancel()
        msg = "worker did not start in time"
        raise AssertionError(msg)
    try:
        yield worker
    finally:
        if not task.done():
            worker.request_shutdown()
        await asyncio.wait_for(task, 60)


@pytest.fixture
def run_worker() -> Callable[..., contextlib.AbstractAsyncContextManager[Worker[Any]]]:
    return running


@contextlib.asynccontextmanager
async def running_all(workers: list[Worker[Any]]) -> AsyncIterator[list[Worker[Any]]]:
    """Run a whole fleet of in-process workers for the duration of the block."""
    async with contextlib.AsyncExitStack() as stack:
        for worker in workers:
            await stack.enter_async_context(running(worker))
        yield workers


def leftover_sandboxes() -> list[int]:
    """Pids of every process still carrying a sandbox marker in its environment."""
    return [pid for pid, environ in sandbox._proc_environs() if b"FRONTA_SANDBOX_ID=" in environ]


async def wait_until(
    predicate: Callable[[], Awaitable[bool]], timeout: float = 10.0, interval: float = 0.05
) -> None:
    """Poll `predicate` until it is true; fail loudly after `timeout` seconds."""
    deadline = time.monotonic() + timeout
    while True:
        if await predicate():
            return
        if time.monotonic() > deadline:
            msg = f"condition not met within {timeout} s"
            raise AssertionError(msg)
        await asyncio.sleep(interval)


def worker_env(settings: Settings, **extra: str) -> dict[str, str]:
    """Environment for a subprocess worker mirroring `settings` (`FRONTA_*`)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("FRONTA_")}
    for key, value in settings.model_dump().items():
        if value is not None:
            env[f"FRONTA_{key.upper()}"] = str(value)
    env["PYTHONUNBUFFERED"] = "1"
    env.update(extra)
    return env


def fronta_cli() -> str:
    return str(Path(sys.executable).parent / "fronta")


def spawn_worker(target: str, env: dict[str, str]) -> subprocess.Popen[bytes]:
    """Start `fronta worker <target>` from the repository root (so `tests.workers` imports)."""
    return subprocess.Popen(  # noqa: S603  # our own CLI, fixed argv
        [fronta_cli(), "worker", target],
        cwd=REPO,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
