"""Task definitions shared by the tests. Worker instances for subprocesses live in `subworkers`."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from datetime import datetime  # noqa: TC003  # pydantic evaluates the annotation at runtime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from fronta import Backoff, Context, NonRetryableError, Sandbox, Worker, process_task, task

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class In(BaseModel):
    n: int = 0
    sleep_s: float = 0.0
    key: str | None = None


class Out(BaseModel):
    n: int
    started: float
    finished: float


class When(BaseModel):
    at: datetime


@dataclass
class Interval:
    task_id: int
    attempt: int
    key: str | None
    start: float
    end: float


INTERVALS: list[Interval] = []
"""Handler-side record of every attempt run in this process (for overlap checks)."""


async def _timed_sleep(ctx: Context[Any], inp: In) -> Out:
    start = time.monotonic()
    await asyncio.sleep(inp.sleep_s)
    end = time.monotonic()
    INTERVALS.append(Interval(ctx.task_id, ctx.attempt, inp.key, start, end))
    return Out(n=inp.n, started=start, finished=end)


@task("sleep", input=In, output=Out, attempt_timeout=30)
async def sleep_task(ctx: Context[Any], inp: In) -> Out:
    return await _timed_sleep(ctx, inp)


@task(
    "limited",
    input=In,
    output=Out,
    attempt_timeout=30,
    max_concurrency=2,
    max_concurrency_per_key=1,
)
async def limited_task(ctx: Context[Any], inp: In) -> Out:
    return await _timed_sleep(ctx, inp)


@task("fail", input=In, max_attempts=3, backoff=Backoff(0.2, 2.0, 1.0), attempt_timeout=30)
async def fail_task(ctx: Context[Any], inp: In) -> None:
    del ctx
    msg = f"failing on purpose ({inp.n})"
    raise RuntimeError(msg)


@task("final", input=In, attempt_timeout=30)
async def final_task(ctx: Context[Any], inp: In) -> None:
    del ctx, inp
    msg = "do not retry"
    raise NonRetryableError(msg)


@task("timeout", input=In, max_attempts=2, backoff=Backoff(0.1, 2.0, 0.5), attempt_timeout=1)
async def timeout_task(ctx: Context[Any], inp: In) -> None:
    del ctx, inp
    await asyncio.sleep(60)


@task("stubborn", input=In, attempt_timeout=1, max_attempts=1)
async def stubborn_task(ctx: Context[Any], inp: In) -> None:
    """Ignores cancellation forever (the fatal path)."""
    del ctx, inp
    while True:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(3600)


@task("blocker", input=In, attempt_timeout=30, max_attempts=1)
async def blocker_task(ctx: Context[Any], inp: In) -> None:
    """Blocks the event loop (the watchdog path)."""
    del ctx, inp
    time.sleep(3600)


@task("progress", input=In, attempt_timeout=30)
async def progress_task(ctx: Context[Any], inp: In) -> dict[str, Any]:
    await ctx.progress({"step": 1})
    await ctx.progress({"step": 2, "n": inp.n})
    return {"done": True}


@task("fanout", input=In, attempt_timeout=30, backoff=Backoff(0.1, 2.0, 0.5))
async def fanout_task(ctx: Context[Any], inp: In) -> int:
    """Enqueues a child, then fails its first attempt: the child must exist anyway."""
    await ctx.enqueue(sleep_task, In(n=inp.n + 1, sleep_s=2.0), key=f"child-of-{ctx.task_id}")
    if ctx.attempt == 1:
        msg = "parent fails after enqueueing"
        raise RuntimeError(msg)
    return ctx.attempt


@task("state", input=In, attempt_timeout=30)
async def state_task(ctx: Context[dict[str, Any]], inp: In) -> dict[str, Any]:
    del inp
    ctx.log.info("using lifespan state")
    return {"resource": ctx.state["resource"], "cancelled": ctx.cancelled.is_set()}


@task("timed", input=When, attempt_timeout=30)
async def timed_task(ctx: Context[Any], inp: When) -> str:
    del ctx
    return inp.at.isoformat()


echo_proc = process_task("echo_proc", ["/bin/sh", "-c", "cat; echo; env | sort; pwd"], input=In)
sleep_proc = process_task(
    "sleep_proc",
    ["/bin/sh", "-c", "sleep 60"],
    input=In,
    attempt_timeout=1,
    max_attempts=1,
)
hostile_proc = process_task(
    "hostile_proc",
    ["/bin/sh", "-c", "trap '' TERM; setsid sleep 600 & sleep 600"],
    input=In,
    attempt_timeout=1,
    max_attempts=1,
    sandbox=Sandbox(max_pids=20),
)
long_proc = process_task(
    "long_proc", ["/bin/sh", "-c", "sleep 600"], input=In, attempt_timeout=600, max_attempts=2
)


@contextlib.asynccontextmanager
async def lifespan(worker: Worker[dict[str, Any]]) -> AsyncIterator[dict[str, Any]]:
    del worker
    yield {"resource": "ready"}
