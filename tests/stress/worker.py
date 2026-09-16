"""A real worker process with benchmark-only, timestamped instrumentation."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import resource
import sys
import time
from collections import defaultdict
from pathlib import Path

from pydantic import BaseModel

from fronta import Settings, Worker, store, task


class Input(BaseModel):
    n: int = 0
    payload: str = ""


def payload_text(config):
    return random.Random(42).randbytes(config.get("payload_bytes", 0) // 2).hex()  # noqa: S311


def definition(config, name="stress"):
    @task(
        name,
        input=Input,
        max_attempts=config.get("max_attempts", 3),
        max_concurrency=config.get("type_limit"),
        max_concurrency_per_key=config.get("key_limit"),
    )
    async def handler(ctx, inp):
        for step in range(config.get("progress", 0)):
            await ctx.progress({"step": step})
        if delay := config.get("sleep_s", 0):
            await asyncio.sleep(delay)
        return {"n": inp.n, "bytes": len(inp.payload)}

    return handler


async def main():
    config = json.loads(os.environ["FRONTA_STRESS_CONFIG"])
    output = Path(sys.argv[1])
    logging.basicConfig(level=logging.WARNING)
    observations = defaultdict(list)
    batch_sizes = defaultdict(list)
    for name in ("claim", "complete", "heartbeat", "set_progress"):
        original = getattr(store, name)

        def instrument(fn, operation):
            async def wrapped(*args, **kwargs):
                begin = time.monotonic()
                result = await fn(*args, **kwargs)
                if operation in ("claim", "complete", "heartbeat"):
                    batch_sizes[operation].append(len(result))
                observations[operation].append(
                    [
                        begin,
                        time.monotonic() - begin,
                        bool(result) if operation == "claim" else True,
                        len(result)
                        if operation in ("claim", "complete", "heartbeat")
                        else int(bool(result)),
                    ]
                )
                return result

            return wrapped

        setattr(store, name, instrument(original, name))

    settings = Settings(
        dsn=os.environ["FRONTA_DSN"],
        pool_size=config.get("pool", 4),
        concurrency=config.get("concurrency", 10),
        poll_interval_s=config.get("poll_s", 1),
        heartbeat_s=config.get("heartbeat_s", 10),
        lease_s=config.get("lease_s", 30),
        reaper_interval_s=config.get("reaper_interval_s", 15),
        grace_s=5,
        retention_s=config.get("retention_s", 7 * 86400),
        purge_interval_s=config.get("purge_interval_s", 600),
    )
    worker = Worker(
        [definition(config, name) for name in config.get("types", ["stress"])], settings=settings
    )
    samples = []

    def write_chunk(chunk):
        with output.with_suffix(".jsonl").open("a") as stream:
            stream.write(json.dumps(chunk) + "\n")

    async def sample():
        last_flush = time.monotonic()
        await worker.started.wait()
        output.with_suffix(".ready").touch()
        while True:
            begin = time.monotonic()
            await asyncio.sleep(0.05)
            usage = resource.getrusage(resource.RUSAGE_SELF)
            samples.append(
                {
                    "time": time.monotonic(),
                    "lag_s": max(0, time.monotonic() - begin - 0.05),
                    "cpu_s": usage.ru_utime + usage.ru_stime,
                    "rss": usage.ru_maxrss,
                    "pool": worker.pool.get_stats(),
                }
            )

            if time.monotonic() - last_flush >= 60:
                chunk = {
                    "operations": dict(observations),
                    "samples": list(samples),
                    "batch_sizes": dict(batch_sizes),
                }
                observations.clear()
                samples.clear()
                batch_sizes.clear()
                await asyncio.to_thread(write_chunk, chunk)
                last_flush = time.monotonic()

    sampling = asyncio.create_task(sample())
    try:
        code = await worker.run()
    finally:
        sampling.cancel()
        await asyncio.gather(sampling, return_exceptions=True)
        output.write_text(
            json.dumps({"operations": observations, "samples": samples, "batch_sizes": batch_sizes})
        )
    return code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
