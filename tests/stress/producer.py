"""A separate SDK producer process, so live enqueue can use more than one Python core."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import resource
import sys
import time

from fronta import Settings, runtime
from tests.stress.worker import Input, definition, payload_text


def fingerprint_id(task_id):
    return int.from_bytes(hashlib.blake2b(task_id.to_bytes(8, "big"), digest_size=16).digest())


async def main():
    config = json.loads(os.environ["FRONTA_STRESS_CONFIG"])
    jobs = int(sys.argv[1])
    clients = config.get("clients", 8)
    settings = Settings(
        dsn=os.environ["FRONTA_DSN"],
        pool_size=clients,
    )
    await runtime.open_pool(settings)
    tasks = [definition(config, name) for name in config.get("types", ["stress"])]
    work = iter(range(jobs))
    started = time.monotonic()
    payload = payload_text(config)
    identity = {"count": 0, "sum": 0, "xor": 0}

    async def produce():
        for i in work:
            if rate := config.get("rate_s"):
                await asyncio.sleep(max(0, started + i / rate - time.monotonic()))
            task_id = await tasks[i % len(tasks)].enqueue(
                Input(n=i, payload=payload),
                concurrency_key=f"k{i % config['keys']}" if config.get("keys") else None,
            )
            if config.get("identity"):
                identity["count"] += 1
                identity["sum"] += task_id
                identity["xor"] ^= fingerprint_id(task_id)

    try:
        await asyncio.gather(*(produce() for _ in range(clients)))
    finally:
        await runtime.close_pool()
    usage = resource.getrusage(resource.RUSAGE_SELF)
    print(  # noqa: T201  # machine-readable subprocess result
        json.dumps(
            {
                "jobs": jobs,
                "elapsed_s": time.monotonic() - started,
                "cpu_s": usage.ru_utime + usage.ru_stime,
                "identity": identity,
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
