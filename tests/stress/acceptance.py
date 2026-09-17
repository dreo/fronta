"""Sustained correctness gates; each run uses a disposable database."""

# ruff: noqa: T201

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import platform
import secrets
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from fronta import Settings, Worker, runtime, store, task
from tests.conftest import running_all, wait_until
from tests.stress.__main__ import db_snapshot, rows, scalar, seed, source_fingerprint
from tests.stress.backfill import measure as backfill
from tests.stress.hint_crash import measure as hint_crash
from tests.stress.latency import measure
from tests.stress.worker import Input


async def renewals(conn, dsn, duration):
    batches, reaped = [], []
    heartbeat, reap = store.heartbeat, store.reap

    async def beat(c, pairs, lease_s):
        result = await heartbeat(c, pairs, lease_s)
        batches.append(
            {"time": time.monotonic(), "requested": len(pairs), "confirmed": len(result)}
        )
        return result

    async def reaper(*args, **kwargs):
        result = await reap(*args, **kwargs)
        reaped.extend(result)
        return result

    @task("renewals", input=Input)
    async def sleeper(_ctx, inp):
        await asyncio.sleep(60)
        return inp.n

    await store.publish_task_type(conn, sleeper.spec)
    await seed(conn, {}, 2000 * (int(duration / 60) + 2), task_type="renewals")
    settings = Settings(dsn=dsn, concurrency=2000, grace_s=0)
    worker = Worker([sleeper], settings=settings)
    samples = []
    store.heartbeat, store.reap = beat, reaper
    try:
        async with running_all([worker]):

            async def full():
                return len(worker.attempts) == 2000

            await wait_until(full, timeout=30)
            started = time.monotonic()
            while time.monotonic() - started < duration:
                await asyncio.sleep(min(10, duration - (time.monotonic() - started)))
                expired = await scalar(
                    conn,
                    "SELECT count(*) FROM fronta.tasks WHERE state='running' AND lease_until<now()",
                )
                samples.append({"seconds": time.monotonic() - started, "expired": expired})
                assert expired == 0
                assert not reaped
                print(f"renewals: {samples[-1]}", flush=True)
    finally:
        store.heartbeat, store.reap = heartbeat, reap
    assert batches
    assert max(b["requested"] for b in batches) == 1000
    assert not reaped
    assert await scalar(conn, "SELECT max(failures) FROM fronta.tasks") == 0
    return {"duration_s": duration, "batches": batches, "samples": samples, "reaped": reaped}


async def contention(conn, dsn, duration):
    active, peaks = Counter(), Counter()
    definitions = []
    for name in ("contention_a", "contention_b", "contention_c"):

        @task(name, input=Input, max_concurrency=8, max_concurrency_per_key=2)
        async def handler(ctx, _inp):
            row = ctx._attempt.row
            keys = (row.type, (row.type, row.concurrency_key))
            for key in keys:
                active[key] += 1
                peaks[key] = max(peaks[key], active[key])
            try:
                assert active[row.type] <= 8
                assert active[row.type, row.concurrency_key] <= 2
                await asyncio.sleep(0.01)
            finally:
                for key in keys:
                    active[key] -= 1

        definitions.append(handler)
        await store.publish_task_type(conn, handler.spec)
    await conn.execute(
        sql.SQL("ALTER DATABASE {} SET deadlock_timeout='100ms'").format(
            sql.Identifier(conn.info.dbname)
        )
    )
    settings = Settings(dsn=dsn, concurrency=16, pool_size=2, grace_s=0)
    config = {"types": [d.name for d in definitions], "keys": 16}
    fleet = [Worker(definitions, settings=settings) for _ in range(20)]
    await seed(conn, config, 10000)
    before = await db_snapshot(conn)
    samples, submitted = [], 10000
    async with running_all(fleet):
        started = last_sample = time.monotonic()
        while time.monotonic() - started < duration:
            queued = await scalar(conn, "SELECT count(*) FROM fronta.tasks WHERE state='queued'")
            if queued < 5000:
                await seed(conn, config, 10000)
                submitted += 10000
                runtime.hints(settings).wake(config["types"])
            if time.monotonic() - last_sample >= min(30, duration):
                snapshot = await db_snapshot(conn)
                assert snapshot["database"]["deadlocks"] == before["database"]["deadlocks"]
                samples.append({"seconds": time.monotonic() - started, "database": snapshot})
                print(
                    f"contention: {time.monotonic() - started:.0f}s, {submitted} submitted",
                    flush=True,
                )
                last_sample = time.monotonic()
            await asyncio.sleep(1)
    after = await db_snapshot(conn)
    assert after["database"]["deadlocks"] == before["database"]["deadlocks"]
    assert await scalar(conn, "SELECT max(failures) FROM fronta.tasks") == 0
    assert not any(active.values())
    assert all(peaks[d.name] == 8 for d in definitions)
    return {
        "duration_s": duration,
        "submitted": submitted,
        "before": before,
        "after": after,
        "peaks": {str(k): v for k, v in peaks.items()},
        "samples": samples,
    }


async def main(args):
    maint = os.environ["FRONTA_STRESS_DSN"]
    name = f"fronta_acceptance_{secrets.token_hex(4)}"
    dsn = make_conninfo(maint, dbname=name)
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "phase": args.phase,
        "source_sha256": source_fingerprint(),
        "python": sys.version,
        "platform": platform.platform(),
        "psycopg": psycopg.__version__,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as output:
        output.write("{}\n")
    async with await psycopg.AsyncConnection.connect(maint, autocommit=True) as admin:
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
                await store.init_schema(conn)
                report["postgres"] = await scalar(conn, "SELECT version()")
                if args.phase == "backfill":
                    report["durability"] = await rows(
                        conn,
                        "SELECT name,setting FROM pg_settings WHERE name IN "
                        "('fsync','synchronous_commit','full_page_writes','shared_buffers','max_wal_size')",
                    )
                    report["result"] = await backfill(conn, dsn, args, args.output.with_suffix(""))
                else:
                    fn = {
                        "renewals": renewals,
                        "contention": contention,
                        "latency": measure,
                        "hint-crash": hint_crash,
                    }[args.phase]
                    report["result"] = await fn(
                        conn, dsn, args.repetitions if args.phase == "latency" else args.seconds
                    )
                report["passed"] = report["result"].get("passed", True)
        except BaseException as exc:
            report.update(passed=False, error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            await runtime.close_pool()
            await admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )
            args.output.write_text(json.dumps(report, indent=2, default=str) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "phase",
        choices=(
            "renewals",
            "contention",
            "latency",
            "hint-crash",
            "backfill",
        ),
    )
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--repetitions", type=int, default=30)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--matrix", type=Path, default=Path("benchmarks/matrix.json"))
    parser.add_argument("--history", type=int, default=1_000_000)
    parser.add_argument("--rate", type=int, default=3000)
    args = parser.parse_args()
    if args.phase in ("renewals", "contention") and not args.seconds:
        parser.error("--seconds is required for sustained checks")
    if args.phase == "backfill":
        args.seconds = 60 if args.seconds is None else args.seconds
        if args.history < 1 or args.rate < 1 or not 15 <= args.seconds <= sys.float_info.max:
            parser.error("positive history/rate and at least 15 seconds of live load are required")
    with contextlib.suppress(KeyboardInterrupt):
        sys.exit(asyncio.run(main(args)))
