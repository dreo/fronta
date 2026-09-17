"""Worker-loss and retention/open-transaction acceptance runs, each in a disposable DB.

Use --mode loss --seconds 600 or --mode soak --seconds 10800. Shorter durations are harness
smoke tests only. Per-minute samples survive failure; worker operation samples stream to disk.
"""

# ruff: noqa: PLR0912, PLR0915, T201

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import platform
import secrets
import shutil
import signal
import subprocess
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from fronta import Settings, State, runtime, store, subscribe
from tests.stress.__main__ import (
    REPO,
    db_snapshot,
    event_ages,
    feed_clock,
    percentiles,
    producer_processes,
    rows,
    scalar,
    seed,
    source_fingerprint,
    wait_done,
    worker_chunks,
)
from tests.stress.producer import fingerprint_id
from tests.stress.sustain import ACCEPTANCE_VERSION, grade
from tests.stress.worker import definition


class Fleet:
    def __init__(self, dsn, config, directory):
        self.directory = directory.resolve()
        self.env = {k: v for k, v in os.environ.items() if not k.startswith("FRONTA_")}
        self.env.update(FRONTA_DSN=dsn, FRONTA_STRESS_CONFIG=json.dumps(config))
        self.active, self.all, self.files, self.handles = [], [], [], []
        self.kills = []

    async def add(self):
        output = self.directory / f"worker-{len(self.all):03d}.json"
        log = output.with_suffix(".log").open("w")
        self.handles.append(log)
        process = subprocess.Popen(  # noqa: S603  # fixed module, isolated DB
            [sys.executable, "-m", "tests.stress.worker", str(output)],
            cwd=REPO,
            env=self.env,
            stdout=log,
            stderr=log,
        )
        self.active.append(process)
        self.all.append(process)
        self.files.append(output)
        async with asyncio.timeout(60):
            while not output.with_suffix(".ready").exists():
                self.check()
                await asyncio.sleep(0.05)

    def check(self):
        for process in self.active:
            if process.poll() is not None:
                raise RuntimeError(f"worker {process.pid} exited {process.returncode}")

    async def kill_two(self, elapsed):
        victims = self.active[:2]
        del self.active[:2]
        for process in victims:
            process.kill()
        for process in victims:
            await asyncio.to_thread(process.wait)
        self.kills.append({"seconds": elapsed, "pids": [p.pid for p in victims]})
        for _ in victims:
            await self.add()

    async def close(self):
        for process in self.active:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
        for process in self.all:
            try:
                await asyncio.to_thread(process.wait, timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait)
        for handle in self.handles:
            handle.close()


class Consumer:
    def __init__(self, loss):
        self.loss = loss
        self.identity = {"count": 0, "sum": 0, "xor": 0}
        self.ages = []
        self.events = 0

    async def run(self, dsn, ready):
        states = list(State) if self.loss else [State.SUCCEEDED, State.FAILED, State.CANCELLED]
        async with subscribe("endurance", states=states, settings=Settings(dsn=dsn)) as feed:
            self.clock = await feed_clock(feed.conn)
            ready.set()
            async for batch in feed:
                self.ages.extend(await event_ages(batch, "endurance", self.clock))
                if self.loss:
                    await batch.conn.execute(
                        "INSERT INTO public.receipts (seq,task_id,state,attempt) "
                        "SELECT * FROM unnest(%s::bigint[],%s::bigint[],%s::text[],%s::int[])",
                        (
                            [e.seq for e in batch.events],
                            [e.id for e in batch.events],
                            [e.state.value for e in batch.events],
                            [e.attempt for e in batch.events],
                        ),
                    )
                for event in batch.events:
                    if event.state in (State.FAILED, State.CANCELLED):
                        raise AssertionError(f"unexpected terminal event: {event}")
                    if not self.loss and event.attempt != 1:
                        raise AssertionError(f"task reaped during soak: {event}")
                await batch.ack()
                self.events += len(batch.events)
                for event in batch.events:
                    if event.state is State.SUCCEEDED:
                        self.identity["count"] += 1
                        self.identity["sum"] += event.id
                        self.identity["xor"] ^= fingerprint_id(event.id)


async def hold_transaction(dsn, started, duration, after=3600):
    await asyncio.sleep(max(0, started + after - time.monotonic()))
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        await conn.execute("UPDATE public.xmin_holder SET n=n+1")
        begin = time.monotonic()
        await asyncio.sleep(duration)
        await conn.commit()
        return {"begin_s": begin - started, "end_s": time.monotonic() - started}


def minute_claims(files, started, ended):
    buckets = defaultdict(list)
    for file in files:
        for chunk in worker_chunks(file):
            for stamp, elapsed, ok, *_ in chunk["operations"].get("claim", []):
                if ok and started <= stamp <= ended:
                    buckets[int((stamp - started) // 60)].append(elapsed)
    return {str(k): percentiles(v) for k, v in sorted(buckets.items())}


class ClaimSamples:
    """Read new complete worker chunks and close minutes once every worker has flushed them."""

    def __init__(self):
        self.positions, self.latest, self.closed = {}, {}, {}
        self.pending = defaultdict(list)

    def read(self, files, started):
        for file in files:
            source = file.with_suffix(".jsonl")
            if not source.exists():
                continue
            with source.open() as stream:
                stream.seek(self.positions.get(file, 0))
                while line := stream.readline():
                    if not line.endswith("\n"):
                        break  # a worker is still writing this chunk; retry next minute
                    chunk = json.loads(line)
                    self.positions[file] = stream.tell()
                    self.latest[file] = chunk["samples"][-1]["time"]
                    for stamp, elapsed, ok, *_ in chunk["operations"].get("claim", []):
                        if ok and stamp >= started:
                            self.pending[int((stamp - started) // 60)].append(elapsed)
        through = min(self.latest.get(file, 0) for file in files)
        ready = {
            str(minute): percentiles(self.pending.pop(minute))
            for minute in sorted(self.pending)
            if started + (minute + 1) * 60 <= through
        }
        self.closed.update(ready)
        return ready


async def verify_loss(conn, expected):
    result = (
        await rows(
            conn,
            "WITH r AS (SELECT task_id,"
            " count(*) FILTER(WHERE state='queued' AND attempt=0) initial,"
            " count(*) FILTER(WHERE state='running') claimed,"
            " count(*) FILTER(WHERE state='queued' AND attempt>0) reaped,"
            " count(*) FILTER(WHERE state='succeeded') terminal,"
            " max(attempt) attempts FROM public.receipts GROUP BY task_id)"
            " SELECT count(*) total, count(*) FILTER(WHERE t.id IS NULL OR r.task_id IS NULL"
            " OR t.state<>'succeeded' OR t.attempt<>1+t.failures OR r.initial<>1"
            " OR r.claimed<>t.attempt OR r.reaped<>t.failures OR r.terminal<>1"
            " OR r.attempts<>t.attempt) bad, coalesce(sum(t.failures),0) reaped"
            " FROM fronta.tasks t FULL JOIN r ON t.id=r.task_id",
        )
    )[0]
    assert result["total"] == expected, result
    assert result["bad"] == 0, result
    return result


async def run(conn, dsn, args, directory, report):
    loss = args.mode == "loss"
    rate = args.rate or (1000 if loss else 3000)
    config = {
        "concurrency": 256,
        "sleep_s": 0.05,
        "lease_s": 10,
        "heartbeat_s": 3,
        "reaper_interval_s": 1,
        "max_attempts": 100 if loss else 3,
        "retention_s": 86400 if loss else 1200,
        "purge_interval_s": 60,
        "producers": 4,
        "clients": 8,
        "identity": True,
        "rate_s": rate / 4,
        "timeout_s": args.seconds + 300,
    }
    report["config"] = config
    report["requested_duration_s"] = args.seconds
    report["preloaded_history"] = args.history
    report["diagnostic"] = args.diagnostic
    report["violations"] = []

    await store.publish_task_type(conn, definition(config).spec)
    await conn.execute("CREATE TABLE public.xmin_holder(n int)")
    await conn.execute("INSERT INTO public.xmin_holder VALUES (0)")
    if loss:
        await conn.execute(
            "CREATE TABLE public.receipts("
            "seq bigint PRIMARY KEY,task_id bigint,state text,attempt int)"
        )
    fleet, consumer = Fleet(dsn, config, directory), Consumer(loss)
    ready = asyncio.Event()
    producing = holding = consuming = None
    samples, claims = [], ClaimSamples()
    report["samples"] = samples
    report["claim_minutes"] = claims.closed
    try:
        for _ in range(8):
            await fleet.add()
        await seed(conn, config, 1000)
        await conn.execute("SELECT pg_notify('fronta_wake', '')")
        await wait_done(conn, 1000, fleet.active, 60)
        await conn.execute("TRUNCATE fronta.tasks, fronta.events RESTART IDENTITY")
        if args.history:
            await seed(conn, config, args.history, state="succeeded")
            await conn.execute("VACUUM ANALYZE fronta.tasks")
        consuming = asyncio.create_task(consumer.run(dsn, ready))
        await asyncio.wait_for(ready.wait(), 10)
        report["feed_clock"] = consumer.clock
        report["feed_latency_bound"] = (
            "upper: timestamp precedes durable commit; includes age query"
        )
        jobs = int(rate * args.seconds)
        started = time.monotonic()
        report["started_monotonic"] = started
        producing = asyncio.create_task(producer_processes(dsn, config, jobs))
        if not loss and args.seconds >= args.hold_at + args.hold_for:
            holding = asyncio.create_task(
                hold_transaction(dsn, started, args.hold_for, args.hold_at)
            )
        next_kill, next_sample = 30.0, min(60.0, args.seconds)
        with (directory / "minutes.jsonl").open("w") as stream:
            while True:
                elapsed = time.monotonic() - started
                fleet.check()
                if consuming.done():
                    consuming.result()
                    raise RuntimeError("consumer exited")
                if loss and elapsed >= next_kill and next_kill <= args.seconds:
                    await fleet.kill_two(elapsed)
                    next_kill += 30
                if elapsed >= next_sample:
                    sample = {
                        "seconds": elapsed,
                        "database": await db_snapshot(conn),
                        "relation_bytes": await rows(
                            conn,
                            "SELECT relname,pg_total_relation_size(relid) bytes "
                            "FROM pg_stat_user_tables WHERE schemaname='fronta'",
                        ),
                        "feed_lag": percentiles(consumer.ages),
                        "acknowledged_events": consumer.events,
                        "terminal_events": consumer.identity["count"],
                        "free_bytes": shutil.disk_usage(directory).free,
                    }
                    sample["completion_seconds"] = time.monotonic() - started
                    consumer.ages.clear()
                    if not loss:
                        sample["backlog"] = (
                            await rows(
                                conn,
                                "SELECT (SELECT count(*) FROM fronta.tasks WHERE state IN"
                                " ('queued','running')) tasks, (SELECT count(*) FROM"
                                " fronta.events) events, (SELECT extract(epoch FROM"
                                " clock_timestamp()-finished_at)::float FROM fronta.tasks"
                                " WHERE state IN ('succeeded','failed','cancelled')"
                                " ORDER BY finished_at LIMIT 1) oldest_terminal_age_s",
                            )
                        )[0]
                        sample["backlog"]["total"] = (
                            sample["backlog"]["tasks"] + sample["backlog"]["events"]
                        )
                        sample["claim_minutes"] = await asyncio.to_thread(
                            claims.read, fleet.files, started
                        )
                    samples.append(sample)
                    stream.write(json.dumps(sample, default=str) + "\n")
                    stream.flush()
                    print(
                        f"{args.mode}: {elapsed:.0f}s, {consumer.identity['count']} completed",
                        flush=True,
                    )
                    if not loss:
                        print(
                            json.dumps(
                                {
                                    "feed_lag": sample["feed_lag"],
                                    "claim_minutes": sample["claim_minutes"],
                                    "relation_bytes": sample["relation_bytes"],
                                    "table": sample["database"]["table"],
                                    "backlog": sample["backlog"],
                                }
                            ),
                            flush=True,
                        )
                    if sample["free_bytes"] < 5 * 1024**3:
                        raise RuntimeError("test stopped with less than 5 GiB free")
                    next_sample += 60
                if producing.done():
                    producer_results = producing.result()
                    if elapsed >= args.seconds:
                        break
                await asyncio.sleep(0.25)
        async with asyncio.timeout(180):
            while consumer.identity["count"] < jobs:
                fleet.check()
                if consuming.done():
                    consuming.result()
                await asyncio.sleep(0.1)
        expected = {"count": 0, "sum": 0, "xor": 0}
        for result in producer_results:
            expected["count"] += result["identity"]["count"]
            expected["sum"] += result["identity"]["sum"]
            expected["xor"] ^= result["identity"]["xor"]
        assert consumer.identity == expected, (consumer.identity, expected)
        assert await scalar(conn, "SELECT count(*) FROM fronta.events") == 0
        assert (
            await scalar(
                conn, "SELECT count(*) FROM fronta.tasks WHERE state IN ('queued','running')"
            )
            == 0
        )
        report.update(
            identity=expected,
            feed_clock=consumer.clock,
            acknowledged_events=consumer.events,
            producer_results=producer_results,
            elapsed_s=time.monotonic() - started,
            producer_tasks_per_s=jobs / max(r["elapsed_s"] for r in producer_results),
            tail_feed_lag=percentiles(consumer.ages),
            samples=samples,
            kills=fleet.kills,
            final_backlog={"tasks": 0, "events": 0},
        )
        if loss:
            report["accounting"] = await verify_loss(conn, jobs)
        elif holding:
            report["open_transaction"] = await holding
    finally:
        ended = time.monotonic()
        if "started_monotonic" in report:
            report.update(
                elapsed_s=ended - started,
                acknowledged_events=consumer.events,
                received_terminal_identity=dict(consumer.identity),
                tail_feed_lag=percentiles(consumer.ages),
                kills=fleet.kills,
            )
        for pending in (producing, consuming, holding):
            if pending:
                pending.cancel()
        await asyncio.gather(
            *(p for p in (producing, consuming, holding) if p), return_exceptions=True
        )
        await fleet.close()
        if "started_monotonic" in report:
            report["claim_minutes"] = minute_claims(fleet.files, started, ended)
    assert all(p.returncode == 0 for p in fleet.active)
    report["worker_exit_codes"] = [p.returncode for p in fleet.all]
    if loss:
        report["passed"] = (
            not args.diagnostic
            and args.seconds >= 600
            and len(fleet.kills) == int(args.seconds // 30)
        )
    else:
        report.update(grade(report))


async def main(args):
    maint = os.environ["FRONTA_STRESS_DSN"]
    name = f"fronta_endurance_{secrets.token_hex(4)}"
    dsn = make_conninfo(maint, dbname=name)
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "source_sha256": source_fingerprint(),
        "python": sys.version,
        "platform": platform.platform(),
        "psycopg": psycopg.__version__,
        "mode": args.mode,
        "acceptance_version": ACCEPTANCE_VERSION,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as output:
        output.write("{}\n")
    directory = args.output.with_suffix("")
    directory.mkdir()
    async with await psycopg.AsyncConnection.connect(maint, autocommit=True) as admin:
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
                await store.init_schema(conn)
                report["postgres"] = await scalar(conn, "SELECT version()")
                await run(conn, dsn, args, directory, report)
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
    parser.add_argument("--mode", choices=("loss", "soak"), required=True)
    parser.add_argument("--seconds", type=float, required=True)
    parser.add_argument("--rate", type=int)
    parser.add_argument("--hold-at", type=float, default=3600, help="transaction start, seconds")
    parser.add_argument(
        "--hold-for", type=float, default=1800, help="transaction duration, seconds"
    )
    parser.add_argument(
        "--history", type=int, default=0, help="preload terminal rows before timing"
    )
    parser.add_argument(
        "--diagnostic",
        action="store_true",
        help="record an exploratory run without certifying the full acceptance test",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.hold_at < 0 or args.hold_for <= 0:
        parser.error("--hold-at must be nonnegative and --hold-for must be positive")
    if (args.hold_at, args.hold_for) != (3600, 1800) and (
        args.mode != "soak" or not args.diagnostic
    ):
        parser.error("custom transaction timing requires --mode soak --diagnostic")
    if args.history < 0 or (args.history and args.mode == "loss"):
        parser.error("--history requires soak mode and a nonnegative count")
    with contextlib.suppress(KeyboardInterrupt):
        sys.exit(asyncio.run(main(args)))
