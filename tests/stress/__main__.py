"""Reproducible stress matrix against a disposable database; see benchmarks/README.md."""

# ruff: noqa: PLR0912, PLR0913, PLR0915, T201
# Scenario orchestration is deliberately linear; this CLI prints progress.

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import platform
import random
import re
import secrets
import signal
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from fronta import Settings, State, runtime, store, subscribe
from fronta.model import TaskFilter
from tests.stress.worker import Input, definition, payload_text

REPO = Path(__file__).resolve().parents[2]


def source_fingerprint():
    source_hash = hashlib.sha256()
    source_root = Path(store.__file__).resolve().parent
    for source in sorted(
        p
        for p in source_root.rglob("*")
        if p.suffix in (".py", ".sql") and not p.name.startswith("._")
    ):
        source_hash.update(str(Path("src/fronta") / source.relative_to(source_root)).encode())
        source_hash.update(source.read_bytes())
    return source_hash.hexdigest()


def percentiles(values):
    ordered = sorted(values)
    if not ordered:
        return {"n": 0}
    return {
        "n": len(ordered),
        "mean_ms": statistics.mean(ordered) * 1000,
        **{
            f"p{p}_ms": ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p / 100))] * 1000
            for p in (50, 95, 99)
        },
        "max_ms": ordered[-1] * 1000,
    }


async def rows(conn, query, params=None):
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return await cur.fetchall()


async def scalar(conn, query, params=None):
    return (await (await conn.execute(query, params)).fetchone())[0]


async def seed(conn, config, n, *, state="queued", task_type="stress", priority=0, future=False):
    # Deterministic, poorly compressible ASCII: large payloads really exercise TOAST/wire costs.
    payload = payload_text(config)
    task_types = config.get("types", [task_type]) if task_type == "stress" else [task_type]
    await conn.execute(
        "INSERT INTO fronta.tasks (type, state, input, concurrency_key, priority, run_at,"
        " max_attempts, attempt_timeout_s, backoff_base_s, backoff_factor, backoff_cap_s,"
        " finished_at, token, lease_until)"
        " SELECT (%s::text[])[(g-1) %% cardinality(%s::text[])+1], %s,"
        " jsonb_build_object('n', g, 'payload', %s::text),"
        " CASE WHEN %s::int > 0 THEN 'k' || (g %% greatest(1, %s))::text END,"
        " %s, now() + make_interval(secs => %s), 3, 300, 1, 2, 3600,"
        " CASE WHEN %s = 'succeeded' THEN now() END,"
        " CASE WHEN %s = 'running' THEN gen_random_uuid() END,"
        " CASE WHEN %s = 'running' THEN now() + interval '1 hour' END"
        " FROM generate_series(1, %s) AS g",
        (
            task_types,
            task_types,
            state,
            payload,
            config.get("keys", 0),
            config.get("keys", 0),
            priority,
            86400 if future else 0,
            state,
            state,
            state,
            n,
        ),
    )


async def monitor(conn, stop, samples):
    captured_lock = False
    while not stop.is_set():
        sample = await rows(
            conn,
            "SELECT state, wait_event_type, wait_event, count(*) AS n FROM pg_stat_activity"
            " WHERE datname = current_database() AND pid <> pg_backend_pid()"
            " GROUP BY state, wait_event_type, wait_event",
        )
        entry = {"time": time.monotonic(), "activity": sample}
        if not captured_lock and any(r["wait_event"] == "object" for r in sample):
            entry["object_locks"] = await rows(
                conn,
                "SELECT l.locktype, l.classid, l.objid, l.mode, left(a.query,80) AS query,"
                " count(*) AS n FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid"
                " WHERE a.datname=current_database() AND NOT l.granted AND l.locktype='object'"
                " GROUP BY 1,2,3,4,5",
            )
            captured_lock = bool(entry["object_locks"])
        samples.append(entry)
        await asyncio.sleep(0.1)


async def wait_done(conn, expected, processes, timeout):
    deadline = time.monotonic() + timeout
    while True:
        for proc in processes:
            if proc.poll() is not None:
                raise RuntimeError(f"worker exited early: {proc.returncode}")
        # Counting all completed history every 100 ms becomes a material CPU load in long soaks.
        # Materialized ordered LIMITs keep both probes on their small active-state indexes.
        active = await scalar(
            conn,
            "WITH queued AS MATERIALIZED (SELECT id FROM fronta.tasks"
            " WHERE state='queued' ORDER BY priority DESC,run_at,id LIMIT 1),"
            " running AS MATERIALIZED (SELECT id FROM fronta.tasks"
            " WHERE state='running' ORDER BY type,concurrency_key LIMIT 1)"
            " SELECT EXISTS(SELECT FROM queued) OR EXISTS(SELECT FROM running)",
        )
        if not active:
            done = await scalar(conn, "SELECT count(*) FROM fronta.tasks WHERE state='succeeded'")
            if done == expected:
                return
            raise AssertionError(f"queue exhausted with {done}/{expected} successes")
        if time.monotonic() > deadline:
            raise TimeoutError(f"tasks remain active after {timeout}s (expected {expected})")
        await asyncio.sleep(0.1)


async def db_snapshot(conn):
    await conn.execute("SELECT pg_stat_clear_snapshot()")
    wal = (await rows(conn, "SELECT wal_records, wal_fpi, wal_bytes::float FROM pg_stat_wal"))[0]
    # PostgreSQL 18 moved WAL write/fsync counters from pg_stat_wal to pg_stat_io.
    # Keep the report's original field names so version comparisons use the same units.
    if conn.info.server_version >= 180000:
        wal_io = (
            await rows(
                conn,
                "SELECT coalesce(sum(writes), 0)::bigint AS wal_write,"
                " coalesce(sum(fsyncs), 0)::bigint AS wal_sync,"
                " coalesce(sum(write_time), 0)::float AS wal_write_time,"
                " coalesce(sum(fsync_time), 0)::float AS wal_sync_time"
                " FROM pg_stat_io WHERE object='wal' AND context='normal'",
            )
        )[0]
        wal_io_source = "pg_stat_io (object=wal, context=normal; all backend types)"
    else:
        wal_io = (
            await rows(
                conn,
                "SELECT wal_write, wal_sync, wal_write_time, wal_sync_time FROM pg_stat_wal",
            )
        )[0]
        wal_io_source = "pg_stat_wal"
    return {
        "wal_lsn": str(await scalar(conn, "SELECT pg_current_wal_insert_lsn()")),
        "table": await rows(
            conn,
            "SELECT relname, n_tup_ins, n_tup_upd, n_tup_hot_upd,"
            " n_live_tup, n_dead_tup, autovacuum_count, autoanalyze_count"
            " FROM pg_stat_user_tables WHERE schemaname='fronta'",
        ),
        "relation_bytes": await scalar(conn, "SELECT pg_total_relation_size('fronta.tasks')"),
        "database": (
            await rows(
                conn,
                "SELECT xact_commit, xact_rollback, blks_read, blks_hit, tup_inserted, tup_updated,"
                " tup_deleted, deadlocks, temp_bytes FROM pg_stat_database"
                " WHERE datname=current_database()",
            )
        )[0],
        "wal": {**wal, **wal_io},
        "wal_io_source": wal_io_source,
        "checkpointer": (
            await rows(
                conn,
                "SELECT * FROM pg_stat_checkpointer"
                if conn.info.server_version >= 170000
                else "SELECT * FROM pg_stat_bgwriter",
            )
        )[0],
        "notify_slru": (
            await rows(
                conn,
                "SELECT blks_zeroed, blks_hit, blks_read, blks_written, flushes, truncates"
                " FROM pg_stat_slru WHERE lower(name)='notify'",
            )
        )[0],
    }


async def profile(conn):
    if not await scalar(
        conn, "SELECT EXISTS(SELECT FROM pg_extension WHERE extname='pg_stat_statements')"
    ):
        return []
    return await rows(
        conn,
        "SELECT query, calls, total_exec_time, mean_exec_time, rows, shared_blks_hit,"
        " shared_blks_read, plans, total_plan_time, wal_bytes::float FROM pg_stat_statements"
        " WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())"
        " ORDER BY total_exec_time DESC LIMIT 20",
    )


async def reset_profile(conn):
    if await scalar(
        conn, "SELECT EXISTS(SELECT FROM pg_extension WHERE extname='pg_stat_statements')"
    ):
        await conn.execute(
            "SELECT pg_stat_statements_reset(0,"
            " (SELECT oid FROM pg_database WHERE datname=current_database()), 0)"
        )


def worker_chunks(file):
    stream = file.with_suffix(".jsonl")
    if stream.exists():
        with stream.open() as records:
            for record in records:
                yield json.loads(record)
    if file.exists():
        yield json.loads(file.read_text())


def worker_metrics(files, start, end):
    operations = defaultdict(list)
    empty_claims = 0
    batch_sizes = defaultdict(list)
    lag = []
    cpu = 0.0
    pool = Counter()
    rss = 0
    completions = Counter()
    for file in files:
        data = {"operations": defaultdict(list), "batch_sizes": defaultdict(list), "samples": []}
        for chunk in worker_chunks(file):
            for category in ("operations", "batch_sizes"):
                for operation, values in chunk[category].items():
                    data[category][operation].extend(values)
            data["samples"].extend(chunk["samples"])
        for name, sizes in data.get("batch_sizes", {}).items():
            batch_sizes[name].extend(sizes)
        for name, entries in data["operations"].items():
            for stamp, duration, ok, *item_count in entries:
                if start <= stamp <= end:
                    if name == "complete" and item_count:
                        bucket = min(
                            int((stamp + duration - start) // 10),
                            max(0, math.ceil((end - start) / 10) - 1),
                        )
                        completions[bucket] += item_count[0]
                    if name == "claim" and not ok:
                        empty_claims += 1
                    else:
                        operations[name].append(duration)
        samples = [s for s in data["samples"] if start <= s["time"] <= end]
        if len(samples) >= 2:
            cpu += samples[-1]["cpu_s"] - samples[0]["cpu_s"]
            rss += max(s["rss"] for s in samples)
            lag.extend(s["lag_s"] for s in samples)
            for key in ("requests_num", "requests_queued", "requests_wait_ms", "usage_ms"):
                pool[key] += samples[-1]["pool"].get(key, 0) - samples[0]["pool"].get(key, 0)
    return {
        "operations": {name: percentiles(v) for name, v in operations.items()},
        "empty_claims": empty_claims,
        "batch_sizes": {
            k: {"calls": len(v), "items": sum(v), "mean": sum(v) / len(v)}
            for k, v in batch_sizes.items()
            if v
        },
        "event_loop_lag": percentiles(lag),
        "worker_cpu_s": cpu,
        "worker_cpu_cores": cpu / (end - start),
        "worker_peak_rss_sum_bytes": rss * (1 if sys.platform == "darwin" else 1024),
        "pool_counters": dict(pool),
        "completion_timeline": [
            {
                "from_s": bucket * 10,
                "jobs": count,
                "tasks_per_s": count / max(0.001, min(10, end - start - bucket * 10)),
            }
            for bucket, count in sorted(completions.items())
        ],
    }


async def drain(conn, dsn, config, directory):
    processes, files, handles = [], [], []
    producer_metrics = None
    feed_metrics, feeding = {}, None
    n = config.get("jobs", 2000)
    definitions = [definition(config, name) for name in config.get("types", ["stress"])]
    for definition_ in definitions:
        await store.publish_task_type(conn, definition_.spec)
    try:
        for i in range(config.get("workers", 1)):
            output = directory / f"worker-{i}.json"
            log = (directory / f"worker-{i}.log").open("w")
            handles.append(log)
            files.append(output)
            env = {k: v for k, v in os.environ.items() if not k.startswith("FRONTA_")}
            env.update(FRONTA_DSN=dsn, FRONTA_STRESS_CONFIG=json.dumps(config))
            processes.append(
                subprocess.Popen(  # noqa: S603  # fixed module, isolated DB
                    [sys.executable, "-m", "tests.stress.worker", str(output)],
                    cwd=REPO,
                    env=env,
                    stdout=log,
                    stderr=log,
                )
            )
        async with asyncio.timeout(60):
            while not all(f.with_suffix(".ready").exists() for f in files):
                if any(p.poll() is not None for p in processes):
                    raise RuntimeError("worker startup failed; inspect worker logs")
                await asyncio.sleep(0.05)
        warmup = config.get("warmup", 100)
        await seed(conn, config, warmup)
        await conn.execute("SELECT pg_notify('fronta_wake', '')")
        await wait_done(conn, warmup, processes, 120)
        # Empty tables after warmup: prepared connections stay warm; measured backlog is exact.
        await conn.execute("TRUNCATE fronta.tasks, fronta.events RESTART IDENTITY")
        # Future gate makes a single UPDATE the only timed release. Setup/enqueue excluded.
        if not config.get("live_enqueue"):
            if config.get("gate") == "publish":
                await conn.execute(
                    "DELETE FROM fronta.task_types WHERE name=ANY(%s)",
                    ([d.name for d in definitions],),
                )
            await seed(conn, config, n, future=config.get("gate") != "publish")
        if history := config.get("history", 0):
            await seed(conn, {}, history, state="succeeded", task_type="history")
        await conn.execute("VACUUM ANALYZE fronta.tasks")
        before = await db_snapshot(conn)
        await reset_profile(conn)
        if config.get("subscription"):
            ready = asyncio.Event()
            feeding = asyncio.create_task(consume_feed(dsn, n, ready, feed_metrics))
            await asyncio.wait_for(ready.wait(), 10)
        start = time.monotonic()
        if config.get("live_enqueue"):
            producer_metrics = await live_produce(dsn, config, definitions, n)
        else:
            async with conn.transaction():
                if config.get("gate") == "publish":
                    for definition_ in definitions:
                        await store.publish_task_type(conn, definition_.spec)
                else:
                    await conn.execute("UPDATE fronta.tasks SET run_at=now() WHERE state='queued'")
                await conn.execute("SELECT pg_notify('fronta_wake', '')")
        await wait_done(conn, n + config.get("history", 0), processes, config.get("timeout_s", 180))
        if feeding is not None:
            await asyncio.wait_for(feeding, 30)
        end = time.monotonic()
        check = (
            await rows(
                conn,
                "SELECT count(*) AS total, count(*) FILTER(WHERE state='succeeded') AS succeeded,"
                " count(*) FILTER(WHERE attempt<>1 OR failures<>0) AS bad_attempts,"
                " count(*) FILTER(WHERE result->>'n' IS DISTINCT FROM input->>'n'"
                " OR (result->>'bytes')::int IS DISTINCT FROM length(input->>'payload'))"
                " AS bad_results,"
                " percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP"
                " (ORDER BY extract(epoch FROM finished_at-started_at)*1000) AS service_ms"
                " FROM fronta.tasks WHERE type=ANY(%s)",
                ([d.name for d in definitions],),
            )
        )[0]
        if (
            check["total"] != n
            or check["succeeded"] != n
            or check["bad_attempts"]
            or check["bad_results"]
        ):
            raise AssertionError(check)
        statements = await profile(conn)
    finally:
        if feeding is not None:
            feeding.cancel()
            await asyncio.gather(feeding, return_exceptions=True)
        for p in processes:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in processes:
            try:
                await asyncio.to_thread(p.wait, timeout=30)
            except subprocess.TimeoutExpired:
                p.kill()
                await asyncio.to_thread(p.wait)
        for handle in handles:
            handle.close()
    after = await db_snapshot(conn)
    exits = [p.returncode for p in processes]
    if any(exits):
        raise AssertionError(f"worker shutdown failed: {exits}")
    return {
        "start": start,
        "end": end,
        "elapsed_s": end - start,
        "tasks_per_s": n / (end - start),
        "correctness": check,
        "exit_codes": exits,
        "statements": statements,
        "db_before": before,
        "db_after": after,
        "producer_metrics": producer_metrics,
        "feed": feed_metrics,
        **worker_metrics(files, start, end),
    }


async def consume_feed(dsn, expected, ready, metrics):
    seen, ages = set(), []
    async with subscribe("benchmark", settings=Settings(dsn=dsn)) as feed:
        clock = await feed_clock(feed.conn)
        metrics["clock"] = clock
        metrics["latency_bound"] = "upper: timestamp precedes durable commit; includes age query"
        ready.set()
        async for batch in feed:
            ages.extend(await event_ages(batch, "benchmark", clock))
            for event in batch.events:
                assert event.id not in seen
                assert event.state is State.SUCCEEDED
                assert event.attempt == 1
                seen.add(event.id)
            await batch.ack()
            if len(seen) == expected:
                break
    metrics.update(events=len(seen), event_age_at_delivery=percentiles(ages))


async def feed_clock(conn):
    return (
        "commit" if await scalar(conn, "SHOW track_commit_timestamp") == "on" else "statement_start"
    )


async def event_ages(batch, subscription, clock):
    # PostgreSQL records its commit timestamp before flushing WAL. Both clocks therefore
    # bound delivery age from above; neither isolates delay after a durable commit.
    timestamp = "pg_xact_commit_timestamp(xmin)" if clock == "commit" else "created_at"
    records = await (
        await batch.conn.execute(
            sql.SQL(
                "SELECT extract(epoch FROM clock_timestamp()-{}) FROM fronta.events "
                "WHERE subscription=%s AND seq=ANY(%s)"
            ).format(sql.SQL(timestamp)),
            (subscription, [e.seq for e in batch.events]),
        )
    ).fetchall()
    return [float(record[0]) for record in records]


async def live_produce(dsn, config, definitions, n):
    """Bounded SDK producers run while the fleet consumes the queue."""
    clients = config.get("clients", 8)
    if config.get("producers", 1) > 1:
        return await producer_processes(dsn, config, n)
    await runtime.open_pool(Settings(dsn=dsn, pool_size=clients))
    work = iter(range(n))
    payload = payload_text(config)

    async def produce():
        for i in work:
            await definitions[i % len(definitions)].enqueue(
                Input(n=i, payload=payload),
                concurrency_key=f"k{i % config['keys']}" if config.get("keys") else None,
            )

    try:
        await asyncio.gather(*(produce() for _ in range(clients)))
    finally:
        await runtime.close_pool()


async def producer_processes(dsn, config, n):
    processes = []
    count = config["producers"]
    env = {k: v for k, v in os.environ.items() if not k.startswith("FRONTA_")}
    env.update(FRONTA_DSN=dsn, FRONTA_STRESS_CONFIG=json.dumps(config))
    try:
        for i in range(count):
            jobs = n // count + (i < n % count)
            processes.append(
                await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "tests.stress.producer",
                    str(jobs),
                    cwd=REPO,
                    env=env,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            )
        async with asyncio.timeout(config.get("timeout_s", 180)):
            outputs = await asyncio.gather(*(p.communicate() for p in processes))
        metrics = []
        for process, (out, err) in zip(processes, outputs, strict=True):
            if process.returncode:
                raise RuntimeError(f"producer exited {process.returncode}: {err.decode()}")
            metrics.append(json.loads(out))
        assert sum(m["jobs"] for m in metrics) == n
        return metrics
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
                await process.wait()


async def enqueue(conn, dsn, config, _directory):
    definition_ = definition(config)
    await store.publish_task_type(conn, definition_.spec)
    clients, batch, n = config.get("clients", 8), config.get("batch", 1), config.get("jobs", 2000)
    timings = []
    pool = await runtime.open_pool(Settings(dsn=dsn, pool_size=clients))
    try:
        await pool.resize(clients, clients)
        await pool.wait()
        work = iter(range(0, n, batch))

        async def produce():
            for offset in work:
                begin = time.monotonic()
                async with pool.connection() as c:
                    if config.get("non_autocommit"):
                        await c.set_autocommit(False)
                    try:
                        async with c.transaction():
                            for i in range(offset, min(n, offset + batch)):
                                await definition_.enqueue(Input(n=i), conn=c)
                    finally:
                        if config.get("non_autocommit"):
                            await c.set_autocommit(True)
                timings.append(time.monotonic() - begin)

        await reset_profile(conn)
        start = time.monotonic()
        await asyncio.gather(*(produce() for _ in range(clients)))
        end = time.monotonic()
        count = await scalar(conn, "SELECT count(*) FROM fronta.tasks WHERE state='queued'")
        assert count == n, count
        return {
            "start": start,
            "end": end,
            "elapsed_s": end - start,
            "tasks_per_s": n / (end - start),
            "batch_latency": percentiles(timings),
            "correctness": {"queued": count},
            "statements": await profile(conn),
        }
    finally:
        await runtime.close_pool()


async def claims(conn, _dsn, config, _directory):
    await store.publish_task_type(conn, definition(config).spec)
    shape, backlog = config.get("shape", "due"), config.get("backlog", 100000)
    if shape == "due":
        await seed(conn, {}, backlog)
    elif shape == "future":
        await seed(conn, {}, backlog, priority=10, future=True)
    elif shape == "unrelated":
        await seed(conn, {}, backlog, task_type="other", priority=10)
    elif shape == "saturated":
        await conn.execute(
            "INSERT INTO fronta.task_types SELECT 'other', executor, input_schema,"
            " output_schema, policy, 1, NULL, fingerprint, updated_at, false FROM fronta.task_types"
        )
        await seed(conn, {}, 1, state="running", task_type="other")
        await seed(conn, {}, backlog, task_type="other", priority=10)
    elif shape == "history":
        await seed(conn, {}, backlog, state="succeeded", task_type="other")
    else:
        raise ValueError(f"unknown backlog shape: {shape}")
    await seed(conn, {}, config.get("eligible", 200))
    await conn.execute("VACUUM ANALYZE fronta.tasks")
    types = ["stress", "other"] if shape == "saturated" else ["stress"]
    candidate_params = {
        "types": types,
        "skip": [],
        "skip_types": [],
        "skip_key_types": [],
        "skip_keys": [],
        "count": 1,
    }
    async with conn.transaction():
        plan = await scalar(
            conn,
            sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + store._CANDIDATE,
            candidate_params,
        )
        raise psycopg.Rollback
    timings = []
    await reset_profile(conn)
    start = time.monotonic()
    for _ in range(config.get("samples", 50)):
        begin = time.monotonic()
        row = await store.claim(
            conn, types=types, worker="probe", lease_s=30, deadline_s=5, count=1
        )
        timings.append(time.monotonic() - begin)
        assert len(row) == 1
        assert row[0].type == "stress"
    end = time.monotonic()
    return {
        "start": start,
        "end": end,
        "elapsed_s": end - start,
        "claim_latency": percentiles(timings),
        "candidate_plan": plan,
        "correctness": {"claimed": len(timings)},
        "statements": await profile(conn),
    }


async def listing(conn, _dsn, config, _directory):
    """Rare terminal-type history detects the read cost of removing full state/type indexes."""
    await seed(conn, {}, 100, state="succeeded")
    await seed(conn, {}, config.get("backlog", 1000000), state="succeeded", task_type="other")
    await conn.execute("VACUUM ANALYZE fronta.tasks")
    timings = []
    await reset_profile(conn)
    start = time.monotonic()
    for _ in range(config.get("samples", 30)):
        begin = time.monotonic()
        found = await store.list_tasks(conn, TaskFilter(type="stress", state=State.SUCCEEDED))
        timings.append(time.monotonic() - begin)
        assert len(found) == 50
        assert all(r.type == "stress" for r in found)
    return {
        "start": start,
        "end": time.monotonic(),
        "list_latency": percentiles(timings),
        "correctness": {"listed_per_call": 50},
        "statements": await profile(conn),
    }


async def run_case(dsn, config, directory):
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(
            "TRUNCATE fronta.tasks, fronta.task_types, "
            "fronta.subscriptions, fronta.events RESTART IDENTITY"
        )
        samples, stop = [], asyncio.Event()
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as observer:
            sampling = asyncio.create_task(monitor(observer, stop, samples))
            try:
                result = await {
                    "drain": drain,
                    "enqueue": enqueue,
                    "claims": claims,
                    "listing": listing,
                }[config.get("mode", "drain")](conn, dsn, config, directory)
            finally:
                stop.set()
                await sampling
        counts, peak = Counter(), 0
        for sample in samples:
            if result["start"] <= sample["time"] <= result["end"]:
                peak = max(peak, sum(r["n"] for r in sample["activity"]))
                for row in sample["activity"]:
                    if row["state"] == "active":
                        counts[
                            f"{row['wait_event_type'] or 'CPU/other'}:{row['wait_event'] or '-'}"
                        ] += row["n"]
        result.update(active_wait_samples=dict(counts), peak_connections=peak)
        functions = await rows(
            conn,
            "SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(%s)",
            (
                f"fronta.claim_v{store.SCHEMA_VERSION}"
                "(text[],text,double precision,double precision,integer)",
            ),
        )
        result["claim_function_sha256"] = (
            hashlib.sha256(functions[0]["prosrc"].encode()).hexdigest() if functions else None
        )
        result["indexes"] = await rows(
            conn,
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='fronta' "
            "ORDER BY indexname",
        )
        result["object_lock_examples"] = [r for s in samples for r in s.get("object_locks", [])]
        return result


async def main(args):
    if args.output.exists():
        raise SystemExit(f"Output already exists: {args.output}; choose a new path")
    if args.repeats < 1 or (args.jobs is not None and args.jobs < 1):
        raise SystemExit("--repeats and --jobs must be positive")
    maint = os.environ.get("FRONTA_STRESS_DSN")
    if not maint:
        raise SystemExit(
            "Set FRONTA_STRESS_DSN to a maintenance DSN with CREATEDB "
            "(dedicated server recommended)."
        )
    name = f"fronta_stress_{secrets.token_hex(6)}"
    dsn = make_conninfo(**{**conninfo_to_dict(maint), "dbname": name})
    config_list = json.loads(args.matrix.read_text())
    configs = [c for c in config_list if re.search(args.cases, c["name"])]
    if not configs:
        raise SystemExit("No cases matched")
    report = {
        "label": args.label,
        "created_at": datetime.now(UTC).isoformat(),
        "python": sys.version,
        "platform": platform.platform(),
        "host_cpus": os.cpu_count(),
        "psycopg": psycopg.__version__,
        "source_sha256": source_fingerprint(),
        "source_hash_format": "relative Python/SQL paths and contents; excludes ._* metadata",
        "argv": sys.argv[1:],
        "seed": args.seed,
        "runs": [],
    }
    harness_hash = hashlib.sha256()
    for source in sorted(
        p for p in Path(__file__).parent.glob("*.py") if not p.name.startswith("._")
    ):
        harness_hash.update(source.read_bytes())
    report["harness_sha256"] = harness_hash.hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    logs = args.output.with_suffix("")
    logs.mkdir(exist_ok=True)
    async with await psycopg.AsyncConnection.connect(maint, autocommit=True) as admin:
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
                await store.init_schema(conn)
                # Optional profiling; absence must not block the benchmark.
                try:
                    await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
                    await conn.execute("SELECT count(*) FROM pg_stat_statements")
                except psycopg.Error:
                    await conn.execute("DROP EXTENSION IF EXISTS pg_stat_statements")
                report["postgres"] = await scalar(conn, "SELECT version()")
                report["postgres_settings"] = await rows(
                    conn,
                    "SELECT name, setting, unit FROM pg_settings WHERE name=ANY(%s)",
                    (
                        [
                            "max_connections",
                            "shared_buffers",
                            "effective_cache_size",
                            "work_mem",
                            "fsync",
                            "synchronous_commit",
                            "full_page_writes",
                            "wal_sync_method",
                            "wal_buffers",
                            "wal_compression",
                            "commit_delay",
                            "commit_siblings",
                            "max_wal_size",
                            "checkpoint_timeout",
                            "autovacuum",
                            "jit",
                            "track_io_timing",
                            "track_wal_io_timing",
                            "track_commit_timestamp",
                            "data_checksums",
                            "default_toast_compression",
                            "io_method",
                            "io_workers",
                            "effective_io_concurrency",
                            "maintenance_io_concurrency",
                            "notify_buffers",
                            "lc_collate",
                        ],
                    ),
                )
            order = [(repeat, c) for repeat in range(args.repeats) for c in configs]
            random.Random(args.seed).shuffle(order)  # noqa: S311  # reproducible case ordering
            for i, (repeat, case) in enumerate(order):
                config = dict(case)
                if args.jobs is not None:
                    config["jobs"] = args.jobs
                directory = logs / f"{i:03d}-{case['name']}-r{repeat}"
                directory.mkdir(exist_ok=True)
                print(f"[{i + 1}/{len(order)}] {case['name']} repeat {repeat}", flush=True)
                started = time.monotonic()
                try:
                    result = await run_case(dsn, config, directory)
                except Exception as exc:
                    result = {
                        "error": f"{type(exc).__name__}: {exc}",
                        "elapsed_s": time.monotonic() - started,
                    }
                    logging.exception("case failed")
                report["runs"].append({"config": config, "repeat": repeat, **result})
                args.output.write_text(json.dumps(report, indent=2, default=str) + "\n")
                print(
                    json.dumps(
                        {
                            k: result[k]
                            for k in (
                                "tasks_per_s",
                                "claim_latency",
                                "active_wait_samples",
                                "error",
                            )
                            if k in result
                        }
                    ),
                    flush=True,
                )
                # Keep warning/error logs; measurements are aggregated into the result.
                for artifact in directory.iterdir():
                    if artifact.suffix != ".log" or artifact.stat().st_size == 0:
                        artifact.unlink()
                if not any(directory.iterdir()):
                    directory.rmdir()
        finally:
            await admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )
    return int(any("error" in r for r in report["runs"]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=REPO / "benchmarks/matrix.json")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="working-tree")
    parser.add_argument("--cases", default=".", help="Regular expression over case names")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--jobs", type=int)
    parser.add_argument("--seed", type=int, default=42)
    sys.exit(asyncio.run(main(parser.parse_args())))
