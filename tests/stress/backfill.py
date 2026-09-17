"""Million-row backfill, durable live load, nonblocking barriers and vacuum measurements.

Run through tests.stress.acceptance backfill. Worker, producer and concurrency counts
come from the feed case in benchmarks/matrix.json.
"""

# ruff: noqa: PLR0915, T201

from __future__ import annotations

import asyncio
import json
import random
import time
from collections import defaultdict

import psycopg

from fronta import Settings, Worker, runtime, store, subscribe
from fronta import feed as feed_module
from fronta.model import NewTask, Policy
from tests.conftest import running_all, wait_until
from tests.stress.__main__ import (
    producer_processes,
    rows,
    scalar,
    wait_done,
    worker_chunks,
    worker_metrics,
)
from tests.stress.endurance import Fleet
from tests.workers import In, sleep_task


async def feed_backfill_soak(conn, settings, seed_value, *, seconds=6):
    """Compare actual terminal identities with acknowledged events during continuous work."""
    current = settings.model_copy(update={"concurrency": 16, "lease_s": 10, "heartbeat_s": 1})
    complete = asyncio.Event()
    submitted, delivered = set(), []

    async def produce():
        async with await psycopg.AsyncConnection.connect(
            runtime.dsn_of(current), **runtime.connection_kwargs(current, "backfill-producer")
        ) as producer:
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                submitted.add(await sleep_task.enqueue(In(), conn=producer))
                await asyncio.sleep(0.005)

        async def empty():
            row = await (
                await conn.execute(
                    "SELECT NOT EXISTS (SELECT FROM fronta.tasks "
                    "WHERE state IN ('queued','running'))"
                )
            ).fetchone()
            return row[0]

        await wait_until(empty, timeout=30)
        complete.set()

    async with (
        running_all([Worker([sleep_task], settings=current) for _ in range(2)]),
        asyncio.TaskGroup() as tg,
    ):
        tg.create_task(produce())
        await asyncio.sleep(random.Random(seed_value).uniform(1, 4))  # noqa: S311  # seeded timing
        async with subscribe("fanin", settings=current, backfill=True) as feed:
            while True:
                try:
                    batch = await asyncio.wait_for(anext(feed), 0.5)
                except TimeoutError:
                    if complete.is_set():
                        break
                    continue
                delivered.extend(batch.events)
                await batch.ack()

    rows = await (await conn.execute("SELECT id, state, attempt FROM fronta.tasks")).fetchall()
    terminal = {r[0] for r in rows if r[1] in ("succeeded", "failed", "cancelled")}
    assert terminal == submitted
    assert all(state == "succeeded" and attempt == 1 for _, state, attempt in rows)
    groups = defaultdict(list)
    for event in delivered:
        groups[event.id].append(event)
    for events in groups.values():
        assert len({(e.id, e.state, e.attempt) for e in events}) == 1
        assert len({e.seq for e in events}) == len(events)
    result = {
        "seed": seed_value,
        "tasks": len(terminal),
        "events": len(delivered),
        "duplicates": len(delivered) - len(groups),
        "missing": len(terminal - groups.keys()),
        "unexpected": len(groups.keys() - terminal),
    }
    assert result["missing"] == result["unexpected"] == 0, result
    return result


async def event_storage(conn):
    await conn.execute("SELECT pg_stat_clear_snapshot()")
    return (
        await rows(
            conn,
            "SELECT pg_relation_size('fronta.events') AS heap_bytes, "
            "pg_indexes_size('fronta.events') AS index_bytes, "
            "(SELECT sum(pg_relation_size(indexrelid))::bigint FROM pg_index "
            "WHERE indrelid='fronta.events'::regclass) AS index_main_bytes, "
            "n_live_tup, n_dead_tup, "
            "vacuum_count, autovacuum_count FROM pg_stat_user_tables "
            "WHERE schemaname='fronta' AND relname='events'",
        )
    )[0]


async def consume(feed, conn, producing, expected):
    events = 0
    while True:
        try:
            batch = await asyncio.wait_for(anext(feed), 0.5)
        except TimeoutError:
            if producing.done():
                await producing
                if await scalar(
                    conn, "SELECT count(*) FROM fronta.tasks WHERE state='succeeded'"
                ) == expected and not await scalar(
                    conn, "SELECT EXISTS (SELECT FROM fronta.events)"
                ):
                    return events
            continue
        await batch.conn.execute(
            "INSERT INTO backfill_receipts SELECT * FROM "
            "unnest(%s::bigint[], %s::bigint[], %s::text[], %s::int[])",
            (
                [e.seq for e in batch.events],
                [e.id for e in batch.events],
                [e.state.value for e in batch.events],
                [e.attempt for e in batch.events],
            ),
        )
        await batch.ack()
        events += len(batch.events)


async def contend(conn, dsn):
    polls = []
    captured = asyncio.Event()
    blockers = store.backfill_blockers

    async def timed(*args):
        begin = time.monotonic()
        result = await blockers(*args)
        polls.append(time.monotonic() - begin)
        captured.set()
        return result

    async def register():
        # Empty types isolate the barrier measurement from another million-row backfill.
        async with subscribe("contended", settings=Settings(dsn=dsn), types=[], backfill=True):
            pass

    async with (
        asyncio.timeout(90),
        await psycopg.AsyncConnection.connect(dsn, application_name="backfill-blocker") as holder,
    ):
        await store.enqueue(holder, NewTask("held", "{}", Policy()))
        began = time.monotonic()
        store.backfill_blockers = timed
        try:
            async with asyncio.TaskGroup() as tg:
                creating = tg.create_task(register())
                await captured.wait()
                await asyncio.sleep(30)
                assert not creating.done(), "backfill crossed an open pre-registration transaction"
                assert await scalar(
                    conn,
                    "SELECT backfill IS NOT NULL FROM fronta.subscriptions WHERE name='contended'",
                )
                held_until = time.monotonic()
                await holder.rollback()
        finally:
            store.backfill_blockers = blockers
        elapsed = time.monotonic() - began
        assert await scalar(
            conn, "SELECT backfill IS NULL FROM fronta.subscriptions WHERE name='contended'"
        )
    return {
        "start": began,
        "held_until": held_until,
        "seconds": elapsed,
        "poll_interval_s": feed_module._BACKFILL_POLL_S,
        "poll_count": len(polls),
        "poll_total_s": sum(polls),
        "poll_mean_ms": 1000 * sum(polls) / len(polls),
        "poll_max_ms": 1000 * max(polls),
        "passed": True,  # The caller also verifies worker progress throughout this wait.
    }


async def measure(conn, dsn, args, directory):
    directory.mkdir()
    config = next(c for c in json.loads(args.matrix.read_text()) if c["name"] == "feed")
    config.update(
        rate_s=args.rate / config["producers"],
        timeout_s=600,
        identity=True,
        jobs=int(args.rate * args.seconds),
    )
    fleet = Fleet(dsn, config, directory)
    await conn.execute(
        "INSERT INTO fronta.tasks (type, state, input, attempt, max_attempts, attempt_timeout_s, "
        "backoff_base_s, backoff_factor, backoff_cap_s, finished_at) "
        "SELECT 'history', 'succeeded', '{}', 1, 3, 300, 1, 2, 3600, now()-interval '1h' "
        "FROM generate_series(1, %s)",
        (args.history,),
    )
    await conn.execute("VACUUM ANALYZE fronta.tasks")
    await conn.execute(
        "CREATE TABLE backfill_receipts (seq bigint PRIMARY KEY, "
        "task_id bigint, state text, attempt int)"
    )
    baseline = await event_storage(conn)
    chunks = []
    chunk = store.backfill_chunk

    async def counted(*params):
        result = await chunk(*params)
        assert result is not None
        chunks.append(result[0])
        return result

    producing = None
    try:
        for _ in range(config["workers"]):
            await fleet.add()
        jobs = config["jobs"]
        producing = asyncio.create_task(producer_processes(dsn, config, jobs))
        await asyncio.sleep(5)
        store.backfill_chunk = counted
        registration = time.monotonic()
        async with subscribe(
            "bench", settings=Settings(dsn=dsn), backfill=True, batch_size=1000
        ) as feed:
            backfilled = time.monotonic()
            assert not producing.done(), "live load ended before backfill completed"
            after_backfill = await event_storage(conn)
            async with asyncio.timeout(600):
                events = await consume(feed, conn, producing, args.history + jobs)
        drained = time.monotonic()
        store.backfill_chunk = chunk
        producers = await producing
        await wait_done(conn, args.history + jobs, fleet.active, 30)
        await conn.execute("CREATE INDEX ON backfill_receipts (task_id)")
        missing = await scalar(
            conn,
            "SELECT count(*) FROM fronta.tasks t WHERE NOT EXISTS "
            "(SELECT FROM backfill_receipts r WHERE r.task_id=t.id "
            "AND r.state=t.state AND r.attempt=t.attempt)",
        )
        unexpected = await scalar(
            conn,
            "SELECT count(*) FROM backfill_receipts r LEFT JOIN fronta.tasks t ON t.id=r.task_id "
            "WHERE t.id IS NULL OR (r.state,r.attempt) IS DISTINCT FROM (t.state,t.attempt)",
        )
        duplicates = events - await scalar(
            conn, "SELECT count(DISTINCT task_id) FROM backfill_receipts"
        )
        assert missing == unexpected == 0
        assert await scalar(conn, "SELECT count(*) FROM fronta.events") == 0
        vacuum = []
        # Ordinary VACUUM frees/reuses index pages; it does not promise to shrink index files.
        # AUTO may skip small index cleanups, leaving dead line pointers even in an empty table.
        # Explicit cleanup makes the zero-dead-tuples measurement deterministic without a rewrite.
        vacuum_sql = "VACUUM (ANALYZE, INDEX_CLEANUP ON) fronta.events"
        for _ in range(2):
            await conn.execute(vacuum_sql)
            vacuum.append(await event_storage(conn))
        backfilled_rows = sum(chunks)
        print(
            f"backfill: {backfilled_rows} rows in {backfilled - registration:.3f}s; "
            f"{missing} missing, {duplicates} duplicates",
            flush=True,
        )
        # A second live run keeps an enqueue open for 30s while registration waits without locks.
        producing = asyncio.create_task(producer_processes(dsn, config, int(args.rate * 35)))
        await asyncio.sleep(2)
        contention = await contend(conn, dsn)
        await producing
        await wait_done(conn, args.history + jobs + int(args.rate * 35), fleet.active, 120)
        bad_tasks = await scalar(
            conn,
            "SELECT count(*) FROM fronta.tasks "
            "WHERE state<>'succeeded' OR attempt<>1 OR failures<>0",
        )
        # The existing benchmark subscription deliberately keeps these live events so worker
        # publication has the same cost throughout the contention run.
    finally:
        store.backfill_chunk = chunk
        if producing is not None:
            producing.cancel()
            await asyncio.gather(producing, return_exceptions=True)
        await fleet.close()
    metrics = worker_metrics(fleet.files, registration - 5, registration + 5)
    claim = metrics["operations"]["claim"]
    all_metrics = worker_metrics(fleet.files, registration - 5, time.monotonic())
    exits = [p.returncode for p in fleet.all]
    worker_errors = [
        line
        for path in directory.glob("*.log")
        for line in path.read_text().splitlines()
        if "ERROR" in line or "Traceback" in line
    ]
    completions = [
        entry
        for path in fleet.files
        for chunk_ in worker_chunks(path)
        for entry in chunk_["operations"].get("complete", [])
    ]
    progress = []
    # Full five-second intervals avoid making an assertion about a partial last interval.
    for start in range(int((contention["held_until"] - contention["start"]) // 5)):
        completed = sum(
            entry[3]
            for entry in completions
            if contention["start"] + 5 * start
            <= entry[0] + entry[1]
            < contention["start"] + 5 * (start + 1)
        )
        progress.append(completed)
    contention["completions_per_5s"] = progress
    contention["worker_metrics_while_waiting"] = worker_metrics(
        fleet.files, contention["start"], contention["held_until"]
    )
    contention["passed"] = bool(progress) and all(n > 0 for n in progress)
    storage_passed = vacuum[-1]["n_dead_tup"] == 0 and all(
        vacuum[-1][key] <= vacuum[0][key] for key in ("heap_bytes", "index_main_bytes")
    )
    return {
        "config": config,
        "retained": args.history,
        "live_jobs": jobs,
        "target_completion_rate_s": args.rate,
        "backfill_seconds": backfilled - registration,
        "backfilled_rows": backfilled_rows,
        "chunks": len(chunks),
        "backfill_rows_per_s": backfilled_rows / (backfilled - registration),
        "drain_seconds": drained - backfilled,
        "events": events,
        "duplicates": duplicates,
        "missing": missing,
        "unexpected": unexpected,
        "claim_10s": claim,
        "worker_metrics": all_metrics,
        "worker_exits": exits,
        "worker_errors": worker_errors,
        "bad_tasks": bad_tasks,
        "producer_metrics": producers,
        "storage": {
            "vacuum_sql": vacuum_sql,
            "before": baseline,
            "after_backfill": after_backfill,
            "after_vacuum": vacuum,
            "stable_after_two_vacuums": storage_passed,
            "returned_to_initial_bytes": all(
                vacuum[-1][key] <= baseline[key] for key in ("heap_bytes", "index_bytes")
            ),
        },
        "contention": contention,
        "passed": not any(exits)
        and not worker_errors
        and bad_tasks == 0
        and missing == unexpected == 0
        and storage_passed
        and contention["passed"],
    }
