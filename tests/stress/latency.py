"""Measure chain, feed and cooperative cancellation latency on a disposable database."""

from __future__ import annotations

import asyncio
import time

import psycopg

from fronta import Settings, Worker, runtime, store, subscribe, task
from fronta.hints import Hints
from fronta.server.service import Service
from tests.conftest import running, running_all
from tests.stress.__main__ import percentiles
from tests.stress.worker import Input


async def sparse(worker, definition, finished):
    """Interleave 1,000 sparse tasks per path; both use the identical durable outcome SQL."""
    writer = worker._completions
    automatic = writer.complete
    durations = {"automatic": [], "direct": []}

    async def direct(outcome):
        return (await writer.write([outcome])).get(outcome.id)

    try:
        for _ in range(10):
            for mode, method in (("automatic", automatic), ("direct", direct)):
                writer.complete = method
                for _ in range(100):
                    finished.clear()
                    started = time.monotonic()
                    await definition.enqueue(Input(n=9))
                    await asyncio.wait_for(finished.wait(), 5)
                    durations[mode].append(time.monotonic() - started)
    finally:
        writer.complete = automatic
    metrics = {name: percentiles(values) for name, values in durations.items()}
    metrics["median_added_ms"] = metrics["automatic"]["p50_ms"] - metrics["direct"]["p50_ms"]
    return metrics


async def idle_polling(definition, settings):
    original, count = store.claim, 0

    async def claim(*args, **kwargs):
        nonlocal count
        result = await original(*args, **kwargs)
        assert not result
        count += 1
        return result

    store.claim = claim
    try:
        async with running_all([Worker([definition], settings=settings) for _ in range(8)]):
            await asyncio.sleep(3)  # reach the one-second backoff ceiling
            count = 0
            start = time.monotonic()
            await asyncio.sleep(30)
            elapsed = time.monotonic() - start
            return {
                "workers": 8,
                "seconds": elapsed,
                "empty_claims": count,
                "claims_per_s": count / elapsed,
            }
    finally:
        store.claim = original


async def measure(_conn, dsn, repetitions):  # noqa: PLR0915  # linear measurement procedure
    settings = Settings(dsn=dsn)
    await runtime.open_pool(settings)
    completed, received, last_ids = {}, {}, set()
    completion, send = store.complete, Hints._send
    finished = asyncio.Event()
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def complete(conn, outcomes):
        before = time.monotonic()
        result = await completion(conn, outcomes)
        after = time.monotonic()
        for task_id in result:
            completed[task_id] = (before, after)
            if task_id in last_ids:
                finished.set()
        return result

    @task("latency_chain", input=Input)
    async def chain(ctx, inp):
        if inp.n < 9:
            await ctx.enqueue(chain, Input(n=inp.n + 1))
        else:
            last_ids.add(ctx._attempt.row.id)

    @task("latency_cancel", input=Input)
    async def sleeper(_ctx, _inp):
        entered.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def broken_send(*_args, **_kwargs):
        raise psycopg.OperationalError("latency test: hints disconnected")

    async def consume(feed):
        async for batch in feed:
            stamp = time.monotonic()
            for event in batch.events:
                received[event.id] = stamp
            await batch.ack()

    worker = Worker([chain, sleeper], settings=settings)
    service = Service(settings)
    await service.start()
    chains, cancellations = {}, []
    store.complete = complete
    try:
        async with running(worker), subscribe("latency", settings=settings) as feed:
            consumer = asyncio.create_task(consume(feed))
            try:
                for broken in (False, True):
                    Hints._send = broken_send if broken else send
                    durations = []
                    for _ in range(int(repetitions)):
                        finished.clear()
                        started = time.monotonic()
                        await chain.enqueue(Input())
                        await asyncio.wait_for(finished.wait(), 10)
                        durations.append(time.monotonic() - started)
                    chains["broken_hints" if broken else "hints"] = percentiles(durations)
                    if not broken:
                        # Only connected-hint transitions enter the feed latency measurement.
                        async with asyncio.timeout(5):
                            while not completed.keys() <= received.keys():
                                await asyncio.sleep(0.001)
                        upper = [received[i] - start for i, (start, _) in completed.items()]
                        lower = [received[i] - end for i, (_, end) in completed.items()]
                        feed_bounds = {"upper": percentiles(upper), "lower": percentiles(lower)}
                Hints._send = send
                await runtime.close_hints()
                for _ in range(int(repetitions)):
                    entered.clear()
                    cancelled.clear()
                    task_id = await sleeper.enqueue(Input())
                    await asyncio.wait_for(entered.wait(), 5)
                    started = time.monotonic()
                    await service.cancel(task_id)
                    await asyncio.wait_for(cancelled.wait(), 5)
                    cancellations.append(time.monotonic() - started)
                sparse_metrics = await sparse(worker, chain, finished)
            finally:
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
    finally:
        store.complete, Hints._send = completion, send
        await service.stop()
        await runtime.close_pool()
    result = {
        "chains": chains,
        "feed_commit_latency_bounds": feed_bounds,
        "cancel": percentiles(cancellations),
        "sparse": sparse_metrics,
        "idle_polling": await idle_polling(chain, settings),
    }
    result["passed"] = (
        chains["hints"]["p50_ms"] < 100
        and chains["broken_hints"]["p50_ms"] < 1500
        and feed_bounds["upper"]["p50_ms"] < 20
        and result["cancel"]["max_ms"] < 50
        and sparse_metrics["median_added_ms"] < 1
        and result["idle_polling"]["claims_per_s"] <= 9
    )
    return result
