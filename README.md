# Fronta

```text
8888888888 8888888b.   .d88888b.  888b    888 88888888888     d8888
888        888   Y88b d88P" "Y88b 8888b   888     888        d88888
888        888    888 888     888 88888b  888     888       d88P888
8888888    888   d88P 888     888 888Y88b 888     888      d88P 888
888        8888888P"  888     888 888 Y88b888     888     d88P  888
888        888 T88b   888     888 888  Y88888     888    d88P   888
888        888  T88b  Y88b. .d88P 888   Y8888     888   d8888888888
888        888   T88b  "Y88888P"  888    Y888     888  d88P     888
```

Fronta is an **asyncio task queue for Python backed by PostgreSQL**: use it for background jobs
and sandboxed tools alongside a Postgres application; choose a more mature queue or workflow
platform when you need API stability or built-in orchestration.

**Alpha.** APIs and database schemas can change before 1.0; upgrades require stopping old
workers and clients. The alternatives below have longer production histories.

[Quick start](#quick-start) · [Comparison](#compare-the-options) · [Performance](#performance) ·
[Reference](REFERENCE.md)

## What it does

- **PostgreSQL only.** The SDK and workers connect directly to PostgreSQL. No broker required.
- **Asyncio tasks or processes.** Run Python libraries in `async def` tasks, or arbitrary
  executables wrapped with `bubblewrap` on Linux.
- **Transactional enqueue.** Commit tasks and application writes in the same transaction.
- **Task controls.** Retries with backoff, timeouts, delayed runs, priorities, active-task
  deduplication and per-type/per-key concurrency limits.
- **Event feed.** Live task-state events with durable subscriptions and backfill. Acknowledge
  events and enqueue follow-up tasks in one transaction. Workflow logic is your code.
- **SQL and APIs.** Query state, progress, results and errors with SQL. Optional REST/MCP server
  and dashboard.

**Limits:** tasks can run more than once; external effects must tolerate retries. No built-in
cron, rate limiting, workflow replay or task-output streaming. Queue traffic uses your database's
capacity. [Details](REFERENCE.md).

## Compare the options

Self-hosted setups, checked September 2026. Every option also needs workers; the service column
lists the infrastructure around them. PostgreSQL-only storage is not unique to Fronta.

| Option | Services | Python and tools | Workflows and live data | Maturity / best fit |
|---|---|---|---|---|
| **Fronta** | PostgreSQL; optional API server | Native asyncio handlers; Linux sandboxed executables | Transactional task-state feed and backfill; application owns orchestration | **Alpha**; Python/Postgres apps that want a queue and own their workflow logic |
| [Celery][celery] | Broker, commonly RabbitMQ or Redis; optional result backend | Python [process, thread or greenlet pools][celery-pools]; no standard asyncio pool | [Chains, groups and chords][celery-canvas]; periodic jobs and monitoring events | **Mature**; broad Python ecosystem, routing and worker-pool choices |
| [Hatchet][hatchet] | PostgreSQL + engine/API; [RabbitMQ optional, embedded mode available][hatchet-embedded] | Async/sync Python and other language SDKs | Durable workflows, DAGs, cron and [output streams][hatchet-streams] | **Established workflow platform**; orchestration supplied by the engine |
| [Windmill][windmill] | [PostgreSQL + server][windmill-hosting] | Multi-language scripts, including Python; configurable [nsjail sandbox][windmill-sandbox] | Flows, schedules, approvals and internal apps | **Established automation platform**; scripts, workflows and operator UIs together |
| [Procrastinate][procrastinate] | PostgreSQL | Sync and native async Python tasks | [Periodic jobs, locks and Django integration][procrastinate-features] | **Established queue**; a close alternative for Python/Postgres jobs |

For long-lived workflows with replay and signals, also consider [Temporal][temporal].

## Performance

Fronta batches database operations and runs async handlers concurrently. The checked-in
**0.5.0 benchmarks** report these three-run medians with durable commits, macOS clients and
PostgreSQL 18 in Docker, using eight workers unless noted:

| Workload | Tasks/s | What is timed |
|---|---:|---|
| No-op tasks, default settings | 7,244 | Drain a preloaded backlog |
| No-op tasks, concurrency 256 per worker | 32,887 | Drain a preloaded backlog |
| Live producers + acknowledged completion feed | 3,063 | Enqueue, execute and consume completions |
| 512 KiB inputs, four workers | 242 | Drain a preloaded backlog |

These measure queue overhead, not real application or sandbox throughput. A separate ten-minute
Linux run (i5-13500T, NVMe) completed 1.8 million tasks at **2,994.5/s including final drain**. The
[reports](benchmarks/RESULTS.md) include hardware, settings, missed throughput targets and
slowdowns under long transactions; [reproduce the workloads](benchmarks/README.md).

**We have not benchmarked Fronta against these alternatives under the same conditions.**
[Windmill's cross-engine benchmarks][workflow-benchmarks] use other workloads and omit Fronta.
Compare the same handler, durability, concurrency and hardware before choosing on speed.

## Quick start

Requires **Python 3.12+** and **PostgreSQL 16+** (18 recommended). Python workers run on Linux
and macOS; sandboxed process tasks require Linux. Windows is not supported.

Install the package (`pip install fronta` also works), then initialize the database schema:

```bash
uv add fronta
export FRONTA_DSN=postgresql://user:pass@localhost/app
uv run fronta db init   # creates the fronta schema; safe to repeat
```

The database must exist and the role must be able to create the schema. Define a task and its
worker:

```python
# tasks.py
import fronta
from pydantic import BaseModel


class Text(BaseModel):
    text: str


@fronta.task("word_count", input=Text, max_attempts=5, attempt_timeout=30)
async def word_count(ctx: fronta.Context, job: Text) -> dict[str, int]:
    await ctx.progress({"stage": "counting"})
    return {"words": len(job.text.split())}


worker = fronta.Worker([word_count])
```

Run `uv run fronta worker tasks:worker` in one terminal. In another, with the same `FRONTA_DSN`,
run `uv run python enqueue.py`:

```python
# enqueue.py
import asyncio

import fronta
from tasks import Text, word_count


async def main() -> None:
    await fronta.open_pool()
    try:
        task_id = await word_count.enqueue(Text(text="hello world"), key="example", priority=5)
        print(task_id)
    finally:
        await fronta.close_pool()


asyncio.run(main())
```

Read state, progress and result with `await fronta.get_task(task_id)`. In a service, open the
pool at startup and close it at shutdown. Inputs are Pydantic models; results are JSON.

Pass `conn=conn` inside your own psycopg transaction to commit application writes and the
enqueue together. Use `run_at=` with a timezone-aware datetime for scheduling. A `key`
deduplicates only while a task with that key is queued or running; it is not permanent
idempotency. Handlers must honor `asyncio.CancelledError` and be safe to run twice.

## Sandboxed processes

Run an executable in any language. The worker defines the command; callers supply only data.
The process receives the input as JSON on stdin; other interfaces need a small adapter. This
example needs a Linux worker with `jq`, `bubblewrap`, `util-linux` and unprivileged user
namespaces. Add it to `tasks.py`, replacing the final worker declaration:

```python
uppercase = fronta.process_task(
    "uppercase",
    ["/usr/bin/jq", "-r", ".text | ascii_upcase"],
    input=Text,
    sandbox=fronta.Sandbox(memory_bytes=128 << 20, cpu_time_s=10, max_pids=16),
    max_concurrency=4,
)

worker = fronta.Worker([word_count, uppercase])
```

Enqueue with `await uppercase.enqueue(Text(text="hello world"))`. The result contains
`exit_code`, `stdout`, `stderr` and `truncated`; here stdout is `"HELLO WORLD\n"`. Exit zero
succeeds; other exits follow the retry policy.

Sandboxes get a private `/work` and `/tmp`, allowlisted read-only host paths, a cleared
environment and no network. CPU and address-space limits apply per process. Python handlers run
in the worker's event loop without this isolation, so keep blocking or heavy CPU work out of
them. [Sandbox contract and limits](REFERENCE.md#sandboxed-processes).

## Event feed and workflows

Tasks can enqueue children with `ctx.enqueue(...)`; those enqueues commit independently of the
parent's outcome. For branching, joining or reacting to completions, run a consumer. Workflow
rules and checkpoints stay in your code and your tables; Fronta does not replay workflow code
or provide durable steps.

This consumer chains **uppercase → word count**. Run `uv run python workflow.py` alongside the
Linux worker:

```python
# workflow.py
import asyncio

import fronta
from tasks import Text, word_count


async def main() -> None:
    async with fronta.subscribe(
        "count-uppercase", types=["uppercase"], states=[fronta.State.SUCCEEDED], backfill=True
    ) as feed:
        async for batch in feed:
            for event in batch.events:
                source = await fronta.get_task(event.id, conn=batch.conn)
                await word_count.enqueue(Text(text=source.result["stdout"]), conn=batch.conn)
            await batch.ack()  # child enqueues and acknowledgement commit together


asyncio.run(main())
```

Without an ack, the batch rolls back and is delivered again. Reuse the subscription name across
restarts; several consumers can share it. `backfill=True` projects retained matching tasks into
a new subscription (a duration limits terminal history); interrupted backfills resume, and
startup waits for older transactions without pausing the queue. Delivery is at least once, and
live events can overlap backfill, so real side effects need idempotency.
[Feed guarantees, dedupe and backfill](REFERENCE.md#event-feed).

## Server

Install `fronta[server]`, set `FRONTA_SERVER_TOKEN`, and run `uv run fronta server`. It serves
the dashboard at `/`, REST under `/api/v1` and MCP at `/mcp` on `127.0.0.1:8000`. REST and MCP
require `Authorization: Bearer <token>` and expose enqueue, lookup, listing, cancellation,
pause/resume, requeue and statistics. [Endpoints and reverse proxy setup](REFERENCE.md#server).

## Operating

- **Execution is at least once.** Lease loss can cause duplicates: make external effects
  idempotent.
- **Retention.** Finished tasks and unacknowledged events expire after seven days by default.
  Keep transactions short and size retention for your workload.
- **Deployment.** Run workers under a supervisor that restarts them; configure everything with
  `FRONTA_*` variables. [Settings](REFERENCE.md#configuration) and
  [PostgreSQL configuration](REFERENCE.md#postgresql-configuration).
- **Upgrades.** No backward compatibility between releases: stop old clients, run
  `fronta db init`, start the new release. [Procedure](REFERENCE.md#initialization-and-upgrades).
- **Changing a task's contract.** Workers route by name. For an incompatible change, use a new
  name such as `word_count_v2`, start its workers, switch producers, then drain the old name.
  Dedupe and concurrency limits are scoped by name, so their domains split.

## Development

```bash
uv sync --all-extras
# Use a test database separate from your application.
docker run -d --name fronta-test-pg -e POSTGRES_USER=fronta -e POSTGRES_PASSWORD=fronta \
  -e POSTGRES_DB=fronta -p 127.0.0.1:5439:5432 postgres:18
export FRONTA_TEST_DSN=postgresql://fronta:fronta@127.0.0.1:5439/fronta
make check       # lint, formatting, types, architecture and dependencies
make checkall    # also tests and the dependency audit
```

[Reference](REFERENCE.md) · [Changelog](CHANGELOG.md) · [Benchmarks](benchmarks/README.md) ·
[Public API](src/fronta/__init__.py). MIT licensed; the dashboard bundles Alpine.js
([notice](THIRD_PARTY_NOTICES.md)). Report vulnerabilities privately to the maintainer listed in
`pyproject.toml`.

[celery]: https://docs.celeryq.dev/en/stable/getting-started/introduction.html
[celery-pools]: https://docs.celeryq.dev/en/stable/userguide/concurrency/index.html
[celery-canvas]: https://docs.celeryq.dev/en/stable/userguide/canvas.html
[hatchet]: https://docs.hatchet.run/v1
[hatchet-embedded]: https://docs.hatchet.run/v1/embedded
[hatchet-streams]: https://docs.hatchet.run/v1/advanced-tasks/streaming
[windmill]: https://www.windmill.dev/docs/intro
[windmill-hosting]: https://www.windmill.dev/docs/advanced/self_host
[windmill-sandbox]: https://www.windmill.dev/docs/advanced/security_isolation
[procrastinate]: https://procrastinate.readthedocs.io/en/stable/howto/basics/tasks.html
[procrastinate-features]: https://procrastinate.readthedocs.io/en/stable/howto/advanced.html
[temporal]: https://docs.temporal.io/develop/python
[workflow-benchmarks]: https://www.windmill.dev/docs/misc/benchmarks/competitors
