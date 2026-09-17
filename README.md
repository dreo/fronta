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

**A task queue for Python that lives in your PostgreSQL database, with optional sandboxed
execution of untrusted-input tools.**

Fronta is for applications that already run on PostgreSQL and asyncio and want background work
without adding a message broker. Your code enqueues a task in the same transaction as the
business change that needs it; workers claim tasks with `FOR UPDATE SKIP LOCKED`, run either an
`async def` handler or an executable in a Linux sandbox, and write the outcome back to the same
database. Every task is a row: its state, input, result, error and progress can be read with
SQL or through the Python API.

**Use it if you:**

- run PostgreSQL 16+ (18 recommended) and Python 3.12+ asyncio code on Linux or macOS;
- want transactional enqueue, retries with jittered backoff, scheduled runs, priorities, dedupe
  keys and exact per-type and per-key concurrency limits, all enforced in the database;
- need to run tools that consume hostile input (parsers, converters, CPU-heavy commands, in any
  language) in a separate process with a private filesystem, no network and resource limits;
- want a durable, acknowledged event feed so your own code can react to task outcomes and build
  workflows on top.

**Look elsewhere if you need:** a broker-based queue (RabbitMQ, Redis), a durable workflow
engine that replays code after failures, cron or periodic scheduling, rate limiting,
exactly-once side effects, Windows, or a stable API. Fronta is **alpha**: the API and the
schema can change between minor versions before 1.0, and upgrades require stopping old clients.

## How it works

```text
 Python SDK --------+
 REST / MCP server -+---> PostgreSQL <----> workers
                         tasks + events     |-- async Python handlers
                               |            `-- Linux process sandboxes
                               v
                    your event consumers (optional)
```

- **Enqueue** inserts a row, optionally inside your own transaction. A `key` deduplicates
  against queued and running tasks; `run_at` schedules; `priority` orders.
- **Workers** claim batches in priority order, hold leases renewed by heartbeats, and record
  every outcome in one fenced statement: a worker that lost its lease cannot overwrite a newer
  attempt's result. Crashed workers' tasks are reaped and retried while attempts remain.
- **Handlers** are `async def` functions in the worker process. **Process tasks** are
  executables run by bubblewrap with a private `/work`, allowlisted read-only host paths, no
  network, a cleared environment and CPU, memory and PID limits.
- **Event feed:** named subscriptions receive one durable row per state transition and
  acknowledge batches; a consumer can enqueue the next task and acknowledge in the same
  transaction. New subscriptions can backfill retained history.
- **Server** (optional): REST, an MCP endpoint and a small dashboard for enqueueing, inspection,
  cancellation, pause/resume and requeue. Workers and the SDK talk to PostgreSQL directly.

Measured with durable commits: a physical Linux host (i5-13500T, NVMe) sustained 3,000 tasks/s
for ten minutes, 1.8 million tasks, through a held-open transaction; a laptop against Docker
PostgreSQL drained a preloaded backlog of no-op tasks at 32,887 tasks/s. Details, hardware
and limitations are in the [benchmark results](benchmarks/RESULTS.md).

## Compared with

| Project | Required service | Where it differs from Fronta |
|---|---|---|
| [Procrastinate](https://procrastinate.readthedocs.io/en/stable/) | PostgreSQL | Sync and async tasks, Django integration, periodic jobs; no sandboxing or event feed. |
| [PGQueuer](https://github.com/janbjorge/pgqueuer) | PostgreSQL | asyncpg-based job queue with cron-style scheduling; no sandboxing or transactional event feed. |
| [Celery](https://docs.celeryq.dev/en/stable/getting-started/introduction.html) | RabbitMQ or Redis | Workflow composition, routing and periodic scheduling; broker-based delivery. |
| [Dramatiq](https://dramatiq.io/) | RabbitMQ or Redis | Actor-style tasks, retries and middleware; broker-based. |
| [arq](https://arq-docs.helpmanual.io/) / [RQ](https://python-rq.org/) | Redis | Small Redis job queues (asyncio and sync respectively). |
| [Temporal](https://docs.temporal.io/) | Temporal service | Durable workflows that resume after failures; much larger operational footprint. |

Fronta's distinguishing pieces are fenced execution tokens and exact concurrency limits inside
the claim transaction, sandboxed executables as first-class tasks, and a transactional event
feed with backfill, all with PostgreSQL as the only dependency.

## Quick start

Install the published package with `uv add fronta` (or `pip install fronta`). This README
describes the main branch (0.6.0, unreleased); to try it, install from source:

```bash
uv add git+https://github.com/dreo/fronta.git
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
