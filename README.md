# Fronta

Task queue on PostgreSQL for Python. Workers run `async def` handlers in-process and executables
in bubblewrap sandboxes; an optional server exposes the queue over REST and MCP with a small
dashboard.

Fronta keeps its tables in a `fronta` schema of your own PostgreSQL database; there is no
broker. PostgreSQL 18 is the default; versions 16+ remain supported.
Workers claim rows with `SELECT … FOR UPDATE SKIP LOCKED`, hold leases renewed by
heartbeats, and record every state change in one fenced statement, so the tasks of a crashed or
stalled worker are reaped and retried while attempts remain. Priorities, scheduled runs, dedupe
keys, retries with jittered backoff and concurrency limits (per task type and per key) are enforced
in the database; attempt timeouts and cancellation by the worker.

**Status:** alpha. The API and the schema can change between minor versions before 1.0 (see
`CHANGELOG.md`). Linux or macOS, Python 3.12–3.14, PostgreSQL 18 by default (16+ supported).
Sandboxed process workers require Linux.

## Install

```bash
uv add fronta               # SDK + worker
uv add "fronta[server]"     # + REST/MCP server and dashboard
```

`pip install fronta` works the same. The SDK, server, and workers containing only asyncio tasks are
supported on Linux and macOS. A worker containing any process task needs Linux, `bwrap`
(bubblewrap), `prlimit` (util-linux), and unprivileged user namespaces.

## Example

```python
# app/tasks.py
import fronta
from pydantic import BaseModel


class Resize(BaseModel):
    image_id: int
    width: int


@fronta.task("resize", input=Resize, max_attempts=5, attempt_timeout=120)
async def resize(ctx: fronta.Context, job: Resize) -> dict[str, int]:
    await ctx.progress({"stage": "download"})
    ...  # idempotent work that honors CancelledError
    return {"bytes": 12345}


worker = fronta.Worker([resize])
```

```bash
export FRONTA_DSN=postgresql://user:pass@host/db   # the role must be able to create the schema
fronta db init                    # creates schema `fronta`; safe to repeat
fronta worker app.tasks:worker    # runs until SIGTERM/SIGINT
```

```python
# enqueue.py: any process that reaches the database
import asyncio

import fronta
from app.tasks import Resize, resize


async def main() -> None:
    await fronta.open_pool()  # once, at application start
    try:
        task_id = await resize.enqueue(Resize(image_id=7, width=800), priority=5, key="resize-7")
        print(task_id)
    finally:
        await fronta.close_pool()  # at application shutdown


asyncio.run(main())
```

`enqueue(..., conn=conn)` joins a non-autocommit psycopg transaction instead of using the pool;
an explicit `conn.transaction()` block also stays caller-owned. Otherwise the insert commits as
one statement and Fronta sends a wake hint afterwards. Caller-owned enqueue uses transactional
NOTIFY, which shares PostgreSQL's notification commit lock; batching enqueues in one transaction
amortizes that cost.
`key` dedupes: while a task with the same key is queued or running, `enqueue` returns its id;
once that task has finished, the same key enqueues a new one.

A handler gets the validated input and a `Context` (`task_id`, `attempt`, `log`, `progress()`,
`enqueue()`, `cancelled`, `metadata`, `state` from the worker lifespan). It must handle
`asyncio.CancelledError` and be safe to run twice: after a lost lease the task runs again.
Inputs are stored as the model's JSON by alias (the shape of the published schema) and validated
again in JSON mode at claim, so strict and aliased fields survive the round trip; an input that
would not, or an invalid argument (priority outside the integer range, NUL in a key, a naive
`run_at`), is refused by `enqueue` with `InvalidInput` before anything is written.

## Event feed

Named subscriptions retain matching transitions in PostgreSQL until acknowledged. A workflow can
enqueue a child and acknowledge its source event in the same database transaction:

```python
import fronta
from app.tasks import Notify, notify


async def run_workflow() -> None:
    async with fronta.subscribe(
        "resize-workflow", states=[fronta.State.SUCCEEDED], types=["resize"]
    ) as feed:
        async for batch in feed:
            for event in batch.events:
                source = await fronta.get_task(event.id, conn=batch.conn)
                await notify.enqueue(
                    Notify(source_id=event.id, result=source.result),
                    conn=batch.conn,
                )
            await batch.ack()  # commits both child enqueues and deletion of these events
```

`TaskEvent` contains `seq`, `id`, `type`, `state`, and `attempt`. Without acknowledgement the
transaction rolls back and the events are delivered again. Multiple consumers may share a
subscription; locked deliveries are skipped. Closing the consumer leaves its subscription and
backlog intact; `await fronta.unsubscribe(name)` deletes both.

Each pull orders visible, unlocked rows by sequence. Sequence allocation precedes commit, so a
lower sequence may arrive later; no cursor advances past it. Registration does not backfill old
transitions or transitions whose statements already read the subscriptions. `get_task()` reads
the latest row, which may be newer than the event or already purged. Unacknowledged events expire
`retention_s` after the event, with warnings in worker logs; terminal tasks expire that interval
after finishing.

Database reactions using `batch.conn` and `ack()` commit atomically. External effects remain at
least once; use `(subscription, seq)` as their idempotency key. The feed and its limitations are
specified in [the reference](docs/reference.md#event-feed).

Keep transaction and feed batch blocks short: a long open transaction can delay PostgreSQL
vacuum cleanup and slow the queue. Retention and this operating constraint are explained in
[the reference](docs/reference.md#event-feed).

On Linux, a sandboxed process task with a placeholder executable:

```python
class Convert(BaseModel):
    source: str


convert = fronta.process_task(
    "convert",
    ["/usr/bin/convert-tool", "--from-stdin"],  # reads the JSON input on stdin
    input=Convert,
    sandbox=fronta.Sandbox(memory_bytes=512 << 20, cpu_time_s=60, max_pids=16),
    max_concurrency=4,
)
```

The process runs in a private tmpfs `/work` without network; its result is
`{"exit_code", "stdout", "stderr", "truncated"}`. Exit code 0 means the task succeeded; anything
else fails the attempt.

## Server

```bash
FRONTA_SERVER_TOKEN=... fronta server      # 127.0.0.1:8000
```

REST under `/api/v1` (task types, enqueue, get, list, cancel, pause/resume, requeue, stats),
MCP at `/mcp`, dashboard at `/`. The SDK also exposes `pause(type)`, `resume(type)`, `requeue(id)`,
and `stats()`. Enqueue accepts optional JSON `metadata`, available on the row and `ctx.metadata`;
children inherit none automatically. Listing and cancellation go through the server.
`FRONTA_SERVER_TOKEN` is required (the server never runs open:
even on loopback a browser could be made to cancel or enqueue tasks); every REST and MCP request
sends it as `Authorization: Bearer <token>`. Put a TLS-terminating reverse proxy in front of it
outside a private network; a proxy that forwards the public hostname must be listed in
`FRONTA_SERVER_ALLOWED_HOSTS` (and its origin in `FRONTA_SERVER_ALLOWED_ORIGINS`), or the MCP
endpoint's DNS-rebinding protection answers 421. Endpoints, inputs, error codes and a proxy
example: [docs/reference.md](https://github.com/dreo/fronta/blob/main/docs/reference.md#server).

## Deploy

One database, any number of workers, optionally a server. Each process reads `FRONTA_*`
environment variables; `FRONTA_DSN` is the only required one (and only where Fronta opens its own
connections: enqueueing through your own connection needs none). A worker holds up to
`FRONTA_POOL_SIZE` connections, one listener, one lease-renewal connection, and one lazy
hint connection. Workers, the server and SDK share hints for the same database and event loop;
each event-feed consumer needs one more connection. Reserved renewals keep pool saturation from
blocking heartbeats. Run workers under a supervisor that restarts them: a worker
exits 0 after a graceful stop, 70 when a handler ignores cancellation or blocks the event loop, and
71 when one of its background loops dies of an unexpected error.

```ini
# /etc/systemd/system/fronta-worker.service
[Unit]
Description=Fronta worker
After=network-online.target

[Service]
User=app
WorkingDirectory=/srv/app
# FRONTA_DSN=... and other FRONTA_* variables; readable by root only (mode 0600)
EnvironmentFile=/etc/fronta/worker.env
ExecStart=/srv/app/.venv/bin/fronta worker app.tasks:worker
# SIGTERM goes to the worker only, which stops its sandboxes itself; SIGKILL to everything
KillMode=mixed
Restart=always
RestartSec=2
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
```

`systemctl enable --now fronta-worker`. On `SIGTERM` the worker stops claiming, lets running
attempts finish for `FRONTA_GRACE_S`, then stops the rest (another grace period for cooperative
cancellation, then a kill) and records every outcome before it exits. That takes at most
`2 × FRONTA_GRACE_S + 6 × FRONTA_KILL_TIMEOUT_S + 1` (91 s with the defaults 30 s and 5 s) unless the
database is unreachable or a sandbox cannot be killed: then the worker keeps trying rather than
lose an outcome, and systemd's `SIGKILL` at `TimeoutStopSec` ends it. That loses no data:
sandboxes die with the worker and unrecorded attempts are retried when their lease expires.

Claims and completions batch automatically: up to free execution slots and 256 rows per claim,
with a 512 KiB stored-input budget (one oversized input is still admitted). Completions flush
immediately when idle and coalesce while a write is in flight. Multi-type workers use the same
path. Idle polling backs off from 50 ms to one second; wake hints and local child enqueues reset
it. Worker-level heartbeats renew due attempts in chunks of 1,000. Task writes retain synchronous
commit; wake, cancel and feed hints use a separate connection.

Upgrade steps and API/configuration details: [reference](docs/reference.md).
For sustained traffic, start with the [PostgreSQL 18 configuration](docs/postgresql.md).

Pre-release measurements reached **11.6k no-op tasks/s at Fronta defaults** and **5.4k/s with
live SDK producers and a terminal feed**. A physical Linux run processed **32.4 million tasks
over three hours**, recovered after an injected long transaction, and reached stable storage
with retention enabled. These are workload-specific measurements; [results and limits](benchmarks/RESULTS.md)
identify the tested source versions and distinguish normal operation from fault recovery.

## Changing a task's contract

Claims route by name, so a worker of an older version happily claims inputs written for a newer
one and fails them permanently when the schema is incompatible, and the last worker to start
publishes the limits and schema for the whole fleet. The safe procedure for an incompatible change
is a new name: declare `resize_v2`, start its workers, switch producers, and keep the old workers
until the old name's queued, scheduled and retrying work has drained. A same-name change must stay
compatible for the whole overlap, or needs a drained, coordinated deployment. Dedupe and
concurrency keys are scoped by name, so a versioned rollout splits their domains (the combined
concurrency of both names can exceed either limit); do not produce the same business work under
both names. The SDK snapshots the retry policy of the definition it enqueues with; the server
snapshots the published one.

## Not covered

- Exactly-once side effects: a worker stalled past its lease may still be running while the task
  is retried elsewhere; the stale attempt's writes to Fronta are rejected, its other effects are not.
- A built-in workflow engine, chains, or periodic tasks (only `run_at`). External consumers can implement
  workflows using named event subscriptions.
- Arbitrary schema migrations: init applies the additive changes shipped by this version;
  incompatible future changes need release-specific deployment instructions.
- Durable workflow steps, inline execution, rate limits, priority aging, tenant fairness, task
  expiry, or cancellation propagation to children.
- Windows.
- Sandboxed process tasks on macOS. A worker containing one fails its startup check with a clear
  platform error; run that worker on Linux.

## Development

```bash
uv sync --all-extras
docker run -d --name fronta-test-pg -e POSTGRES_USER=fronta -e POSTGRES_PASSWORD=fronta \
  -e POSTGRES_DB=fronta -p 127.0.0.1:5439:5432 postgres:18
export FRONTA_TEST_DSN=postgresql://fronta:fronta@127.0.0.1:5439/fronta
make check       # lint, format, types, architecture, deps (also the git pre-commit hook)
make checkall    # check + the full test suite + pip-audit
```

CI runs the full `make checkall` gate once on every pull request and push to `main`; compatibility
legs cover Python 3.12–3.14, lower dependency bounds with PostgreSQL 16, real Linux process
sandboxes, and the portable SDK, asyncio worker, and server on macOS without repeating the
stress/browser tiers. To release: set the version (`uv version X.Y.Z`), add the
CHANGELOG section, merge, then push the tag `vX.Y.Z` from that `main` commit; the gate runs again,
the package goes to PyPI and a GitHub release is created. `SPEC.md` is the contract.

## License

MIT. The dashboard bundles Alpine.js (MIT); see `THIRD_PARTY_NOTICES.md`.
