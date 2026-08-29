# Fronta

Task queue on PostgreSQL for Python. Workers run `async def` handlers in-process and executables
in bubblewrap sandboxes; an optional server exposes the queue over REST and MCP with a small
dashboard.

Fronta keeps its tables in a `fronta` schema of your own PostgreSQL 16+ database; there is no
broker. Workers claim rows with `SELECT … FOR UPDATE SKIP LOCKED`, hold leases renewed by
heartbeats, and record every state change in one fenced transaction, so the tasks of a crashed or
stalled worker are reaped and retried while attempts remain. Priorities, scheduled runs, dedupe
keys, retries with jittered backoff and concurrency limits (per task type and per key) are enforced
in the database; attempt timeouts and cancellation by the worker.

**Status:** alpha. The API and the schema can change between minor versions before 1.0 (see
`CHANGELOG.md`). Linux or macOS, Python 3.12–3.14, PostgreSQL 16+. Sandboxed process workers
require Linux.

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

`enqueue(..., conn=conn)` joins your own psycopg transaction instead of using the pool. `key`
dedupes: while a task with the same key is queued or running, `enqueue` returns its id; once
that task has finished, the same key enqueues a new one.

A handler gets the validated input and a `Context` (`task_id`, `attempt`, `log`, `progress()`,
`enqueue()`, `cancelled`, `state` from the worker lifespan). It must handle
`asyncio.CancelledError` and be safe to run twice: after a lost lease the task runs again.

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

REST under `/api/v1` (task types, enqueue, get, list, cancel), MCP at `/mcp`, dashboard at `/`.
The SDK only enqueues; inspection and cancellation go through the server. `FRONTA_SERVER_TOKEN`
is required (the server never runs open: even on loopback a browser could be made to cancel or
enqueue tasks); every REST and MCP request sends it as `Authorization: Bearer <token>`. Put a
TLS-terminating reverse proxy in front of it outside a private network. Endpoints, inputs and error codes:
[docs/reference.md](https://github.com/dreo/fronta/blob/main/docs/reference.md#server).

## Deploy

One database, any number of workers, optionally a server. Each process reads `FRONTA_*`
environment variables; `FRONTA_DSN` is the only required one. Run workers under a supervisor that
restarts them: a worker exits 0 after a graceful stop and 70 when a handler ignores cancellation or
blocks the event loop.

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
`2 × FRONTA_GRACE_S + 6 × FRONTA_KILL_TIMEOUT_S` (91 s with the defaults 30 s and 5 s) unless the
database is unreachable or a sandbox cannot be killed: then the worker keeps trying rather than
lose an outcome, and systemd's `SIGKILL` at `TimeoutStopSec` ends it. That loses no data:
sandboxes die with the worker and unrecorded attempts are retried when their lease expires.

Throughput is bounded by the database's commit rate (at least two durable commits per task, plus
heartbeats and progress): about 150 no-op tasks/s in total on a laptop PostgreSQL with fsync; a
claim costs ~5 ms on a 70k-row queue. Configuration, retry policy, guarantees and the
measurements: [docs/reference.md](https://github.com/dreo/fronta/blob/main/docs/reference.md).

## Not covered

- Exactly-once side effects: a worker stalled past its lease may still be running while the task
  is retried elsewhere; the stale attempt's writes to Fronta are rejected, its other effects are not.
- Workflows, chains, or periodic tasks (only `run_at`).
- Schema migrations before 1.0: a release that changes the schema needs `fronta db init` on a
  fresh schema.
- Windows.
- Sandboxed process tasks on macOS. A worker containing one fails its startup check with a clear
  platform error; run that worker on Linux.

## Development

```bash
uv sync --all-extras
docker run -d --name fronta-test-pg -e POSTGRES_USER=fronta -e POSTGRES_PASSWORD=fronta \
  -e POSTGRES_DB=fronta -p 127.0.0.1:5439:5432 postgres:16
export FRONTA_TEST_DSN=postgresql://fronta:fronta@127.0.0.1:5439/fronta
make check       # lint, format, types, architecture, deps (also the git pre-commit hook)
make checkall    # check + the full test suite + pip-audit
```

CI runs the full `make checkall` gate once on every pull request and push to `main`; compatibility
legs cover Python 3.12–3.14, lower dependency bounds, real Linux process sandboxes, and the
portable SDK, asyncio worker, and server on macOS without repeating the stress/browser tiers. To
release: set the version (`uv version X.Y.Z`), add the
CHANGELOG section, merge, then push the tag `vX.Y.Z` from that `main` commit; the gate runs again,
the package goes to PyPI and a GitHub release is created. `SPEC.md` is the contract.

## License

MIT. The dashboard bundles Alpine.js (MIT); see `THIRD_PARTY_NOTICES.md`.
