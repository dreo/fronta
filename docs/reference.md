# Fronta reference

Details behind the [README](../README.md): the HTTP/MCP interface, configuration, retry policy, guarantees and measured throughput.

## Server

| Operation | REST | MCP tool |
|---|---|---|
| list task types | `GET /api/v1/task-types` | `list_task_types` |
| enqueue | `POST /api/v1/tasks` `{type, input, priority?, run_at?, key?, concurrency_key?}` → 201 `{id}` | `enqueue` |
| get task | `GET /api/v1/tasks/{id}` | `get_task` |
| list tasks | `GET /api/v1/tasks?type&state&key&before&limit` → `{items, next}` | `list_tasks` |
| cancel | `POST /api/v1/tasks/{id}/cancel` → `{id, state}` | `cancel` |

Errors: 401 missing/invalid token, 404 unknown type or id, 409 not cancellable, 413 over the cap,
422 invalid input (validated against the published JSON schema). With `FRONTA_SERVER_TOKEN` set,
every REST and MCP request needs `Authorization: Bearer <token>`; the dashboard at `/` is public
and asks for the token once. The MCP endpoint is `/mcp`. The server binds 127.0.0.1 by default and
is meant for a trusted network.

## Configuration

Everything is read from `FRONTA_*` environment variables (see `fronta.Settings`), validated at
startup. The important ones:

| Variable | Default | Meaning |
|---|---|---|
| `FRONTA_DSN` | — | PostgreSQL connection string (required) |
| `FRONTA_CONCURRENCY` | 10 | attempts a worker runs at once (both executors) |
| `FRONTA_LEASE_S` / `FRONTA_HEARTBEAT_S` | 30 / 10 | lease length and heartbeat interval |
| `FRONTA_GRACE_S` | 30 | time a stopped attempt gets to end before it is killed |
| `FRONTA_REAPER_INTERVAL_S` / `FRONTA_POLL_INTERVAL_S` | 15 / 5 | reaper and poll fallback |
| `FRONTA_RETENTION_S` | 604800 | terminal rows are purged after 7 days |
| `FRONTA_PAYLOAD_CAP` / `FRONTA_RESULT_CAP` | 1 MiB | UTF-8 bytes of the JSON encoding |
| `FRONTA_PROGRESS_CAP` / `FRONTA_ERROR_CAP` | 64 KiB | |
| `FRONTA_SERVER_HOST` / `FRONTA_SERVER_PORT` / `FRONTA_SERVER_TOKEN` | 127.0.0.1 / 8000 / unset | |
| `FRONTA_BWRAP_PATH` | `bwrap` | |
| `LOG_LEVEL_OURS` / `LOG_LEVEL_LIBS` | INFO / WARNING | log levels for Fronta and for libraries |

Fronta's connections carry an `application_name` (`fronta-worker`, `fronta-listener`,
`fronta-server`, `fronta-sdk`), so `pg_stat_activity` tells them apart.

Retry policy per task type: `max_attempts` (3), `attempt_timeout` (1 h), `backoff`
(`Backoff(base_s=1, factor=2, cap_s=3600)`, jittered to `[d/2, d]`), `max_concurrency`,
`max_concurrency_per_key`. Each task snapshots the retry policy at enqueue; concurrency limits are
the values last published by a worker and are enforced exactly.

## Guarantees and their limits

- A task is delivered until it reaches a terminal state or exhausts its retry budget. Duplicate
  execution is possible after a lease loss (a stalled worker past its lease may still be running):
  a stale worker's writes are rejected by the execution token, but its side effects are yours.
- Concurrency limits bound valid leases, not live processes.
- An asyncio handler that ignores cancellation past the grace period makes the worker exit with
  status 70 after recording the attempt and releasing its other tasks: run workers under a
  supervisor that restarts them. A handler that blocks the event loop for a lease trips a watchdog
  with the same status.
- `SIGTERM`/`SIGINT` stop claiming, give running attempts the grace period, then release the rest
  to the queue without charging a failure. The worker exits only when every outcome is recorded:
  with the database unreachable it keeps retrying instead of losing a completed attempt; a second
  signal skips the grace period and, if a write is still stuck, leaves that task to the reaper.
- Sandboxed processes die with the worker (`--die-with-parent`); a sandbox orphaned in
  bubblewrap's few-millisecond startup window is killed by the next worker on the host. CPU time
  and memory rlimits are per process; the PID limit and the tmpfs bounds are sandbox-wide.

## Performance

A claim is one short transaction that walks the queue index in claim order and stops at the first
eligible row (~5 ms on a 70k-row queue), then one more transaction records the outcome. Both
commit durably, so a database's commit rate bounds the task rate: on a laptop Docker PostgreSQL
with fsync that is ~150 no-op tasks/s in total, far above what sandboxed or I/O-bound work needs.
Workers scale out horizontally; claims of a task type without concurrency limits do not serialize.
The stress suite (`tests/test_scale.py`) pins this: claims stay ~5–10 ms on a 60k-row backlog and
behind 30k higher-priority tasks scheduled for later, 2,000 concurrent enqueues (with dedupe) and
200 concurrent API clients stay consistent, 2,000 quick tasks run exactly once with no resource
growth, limits hold under a 600-task keyed burst, and listing/purging cost does not grow with a
100k-row table.
