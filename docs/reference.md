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
422 invalid input (validated against the published JSON schema; also a priority outside the 32-bit
range, NUL or lone surrogates in keys or inputs, an oversized key, a naive `run_at`). MCP reports
the same cases as tool errors. `FRONTA_SERVER_TOKEN` is required; every REST and MCP request needs
`Authorization: Bearer <token>`; the dashboard at `/` is public and asks for the token once. The
MCP endpoint is `/mcp`. The server binds 127.0.0.1 by default and is meant for a trusted network.

### Reverse proxy

The MCP transport validates the `Host` (and `Origin`) header against an allowlist to defeat DNS
rebinding; with the default loopback bind only loopback names pass. A proxy that keeps the public
hostname needs it listed:

```bash
FRONTA_SERVER_ALLOWED_HOSTS=fronta.example.com          # comma-separated; a bare host matches any port
FRONTA_SERVER_ALLOWED_ORIGINS=https://fronta.example.com  # for browser-based MCP clients
```

```nginx
location / {
    proxy_pass http://127.0.0.1:8000;
    proxy_set_header Host $host;               # or rewrite it to 127.0.0.1:8000 instead of listing it
    proxy_http_version 1.1;
    proxy_buffering off;                       # the MCP endpoint streams server-sent events
}
```

Loopback stays allowed, an unlisted host gets 421 and an unlisted origin 403; REST and the
dashboard do not check the host. Without any allowed host, a non-loopback bind runs the MCP
endpoint with the check disabled (the MCP SDK's default), so prefer listing the names.

### Inputs

Stored inputs are the model's JSON-mode dump by alias with `round_trip=True`: the shape the
published validation schema describes and the server stores. Workers validate stored inputs in
JSON mode accepting both aliases and field names, so strict `datetime`/`UUID` fields, aliased
fields and `Json[T]` fields survive the queue. `enqueue()` validates the encoded input once more
the way a worker will and raises `InvalidInput` when it would not (a serializer that changes the
shape, aliases that differ between validation and serialization), instead of failing the task at
claim. Process tasks receive the same JSON on stdin.

## Configuration

Everything is read from `FRONTA_*` environment variables (see `fronta.Settings`), validated at
startup. The important ones:

| Variable | Default | Meaning |
|---|---|---|
| `FRONTA_DSN` | — | PostgreSQL connection string (required) |
| `FRONTA_CONCURRENCY` | 10 | attempts a worker runs at once (both executors) |
| `FRONTA_LEASE_S` / `FRONTA_HEARTBEAT_S` | 30 / 10 | lease length and heartbeat interval (at most half the lease) |
| `FRONTA_GRACE_S` | 30 | time a stopped attempt gets to end before it is killed |
| `FRONTA_REAPER_INTERVAL_S` / `FRONTA_POLL_INTERVAL_S` | 15 / 5 | reaper and poll fallback |
| `FRONTA_RETENTION_S` | 604800 | terminal rows are purged after 7 days |
| `FRONTA_PAYLOAD_CAP` / `FRONTA_RESULT_CAP` | 1 MiB | UTF-8 bytes of the JSON encoding |
| `FRONTA_PROGRESS_CAP` / `FRONTA_ERROR_CAP` | 64 KiB | |
| `FRONTA_SERVER_HOST` / `FRONTA_SERVER_PORT` / `FRONTA_SERVER_TOKEN` | 127.0.0.1 / 8000 / required | the server never runs without a token |
| `FRONTA_SERVER_ALLOWED_HOSTS` / `FRONTA_SERVER_ALLOWED_ORIGINS` | — | Host / Origin values the MCP endpoint accepts besides loopback (comma-separated) |
| `FRONTA_BWRAP_PATH` | `bwrap` | Linux workers containing process tasks only |
| `LOG_LEVEL_OURS` / `LOG_LEVEL_LIBS` | INFO / WARNING | log levels for Fronta and for libraries |

Fronta's connections carry an `application_name` (`fronta-worker`, `fronta-renewal` for the
worker's reserved lease-renewal connection, `fronta-listener`, `fronta-server`, `fronta-sdk`), so
`pg_stat_activity` tells them apart. Every pool hands out autocommit connections: a single
statement is one round trip and every atomic operation opens an explicit transaction. Durations
must be finite; `FRONTA_DSN` is required only where Fronta opens its own connections (a worker, the
server, the SDK pool, event subscriptions).

Retry policy per task type: `max_attempts` (3), `attempt_timeout` (1 h), `backoff`
(`Backoff(base_s=1, factor=2, cap_s=3600)`, jittered to `[d/2, d]`), `max_concurrency`,
`max_concurrency_per_key`. Each task snapshots the retry policy at enqueue; concurrency limits are
the values last published by a worker and are enforced exactly.

## Guarantees and their limits

- A task is delivered until it reaches a terminal state or exhausts its retry budget. Duplicate
  execution is possible after a lease loss (a stalled worker past its lease may still be running):
  a stale worker's writes are rejected by the execution token, but its side effects are yours.
- A claim never starts with a consumed lease: lease timestamps are taken when the row is written
  (after any lock wait), a claim waits at most half a lease for a type's row and otherwise gives up
  the round, a claimed row starts its attempt as soon as its own claim returns, and a row that
  reaches the worker later than a heartbeat interval renews its lease first (and steps aside when
  the lease is gone). Renewals use a reserved connection with an end-to-end budget of half the slack
  between heartbeat and lease, so a saturated pool or a slow statement cannot cost a healthy task
  its lease.
- Every attempt belongs to its worker until settled: cancelling `Worker.run()` acts like an
  immediate shutdown (stop, settle or explicitly abandon each attempt, then close the pools), and
  an essential background loop that dies ends the worker in order with exit status 71.
- Concurrency limits bound valid leases, not live processes.
- The reaper requeues up to 100 expired leases per pass (every `FRONTA_REAPER_INTERVAL_S`, in
  every worker); after a whole fleet dies, a backlog of `n` expired leases is back in the queue
  within about `n / (100 × workers)` intervals.
- An asyncio handler that ignores cancellation past the grace period makes the worker exit with
  status 70 after recording the attempt and releasing its other tasks: run workers under a
  supervisor that restarts them. A handler that blocks the event loop for a lease trips a watchdog
  with the same status.
- Deterministic bad results (cycles, nesting beyond 200 levels, unstorable text) fail without
  retry; unencodable exception text is sanitized and a broken `__str__` yields placeholder metadata,
  so the outcome is always recorded.
- `SIGTERM`/`SIGINT` stop claiming, give running attempts the grace period, then release the rest
  to the queue without charging a failure. The worker exits only when every outcome is recorded:
  with the database unreachable it keeps retrying instead of losing a completed attempt; a second
  signal skips the grace period and, if a write is still stuck, leaves that task to the reaper.
- On Linux, sandboxed processes die with the worker (`--die-with-parent`); a sandbox orphaned in
  bubblewrap's few-millisecond startup window is killed by the next worker with process tasks
  that starts on the host. CPU time
  and memory rlimits are per process; the PID limit and the tmpfs bounds are sandbox-wide.

## Performance

A claim is one short transaction that walks the queue index in claim order and stops at the first
eligible row (~5 ms on a 70k-row queue; ~25 ms with 50k rows of a saturated higher-priority type
ahead, whose saturation is computed once per claim), then one more transaction records the outcome.
Heartbeats are heap-only updates (`lease_until` is not indexed and the table keeps 10% free space
per page): measured 5000/5000 HOT updates and a third of the WAL of the indexed layout, while the
reaper still finds 2000 running rows in ~3 ms. Listing by key over history uses `tasks_key_idx`
(0.3 ms against 24 ms of sequential scan over 600k rows).

### Changing a task's contract

Claims route by name only. For an incompatible input or policy change use a new name
(`thing_v2`): start its workers, switch producers, keep the old workers until the old name's
queued, scheduled and retrying work has drained, then retire them. A same-name change must stay
compatible for the whole overlap, or needs a drained, coordinated deployment: an older worker that
restarts republishes its own schema and limits for the name (last writer wins). Dedupe and
concurrency keys are scoped by name, so a versioned rollout splits their domains and the combined
concurrency of both names can exceed either limit; never produce the same business work under both
names. The SDK snapshots the retry policy of the definition it enqueues with, the server the
published one; concurrency limits are always the published values. Both
commit durably, so a database's commit rate bounds the task rate: on a laptop Docker PostgreSQL
with fsync that is ~150 no-op tasks/s in total, far above what sandboxed or I/O-bound work needs.
Workers scale out horizontally; claims of a task type without concurrency limits do not serialize.
The stress suite (`tests/test_scale.py`) pins this: claims stay ~5–10 ms on a 60k-row backlog and
behind 30k higher-priority tasks scheduled for later, 2,000 concurrent enqueues (with dedupe) and
200 concurrent API clients stay consistent, 2,000 quick tasks run exactly once with no resource
growth, limits hold under a 600-task keyed burst, and listing/purging cost does not grow with a
100k-row table.
