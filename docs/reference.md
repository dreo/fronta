# Fronta reference

Details behind the [README](../README.md): the HTTP/MCP interface, configuration, retry policy, guarantees and measured throughput.

## Server

| Operation | REST | MCP tool |
|---|---|---|
| list task types | `GET /api/v1/task-types` | `list_task_types` |
| enqueue | `POST /api/v1/tasks` `{type, input, priority?, run_at?, key?, concurrency_key?, metadata?}` → 201 `{id}` | `enqueue` |
| get task | `GET /api/v1/tasks/{id}` | `get_task` |
| list tasks | `GET /api/v1/tasks?type&state&key&before&limit` → `{items, next}` | `list_tasks` |
| cancel | `POST /api/v1/tasks/{id}/cancel` → `{id, state}` | `cancel` |
| pause / resume type | `POST /api/v1/task-types/{type}/pause` or `/resume` | `pause` / `resume` |
| requeue | `POST /api/v1/tasks/{id}/requeue` → `{id, state}` | `requeue` |
| stats | `GET /api/v1/stats` | `stats` |

Errors: 401 missing/invalid token, 404 unknown type or id, 409 not cancellable or not requeueable, 413 over the cap,
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

Declared outputs serialize using their serialization aliases and custom serializers. Fronta checks
that stored JSON against the published output schema, including when a handler returns a mutated
model instance. Invalid results fail without retry. Request bodies allow `payload_cap + progress_cap
+ 64 KiB`; the individual input and metadata caps still apply. Unstorable or excessively deep
inputs return 422 before insertion.

## Configuration

Everything is read from `FRONTA_*` environment variables (see `fronta.Settings`), validated at
startup. Durations are seconds; caps are bytes:

| Variable | Default | Meaning |
|---|---|---|
| `FRONTA_DSN` | — | PostgreSQL connection string (required) |
| `FRONTA_CONCURRENCY` | 10 | attempts a worker runs at once (both executors) |
| `FRONTA_POOL_SIZE` | 4 | ordinary connections per worker/SDK pool; renewals have a reserved connection |
| `FRONTA_CONNECT_TIMEOUT_S` / `FRONTA_STATEMENT_TIMEOUT_S` | 10 / 30 | connection/readiness and statement deadlines |
| `FRONTA_KILL_TIMEOUT_S` | 5 | each hard-stop/verification wait |
| `FRONTA_PURGE_INTERVAL_S` / `FRONTA_PURGE_BATCH` | 600 / 1000 | cleanup interval and maximum rows per deletion batch |
| `FRONTA_LIST_PAGE_SIZE` / `FRONTA_LIST_PAGE_MAX` | 50 / 200 | default and maximum task-list page size |
| `FRONTA_LEASE_S` / `FRONTA_HEARTBEAT_S` | 30 / 10 | lease length and heartbeat interval (at most half the lease) |
| `FRONTA_GRACE_S` | 30 | time a stopped attempt gets to end before it is killed |
| `FRONTA_REAPER_INTERVAL_S` / `FRONTA_POLL_INTERVAL_S` | 15 / 1 | reaper interval and maximum idle poll delay |
| `FRONTA_RETENTION_S` | 604800 | terminal tasks and unacknowledged events expire after 7 days |
| `FRONTA_PAYLOAD_CAP` / `FRONTA_RESULT_CAP` | 1 MiB | UTF-8 bytes of the JSON encoding |
| `FRONTA_PROGRESS_CAP` / `FRONTA_ERROR_CAP` | 64 KiB | metadata shares the progress cap |
| `FRONTA_SERVER_HOST` / `FRONTA_SERVER_PORT` / `FRONTA_SERVER_TOKEN` | 127.0.0.1 / 8000 / required | the server never runs without a token |
| `FRONTA_SERVER_ALLOWED_HOSTS` / `FRONTA_SERVER_ALLOWED_ORIGINS` | — | Host / Origin values the MCP endpoint accepts besides loopback (comma-separated) |
| `FRONTA_BWRAP_PATH` | `bwrap` | Linux workers containing process tasks only |
| `LOG_LEVEL_OURS` / `LOG_LEVEL_LIBS` | INFO / WARNING | log levels for Fronta and for libraries |

Fronta's connections carry an `application_name` (`fronta-worker`, `fronta-renewal` for the
worker's reserved lease-renewal connection, `fronta-listener`, `fronta-server`, `fronta-sdk`,
`fronta-feed`, and the lazy `fronta-hints` connection), so
`pg_stat_activity` tells them apart. Every pool hands out autocommit connections: a single
statement is one round trip. Claims and completions use a single autocommit statement;
operations needing several statements open an explicit transaction. Fronta-owned connections pin
`default_transaction_isolation=read committed`, including when the DSN or database default differs.
Caller-owned connections keep their settings. Durations must be finite; `FRONTA_DSN` is required only where Fronta opens its own connections (a worker, the
server, the SDK pool, event subscriptions).

Retry policy per task type: `max_attempts` (3), `attempt_timeout` (1 h), `backoff`
(`Backoff(base_s=1, factor=2, cap_s=3600)`, jittered to `[d/2, d]`), `max_concurrency`,
`max_concurrency_per_key`. Each task snapshots the retry policy at enqueue; concurrency limits are
the values last published by a worker and are enforced exactly.
Claims hold tuple locks on `task_types`, so direct SQL updates and publication both wait for
in-flight claims. Existing valid leases are not revoked when a limit is lowered.

Enqueue uses one durable statement, with a fresh-snapshot lookup after a concurrent dedupe
conflict and a retry if the conflicting row already finished. A caller-owned transaction keeps
its commit/rollback boundary and a transactional wake NOTIFY. This includes an explicit
`conn.transaction()` on an otherwise autocommit connection. The PostgreSQL notification commit
lock can serialize these transactions; batch several enqueues per transaction to amortize it.

## Event feed

`subscribe(name, *, states=("succeeded", "failed", "cancelled"), types=None, settings=None,
batch_size=256, backfill=None)` registers or updates a named subscription and opens one dedicated
connection.
`types=None` matches every type; an empty filter matches none. Batch size is 1–1,000 and is a
consumer call argument, not a worker setting. Filters apply to subsequent transition statements;
existing backlog keeps its original filters. Reusing a name updates filters, so competing consumers
should specify the same filters. Connection failures surface to the caller; re-enter `subscribe`
with the same name to resume.

Each batch exposes `events` and `conn`. `await batch.ack()` deletes its rows and commits any
reaction written on that connection. Advancing the iterator or leaving the context without ack
rolls back; a stale batch cannot be acknowledged. Keep batch handling short and do not commit or
roll back `batch.conn` yourself. Consumer crashes or ambiguous commit responses may cause
redelivery, while database reactions sharing the acknowledgement transaction remain atomic.

Events record `seq`, task `id`, `type`, `state`, `attempt`; they cover enqueue, claim, outcomes,
retry/release, queued cancel, cancellation acknowledgement, reap, and manual requeue. Heartbeats,
progress, dedupe hits, rejected writes, running cancellation requests and rolled-back transitions
produce no event. A transition inserts one row per matching subscription in the same statement.
With no subscriptions there are no event inserts.

Pulls use `ORDER BY seq FOR UPDATE SKIP LOCKED`. Sequence numbers precede commit; ordering is
only among currently visible and unlocked rows, even with one consumer. No cursor skips a late
commit. With backfill disabled, statements that read subscriptions before registration are not
replayed. Delivery is at least once until acknowledgement or retention expiry. For live-only
subscriptions, use `(subscription, seq)` to dedupe external reactions: manual requeue means
`(id, attempt, state)` is not always unique. Backfill duplicates need the handling below.
`get_task(id, conn=None)` returns the latest row, not an event snapshot, and raises `TaskNotFound`
after purge.

Closing a subscription consumer preserves its backlog. `unsubscribe(name)` removes registration
and backlog transactionally. Publishers and deliveries share-lock the matching registration;
removal waits for those transactions without locking unrelated subscriptions. A busy subscription
or large backlog may exceed `statement_timeout_s`; retry after its consumers finish their batches.
An iterator ends if a pull observes that its registration was removed. Workers purge expired
unacknowledged events in bounded batches and log counts per subscription. Monitor backlog and
oldest age and `backfill_pending` through `stats()` to stay inside `retention_s`.
Use read committed isolation for caller-owned enqueue connections when subsequent statements
must observe filter updates or unsubscribe; repeatable-read snapshots can retain the old filters.

### Backfill at registration

`backfill` is opt-in and accepts these values:

| Value | Matching rows projected into the new subscription |
|---|---|
| `None` or `False` (default) | None; ordinary registration does not wait for earlier transactions |
| `True` | Every retained matching task |
| Nonnegative finite seconds or `timedelta` | Terminal rows with `finished_at >= database time - window`, plus every requested queued/running row |

Zero seconds enables a zero-length window; it does not disable backfill. Negative durations,
NaN, infinity and unsupported argument types raise `ValueError` before connecting. `states`
and `types` apply as usual, including empty filters matching nothing.

Only a creating call starts a backfill. Reusing a name preserves its backlog and updates its
filters without replaying history, even with `backfill=True`. To deliberately backfill a different
filter, unsubscribe and create again. A persisted pending backfill is always resumed, including
by calls that omit `backfill`; its original absolute time boundary and remaining states are
preserved. Type-filter updates apply to the remaining chunks. Register consumers at deploy time
and never unsubscribe as part of a restart.

Registration upserts the filters and installs a marker only for a new backfilling name. After
that statement commits, the consumer captures the virtual transaction IDs currently owned by
other connections to this database. It polls that fixed set in `pg_locks` every 250 ms and starts
chunks only after all those transactions end. A transition that read the old subscription list
must finish before the scan; later transactions see the new registration. This also covers
ordinary repeatable-read caller transactions. The barrier holds no registration locks between
polls, so unrelated transitions, reactions and feed pulls continue throughout the wait.

Virtual transaction locks are the synchronization primitive, including for read-only and idle
transactions. `pg_stat_activity` supplies only public PID/database metadata for database scoping
and optional diagnostic details; correctness does not depend on activity tracking, timestamps,
or monitoring grants. The previous exclusive-table-lock design was rejected because an open
reaction batch could make its lock request stall the entire queue. Prepared transactions
(two-phase commit) and imported snapshots are outside the backfill guarantee: PREPARE releases
the virtual lock, and a later transaction can deliberately import an older snapshot.

The wait has no overall timeout: any older transaction in this database can delay consumer
startup, including this process's own transactions and open batches. Finish them before
awaiting a backfilling subscription, or the caller can wait on itself. Every five seconds,
warnings identify remaining virtual IDs, PIDs, users, application names and transaction ages;
hidden or disabled activity details are reported as unavailable. Cancellation or connection
loss leaves the marker for a later consumer, which captures a fresh set before resuming.

Backfill runs in transactions of at most 5,000 rows, one state at a time in alphabetical order
and ascending task-id order. Its registration row lock allows publishers and feed pulls to
continue. Each chunk checks the marker's generation under that lock; an old consumer cannot
use its completed wait for a deleted and recreated name. The captured transaction set is not
persisted. The feed is yielded only after the pending work finishes or its registration is removed.
INFO logs report start/resume, states, time boundary, inserted rows, chunks and elapsed time;
removal/replacement is logged as stopped, and DEBUG logs report each chunk. `stats()` reports
`backfill_pending` while the marker exists, including during the initial wait.

For stable filters, matching task states present after the registration commit are covered by
backfill or live events, except rows purged before backfill reaches them. This projects the
retained task's current state, not its full transition history. Use a window comfortably shorter
than `retention_s` minus consumer lag. Terminal snapshots have the transition's state and attempt;
queued/running snapshots describe the row when scanned. `get_task()` still returns the latest
row. Backfilled events have a fresh `created_at`, so their retention starts at backfill time.
There is no ordering guarantee between live and backfilled events.

A transition during registration or backfill may produce both a live event and a snapshot,
with identical `(id, attempt, state)` and distinct sequence numbers. During this window dedupe
by that tuple or make reactions idempotent per task: `(subscription, seq)` alone does not collapse
these duplicates. Manual requeue can reuse a state/attempt tuple, so a permanent tuple dedupe
is not suitable when every such transition needs a distinct external reaction.

| Failure | Result |
|---|---|
| Any subscription on a schema missing `subscriptions.backfill` | `ConfigurationError` naming `fronta db init`; nothing registered |
| Older transaction stays open | Registration remains pending; only the starting consumer waits, with periodic blocker warnings |
| Connection lost or consumer cancelled | Committed chunks and progress remain; subscribe with the same name to resume |
| Chunk exceeds `statement_timeout_s` | The entire chunk, including its marker update, rolls back; error surfaces; a later call resumes |
| Concurrent creators or resumers | One marker; chunks serialize and re-read progress, without duplicate backfill inserts |
| Unsubscribe during backfill | Waits for the current chunk, deletes registration and events; the loop stops at its next chunk and the first pull ends. An initial transaction barrier already in progress finishes its wait first |
| Name deleted and recreated while an old consumer is paused | Its next chunk stops without touching the replacement marker; the new consumer waits independently |
| Source task purged between chunks | Its snapshot is no longer available |

Backfill adds one event insert per retained match, once per registration. Estimate that cost as
completion rate × retention for `True`, or completion rate × window for a duration, plus any
matching active tasks. A consumer's startup waits for older transactions and then these chunks.
Each barrier poll scans PostgreSQL's lock table; cost depends on the cluster's lock count and
the number of starting consumers. The stress report records poll duration and worker progress.
Backfill does not replace application reconciliation for retention expiry, poison batches or bugs.

### Retention and transaction hygiene

Fronta uses ordinary task and event tables, without partitioning. Workers delete finished tasks
older than `retention_s` in bounded batches; queued and running tasks remain. This bounds retained
history during continuous operation: 3,000 completions/s with 20-minute retention keeps roughly
3.6 million finished tasks. PostgreSQL vacuum then makes deleted space reusable, so database
files can stop growing without shrinking. Sustained-load tests check both cleanup age and whether
storage reaches a stable size.

Keep caller-owned transactions and feed batch blocks short. An open write transaction anywhere
in the same database can prevent PostgreSQL from reclaiming obsolete task and event row versions,
even when it holds no locks on Fronta tables. Claims and feed pulls then have more obsolete
entries to inspect. Retention deletes expired rows; vacuum makes their space reusable after
those transactions finish. Monitor old transactions alongside backlog and autovacuum. A
database operator can use `idle_in_transaction_session_timeout` to limit abandoned transactions;
Fronta does not terminate caller-owned transactions.
[PostgreSQL transaction timeouts](https://www.postgresql.org/docs/18/runtime-config-client.html),
[vacuum and space reuse](https://www.postgresql.org/docs/18/routine-vacuuming.html).

## Operator actions and metadata

- `await fronta.pause(type)` stops future claims after the operation commits; running attempts
  continue. Worker publication preserves `paused`. `resume(type)` clears it and hints workers.
- `await fronta.requeue(id)` moves failed or cancelled tasks to queued, runs them now, resets
  failures and cancellation, and keeps the last error and attempt count. The next claim increments
  attempt. Missing rows raise `TaskNotFound`; other states or an active dedupe-key conflict raise
  `NotRequeueable` (HTTP 409).
- `await fronta.stats()` returns `types` with `type`, `queued_due`, `queued_scheduled`, `running`,
  `oldest_due_age_s`, and `subscriptions` with `name`, `backlog`, `oldest_event_age_s`,
  `backfill_pending` (boolean, true while a backfill is pending). Empty ages
  are null. Counts scan active tasks and unacknowledged events; cost grows with both backlogs.
- `metadata=` accepts any JSON value at enqueue and shares `progress_cap` (64 KiB by default).
  It appears on `TaskRow` and `ctx.metadata`, with no automatic inheritance by child tasks. Over-cap metadata raises `PayloadTooLarge`.

## Database initialization and upgrades

`fronta db init` installs tables, additive columns and versioned claim functions atomically.
It defaults to a five-minute statement deadline, including lock waits; `--timeout SECONDS` or
`FRONTA_STATEMENT_TIMEOUT_S` overrides it. `fronta db sql` prints the same transactional DDL for
administrator review. Workers and servers report an actionable error if initialization is needed.

### Upgrade to 0.6.0 from 0.5.x

Backward compatibility with older Fronta releases or schemas is not supported. Stop old clients,
run `fronta db init`, then start the new release. Initialization is required before using any
0.6.0 client, including ordinary subscriptions and statistics; mixed-version operation is
unsupported. The initializer adds the nullable `subscriptions.backfill` JSONB column idempotently
without changing the schema version or claim functions. Every subscription reports the upgrade
command if the column is missing; statistics reads the column directly.

### Upgrade from 0.4.x

1. Pause producers and gracefully stop old workers, letting running attempts settle.
2. Install the current release and run `fronta db init`. Restart database connections after the column changes.
3. Start the new workers and consumers, then resume producers.
4. Once old workers are gone, `fronta db init --prune` removes obsolete claim functions.

Use this stop/init/restart procedure for the whole fleet. Retained obsolete claim functions are
not a compatibility guarantee; mixed-version operation is unsupported. Init never lowers a newer
schema version.

Replace `subscribe_events()` with `subscribe(name)` and acknowledge each batch. The new feed is
durable and supports competing consumers; use `(subscription, seq)` for external idempotency.
The default poll ceiling changes from five seconds to one. `Settings.claim_lock_timeout_s` is
removed because claims skip busy type rows.

Users of unreleased development checkpoints should also remove `FRONTA_CLAIM_BATCH_SIZE`,
`FRONTA_COMPLETION_BATCH_SIZE`, `FRONTA_COMPLETION_FLUSH_S`, `FRONTA_DEFER_NOTIFICATIONS`,
`FRONTA_NOTIFICATION_FLUSH_S`, `FRONTA_NOTIFICATION_BATCH_SIZE` and `FRONTA_NOTIFICATION_QUEUE_SIZE`.
Batching and hint coalescing are automatic. Incompatible task definitions need the separate
[task-contract rollout](../README.md#changing-a-tasks-contract).

## Guarantees and their limits

- A task is delivered until it reaches a terminal state or exhausts its retry budget. Duplicate
  execution is possible after a lease loss (a stalled worker past its lease may still be running):
  a stale worker's writes are rejected by the execution token, but its side effects are yours.
- A claim never starts with a consumed lease: lease timestamps are taken when the row is written
  (after any lock wait), a claim skips busy type rows, and claimed rows dispatch as soon as the batch
  returns. A row that reaches the worker later than a heartbeat interval renews its lease first (and steps aside when
  the lease is gone). Renewals use a reserved connection and bounded statements. The worker renews due
  attempts every `heartbeat_s / 2`, in chunks of 1,000, and stops an attempt if it cannot confirm
  renewal within its lease. Database outages can still cause lease loss.
- Every attempt belongs to its worker until settled: cancelling `Worker.run()` acts like an
  immediate shutdown (stop, settle or explicitly abandon each attempt, then close the pools), and
  an essential background loop that dies ends the worker in order with exit status 71.
- Concurrency limits bound valid leases, not live processes. An ambiguous claim response triggers
  fenced release of that worker's untracked claims before another claim, without charging a failure.
- With an idle worker and a responsive database, hints wake claims promptly. Polling starts at
  50 ms after activity, doubles on empty results to `poll_interval_s` (default 1 s), and jitters
  by ±20% without exceeding that ceiling. Running cancellation also has the heartbeat fallback.
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

Use [PostgreSQL 18 and the tested configuration](postgresql.md). Claims batch up to free slots,
256 rows and 512 KiB of stored input; one oversized input is still admitted. Stored/compressed
size does not bound decoded Python memory. Completion batches flush immediately when idle and
coalesce while a write is in flight; no collection timer is used. Attempts retain slots and
renewal ownership through settlement. Completion and renewal rows lock in ascending task-ID order.

Wake, cancel and feed hints share a lazy connection per database/event loop. Only hint-only
transactions use asynchronous commit; all task transitions and feed acknowledgements remain
durable. Hints use a five-second statement timeout and retry at 0.1–1 s. Graceful close allows
five seconds to flush; immediate close discards them. Polling and fenced heartbeat replies recover
lost hints. A cancellation hint requests a durable check and cannot cancel a newer attempt by itself.

[Benchmark results](../benchmarks/RESULTS.md) distinguish measured capacity, retention and fault
recovery. [Replay instructions](../benchmarks/README.md) describe workloads and diagnostics.
