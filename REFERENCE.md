# Fronta reference

Details behind the [README](README.md): scope, the task model and its state machine, the
event feed, the sandbox, the server, configuration, database operation, guarantees and the
security model. Pre-1.0, minor versions may change any of it; see the [changelog](CHANGELOG.md).

## Scope and non-goals

- Python SDK (enqueue, task lookup, durable event subscriptions, pause/resume, requeue, stats),
  asyncio and sandboxed-process executors, PostgreSQL queue, `fronta worker`, `fronta server`
  (REST + MCP + dashboard) and `fronta db`.
- Hosts: Linux and macOS for the SDK, the server and asyncio-only workers; Linux for workers
  containing process tasks.
- Backward compatibility with older Fronta releases or schemas is not supported. Stop old
  clients, run `fronta db init`, then start the new release.
- Non-goals: cron; workflows (no DAG engine, dependencies, automatic triggers, durable steps,
  suspend or signals); inline execution; rate limits; priority aging; tenant fairness; task
  expiry; cancellation propagation to children; exactly-once external effects; global ordering;
  non-PostgreSQL backends; non-Linux sandboxing; result streaming; multi-tenancy; Windows.
  Task and event tables are unpartitioned; retention deletes old rows in batches.

## Architecture

- PostgreSQL is the only infrastructure. Fronta lives in the application's database, schema
  `fronta`. Version 18 is the default; 16 and later are supported.
- Rows are the durable truth. Every state change is one statement (or one bounded batch
  statement) with a state precondition; matching feed rows are inserted in the same statement.
  The first committed transition wins; a write whose precondition fails affects 0 rows and the
  writer discards its outcome.
- Claims use `SELECT ... FOR UPDATE SKIP LOCKED`. A running task holds a lease renewed by
  heartbeats. Every claim issues a fresh random execution token; every worker write (heartbeat,
  progress, completion, failure, release, cancel acknowledgement) requires `state = 'running'`
  and the matching token. Advisory locks are not used. Timestamps come from the database clock;
  lease stamps use `clock_timestamp()` when the row is written, after any lock wait, so a claim
  never returns a partly consumed lease.
- LISTEN/NOTIFY is only a wake-up hint; workers also poll, backing off from 50 ms to
  `poll_interval_s` after empty claims with ±20% jitter. Channels: `fronta_wake` (new or
  requeued work, payload = task type), `fronta_cancel` (payload = task id), `fronta_feed`
  (poll the durable outbox). Hints coalesce per database and event loop and are sent after the
  durable commit on a lazy connection whose notify-only transactions use
  `synchronous_commit = off`; a crash can lose hints but never task or feed rows.
- Fronta-owned connections are autocommit and pin `read committed`, independent of the DSN or
  database defaults. A single statement is one round trip; multi-statement operations open an
  explicit transaction. Caller-owned connections keep their settings.
- Routing is by task type only; there are no named queues. Task definitions and executables
  live on workers; PostgreSQL stores JSON inputs, state and results.
- Delivery is at least once: a task is redelivered after lease loss until it reaches a terminal
  state or exhausts its retry budget. Duplicate and overlapping executions are possible (a
  stalled worker past its lease may still be running). Handlers must be idempotent.

## Tasks

### Definitions

- `fronta.task(name, input=Model, output=Model | None, max_attempts, attempt_timeout, backoff,
  max_concurrency, max_concurrency_per_key)` decorates `async def handler(ctx, input) -> output`.
- `fronta.process_task(name, argv, input=Model, sandbox=Sandbox(...), <same policy>)` runs an
  executable ([sandbox](#sandboxed-processes)).
- There is no global registry: `fronta.Worker(tasks=[...], lifespan=asynccontextmanager)` lists
  the accepted task types explicitly. The lifespan yields application resources (pools,
  clients), exposed to handlers as `ctx.state`.
- `fronta.configure(settings)` is optional; defaults come from the environment. `open_pool()` /
  `close_pool()` manage the pool used by `enqueue()` without `conn`, one pool per event loop,
  opened lazily on first use. `FRONTA_DSN` is required only where Fronta opens its own
  connections; `enqueue(..., conn=conn)` needs none.
- On start, a worker checks the installed schema version, then publishes each definition to
  `task_types` (name, executor, JSON schemas, policy, fingerprint, paused) in one transaction.
  Publication preserves the pause flag. The same name with a different fingerprint: last writer
  wins, with a warning. Each task row snapshots its policy (`max_attempts`, `attempt_timeout`,
  `backoff`) at enqueue, so a newer worker never changes the policy of an already queued task.

### Inputs and results

Stored inputs are the model's JSON-mode dump by alias with `round_trip=True`: the shape the
published validation schema describes and the server stores. Workers validate stored inputs in
JSON mode accepting both aliases and field names, so strict `datetime`/`UUID` fields, aliased
fields and `Json[T]` fields survive the queue. `enqueue()` validates the encoded input once more
the way a worker will and raises `InvalidInput` when it would not round-trip (a serializer that
changes the shape, aliases that differ between validation and serialization) instead of failing
the task at claim. Process tasks receive the same JSON on stdin. Pickle is not supported.

Declared outputs serialize using their serialization aliases and custom serializers. Fronta
checks the stored JSON against the published output schema, including when a handler returns a
mutated model instance. Without a declared output, any JSON value is accepted: object, array,
string, finite number, boolean, null.

`InputValidationError` (stored input does not match the model at claim) and
`ResultSerializationError` (non-JSON, schema-invalid, over-cap or unstorable result: NUL
characters and lone surrogates cannot live in JSONB; circular references and nesting beyond 200
levels count as unstorable) fail the task without retry, as does `NonRetryableError` raised by
a handler. NUL and lone surrogates in error metadata and process output are replaced by U+FFFD,
and an exception whose formatting fails yields placeholder metadata, so an outcome is always
recorded. Failures store structured error metadata (type, message, truncated traceback)
separately from the result; only the last attempt's error is kept on the row.

Caps count UTF-8 bytes of the compact JSON encoding: payload and result 1024 KiB; progress,
metadata and error 64 KiB. An over-cap payload is rejected at enqueue (`PayloadTooLarge`),
over-cap `progress()` raises in the task, and error metadata is truncated.

### Enqueue

`await task.enqueue(input, *, conn=None, priority=0, run_at=None, key=None,
concurrency_key=None, metadata=None) -> int` returns the task id (a monotonic bigint).
`priority` is a 32-bit integer; names are 1..255 and keys 1..1024 UTF-8 bytes without NUL or
lone surrogates; `run_at` must be timezone-aware. An invalid argument raises `InvalidInput`
(also a `ValueError`) before anything is written; the server answers 422 and MCP a tool error.

With a non-autocommit `conn`, the insert joins the caller's transaction: Fronta never commits,
rolls back or closes it. An explicit `conn.transaction()` on an autocommit connection is also
caller-owned. Otherwise the insert commits as one statement and post-commit hints follow.
Caller-owned enqueue writes transactional wake and feed NOTIFYs, which preserves rollback
semantics but serializes commits on PostgreSQL's notification lock; several enqueues in one
transaction amortize it. Use read committed on caller-owned connections when later statements
must observe filter updates or unsubscribe; repeatable-read snapshots can retain old filters.

`key` deduplicates: while a queued or running task of the same type has the key, `enqueue`
returns its id and never mutates it (a partial unique index enforces this; if the conflicting
row finishes between the conflict and the lookup, the insert is retried). Once that task has
finished, the same key enqueues a new one. It is not permanent idempotency.

`metadata` is optional JSON stored on the row and exposed as `ctx.metadata`; it shares the
progress cap and is not inherited by children.

`ctx.enqueue()` is immediate and independent of the task's outcome, so a retried task enqueues
again; make children idempotent or give them a business-level key. Enqueueing a type the same
worker accepts wakes it directly.

### Handler context

`ctx` carries `task_id`, `attempt`, `state` (lifespan resources), `metadata`, `log` (a logger
adapter prefixed with the correlation fields), `cancelled` (an `asyncio.Event`), `progress(value)`
and `enqueue()`. Handlers must honor `asyncio.CancelledError` and be safe to run twice.

One worker loop runs every `heartbeat_s / 2` and renews due attempts in chunks of 1,000
through a reserved connection. Each renewal statement's end-to-end budget is
`min(statement timeout, (lease - heartbeat) / 2)`, further bounded by the earliest local lease
deadline. A missing fenced row stops its attempt as lost; a returned cancellation flag stops it
as cancelled. No confirmed renewal within `lease_s` stops an attempt as lost;
`heartbeat_s` must be at most `lease_s / 2`.

### Lifecycle

States: `queued -> running -> succeeded | failed | cancelled`. Terminal rows keep result and
error until purged. `attempt` counts claims; `failures` counts attempts that ended failed. A
task retries while `failures < max_attempts`, otherwise it is `failed`. A retry is the same row
back to `queued` with `run_at = now + backoff(failures)`.

| Event | Requires | New state | Effects |
|---|---|---|---|
| enqueue | no queued/running row with the same type + key, else return its id | queued | policy and metadata snapshot; wake after commit |
| claim | queued, `run_at <= now`, accepted/published/unpaused type, limits not saturated | running | attempt+1, new token, lease set |
| heartbeat | token | running | lease extended; returns `cancel_requested_at`; 0 rows: the worker stops the task and discards its outcome |
| progress | token | running | progress stored |
| succeed | token | succeeded | result, `finished_at` |
| fail (exception, non-zero exit, timeout, sandbox setup error) | token | queued if `failures+1 < max_attempts` else failed; cancelled if cancel pending | failures+1, error, `run_at = now + backoff` |
| fail without retry (`NonRetryableError`, `InputValidationError`, `ResultSerializationError`) | token | failed | failures+1, error |
| release (shutdown) | token | queued; cancelled if cancel pending | `run_at = now`, no charge |
| cancel request | queued | cancelled | `finished_at` |
| cancel request | running | running | `cancel_requested_at`; cancel hint after commit |
| cancel acknowledgement | token, cancel pending | cancelled | `finished_at` |
| reap | running, lease expired | cancelled if cancel pending, else as fail | token cleared |
| requeue | failed or cancelled; no active dedupe-key conflict | queued | failures reset, run now, cancel/finish cleared, error and attempt kept; wake hint |
| pause / resume | published type | unchanged | type `paused` set/cleared; resume hints workers |
| purge | terminal, `finished_at < now - retention` | deleted | batched |

Every state-changing row in this table inserts matching feed rows in the same statement;
heartbeat, progress, a running cancellation request, pause/resume and purge do not.

- **Claim order** is best-effort among unlocked eligible rows: priority desc, `run_at` asc, id
  asc. A task whose limit is saturated is skipped and does not block lower-priority tasks; the
  saturated types and keys are computed once per claim.
- **Dispatch:** each claim takes up to the worker's free slots and 256 rows, bounded by 512 KiB
  of stored input (`pg_column_size`); the first candidate is always admitted. A row dispatched
  later than a heartbeat interval after its claim renews its lease first: it does not run when
  the lease is gone, and is released without a charge when the database cannot confirm it.
- **Stopping** a running task (timeout, cancel, shutdown) uses one mechanism: asyncio cancel, or
  SIGTERM to the sandbox's processes, then kill after the grace period. The worker records the
  cause before stopping and decides the outcome from it; handlers see only `CancelledError`. A
  process attempt always ends: after the grace period the sandbox is SIGKILLed and verified dead
  before its slot is freed. An asyncio handler still running after the grace period and the kill
  timeout is fatal: the worker records the attempt, releases its other tasks and exits with
  status 70 for its supervisor to restart it; a watchdog aborts the process (status 70) when the
  event loop stays blocked for a lease.
- **Cancel** sets `cancel_requested_at`. A queued task becomes `cancelled` in the same
  statement. A running task learns of it by NOTIFY and by the heartbeat response, stops, and
  acknowledges. A completion that commits first wins. While a request is pending, retry and
  release turn into `cancelled`; `succeeded` and `failed` stand.
- **Completions** use one fenced batch statement of at most 256 distinct ids: written
  immediately when idle, coalesced while a write is in flight, with no collection timer.
  Attempts keep their slots and renewal ownership until the outcome has a definitive answer;
  connection failures retry idempotently.
- **Ambiguous claim responses** (the connection dropped after the claim may have committed)
  trigger a fenced release of the worker's running rows that it does not track, without charging
  a failure and respecting pending cancellation, before the next claim. Worker ids are unique
  per instance.
- **Crash recovery:** every worker runs a reaper over `state = 'running' AND lease_until < now`
  (up to 100 rows per pass): pending cancel becomes `cancelled`, otherwise a failed attempt
  (retry or `failed`). A stalled worker's later writes fail the token check and it stops its task.
- **Graceful shutdown** (SIGTERM/SIGINT): stop claiming (a claim that lands after the signal is
  released, never started), wait up to the grace period, stop the remaining tasks, release them
  to `queued` without charging a failure. The worker exits only once every fenced transition has
  a definitive answer, so an unreachable database delays the exit rather than losing a recorded
  outcome. A second signal skips the grace period and, after the stop protocol's own timeouts,
  abandons a still-stuck transition to the reaper. Cancelling `Worker.run()` acts the same way.
  An essential background loop (listener, renewals, reaper, purger, watchdog tick) that ends
  with an unexpected error shuts the worker down gracefully with exit status 71.
- **Retention:** every worker purges terminal tasks with `finished_at` older than `retention_s`
  in batches, and unacknowledged feed rows of the same age with a warning per subscription.
  Concurrent purges skip locked rows.

### Retry policy

Per task type: `max_attempts` (default 3, the initial attempt plus two retries),
`attempt_timeout` (1 h, at most 30 days), `backoff` (`Backoff(base_s=1, factor=2, cap_s=3600)`;
retry n waits `min(cap_s, base_s * factor**(n-1))` seconds jittered to `[d/2, d]`; base ≤ cap ≤
30 days, factor in [1, 10]), `max_concurrency` and `max_concurrency_per_key`. Priority defaults
to 0. Each task snapshots the retry policy at enqueue; concurrency limits are the values last
published by a worker and are enforced exactly at claim.

### Concurrency limits

- A limit is `(task type, key)`: key null for the cluster-wide type limit (`max_concurrency`),
  or the enqueue `concurrency_key` for `max_concurrency_per_key`. Keys are scoped per task type;
  a task without a key is subject only to the type limit.
- The published values in `task_types` are authoritative; a worker's own definition only feeds
  the publish (last writer wins), so every worker enforces the same limits and a change takes
  effect at the next claim. Lowering a limit below existing occupancy admits nothing until it
  drops; existing valid leases are not revoked.
- The candidate query reads a bounded global queue head first. If every row in that head is
  eligible it uses the head; otherwise it takes bounded ordered index seeks per eligible type
  and merges candidates globally. The function then locks task rows with `FOR UPDATE SKIP
  LOCKED`. As each type is encountered, it locks its policy row with `FOR SHARE SKIP LOCKED`
  (unlimited types) or `FOR UPDATE SKIP LOCKED` (limited types); a concurrent policy change is
  rechecked after the lock, and a limit introduced meanwhile upgrades the lock without waiting
  or skips the type. Raw SQL updates and publication therefore both coordinate with claims.
- Limited claims serialize per type and recount under a fresh read-committed snapshot,
  including admissions pending in the same batch. Busy or paused types are skipped for the call;
  type locks never wait, so claimers cannot deadlock.
- The limit bounds valid leases, not live processes: after a false lease expiry, a stalled
  worker may still execute until its next token check.

### Operator actions

- `await fronta.pause(type)` stops future claims after the operation commits; running attempts
  continue, and worker publication preserves the flag. `resume(type)` clears it and wakes
  workers.
- `await fronta.requeue(id)` moves a failed or cancelled task back to queued, runs it now, resets
  failures and cancellation, and keeps the last error and attempt count. Missing rows raise
  `TaskNotFound`; other states or an active dedupe-key conflict raise `NotRequeueable`.
- `await fronta.stats()` returns `types` (`type`, `queued_due`, `queued_scheduled`, `running`,
  `oldest_due_age_s`) and `subscriptions` (`name`, `backlog`, `oldest_event_age_s`,
  `backfill_pending`). Empty ages are null. These scan active tasks and unacknowledged events;
  cost grows with both backlogs.
- `await fronta.get_task(id, conn=None)` returns the latest durable row (not an event
  snapshot) and raises `TaskNotFound` after purge.

## Event feed

`subscribe(name, *, states=("succeeded", "failed", "cancelled"), types=None, settings=None,
batch_size=256, backfill=None)` registers or updates a named subscription, completes any pending
backfill, and yields batches from one dedicated connection. `types=None` matches every type; an
empty filter matches none. Batch size is 1–1,000. Filters apply to subsequent transition
statements; existing backlog keeps its original filters. Reusing a name updates filters, so
competing consumers should specify the same filters. Connection failures surface to the caller;
re-enter `subscribe` with the same name to resume.

Each batch exposes `events` (`TaskEvent(seq, id, type, state, attempt)`) and `conn`, and holds
row locks. `await batch.ack()` deletes its rows and commits, including any reaction written on
that connection. Advancing the iterator or leaving the context without ack rolls back for
redelivery; a stale batch cannot be acknowledged. Keep batch handling short and never commit or
roll back `batch.conn` yourself. Consumer crashes or ambiguous commit responses may cause
redelivery, while database reactions sharing the acknowledgement transaction stay atomic.

Events cover enqueue, claim, outcomes, retry/release, queued cancel, cancellation
acknowledgement, reap and manual requeue. Heartbeats, progress, dedupe hits, rejected writes,
running cancellation requests and rolled-back transitions produce none. A transition inserts one
row per matching subscription in the same statement; with no subscriptions there are no inserts.

Pulls use `ORDER BY seq FOR UPDATE SKIP LOCKED`; empty pulls roll back before waiting on
`LISTEN fronta_feed` with the poll ceiling as timeout. Sequence numbers are allocated before
commit, so a lower sequence can arrive later even with one consumer, and delete-on-ack has no
cursor that could skip it. Several consumers may share a subscription; each gets available
unlocked rows. Delivery is at least once until acknowledgement or retention expiry. For
live-only subscriptions, `(subscription, seq)` identifies a delivery; manual requeue means
`(id, attempt, state)` is not always unique.

Closing a consumer preserves its registration and backlog. `unsubscribe(name)` deletes both
transactionally after waiting for matching publishers, deliveries and backfill chunks (they
share-lock the registration row); unrelated subscriptions are not blocked. A busy subscription
or large backlog may exceed `statement_timeout_s`; retry after its consumers finish their
batches. Every consumer retains its registration's permanent generation UUID. Each pull checks it
under the existing registration row lock, held until acknowledgement or rollback. Removal or
replacement ends that iterator permanently, even if the name is reused before its next pull.
Workers purge expired unacknowledged events in bounded batches and log counts per subscription;
monitor backlog, oldest age and `backfill_pending` through `stats()` to stay inside `retention_s`.

### Backfill at registration

Ordinary registration does not replay transitions that happened before it. `backfill` is
opt-in and accepts these values:

| Value | Matching rows projected into the new subscription |
|---|---|
| `None` or `False` (default) | None; ordinary registration does not wait for earlier transactions |
| `True` | Every retained matching task |
| Nonnegative finite seconds or `timedelta` | Terminal rows with `finished_at >= database time - window`, plus every requested queued/running row |

Zero seconds enables a zero-length window; it does not disable backfill. Negative durations,
NaN, infinity and unsupported types raise `ValueError` before connecting. `states` and `types`
apply as usual.

Only a creating call starts a backfill. Reusing a name preserves its backlog and updates its
filters without replaying history, even with `backfill=True`; to backfill a different filter,
unsubscribe and create again. A persisted pending backfill is always resumed, including by calls
that omit `backfill`, with its original absolute time boundary and remaining states; type-filter
updates apply to the remaining chunks. Register consumers at deploy time and never unsubscribe
as part of a restart.

Registration is one upsert that installs a permanent generation UUID for every new name and a
backfill marker (the absolute `since` boundary and per-state cursors) only when requested. Filter
updates and backfill completion preserve the generation. After registration commits, the consumer
captures the virtual transaction IDs currently owned by other connections to this database and
polls that fixed set in `pg_locks` every 250 ms, in autocommit, until all of them end; later
transactions do not extend the wait. Any transition that read the old subscription list has
therefore finished before the scan, and later transactions see the new registration. Virtual
locks exist before snapshots and real XIDs and persist through idle transactions, so ordinary
repeatable-read callers are covered. `pg_stat_activity` supplies only public PID/database
scoping and optional diagnostics; correctness needs no monitoring grants or activity tracking.
Prepared transactions (two-phase commit) and imported snapshots are outside the guarantee.

The wait has no overall timeout: any older transaction in this database delays the consumer's
startup, including this process's own transactions and open batches. Finish them before
awaiting a backfilling subscription. Every five seconds, warnings name the remaining virtual
IDs, PIDs, users, application names and transaction ages where visible. The barrier holds no
registration locks between polls, so unrelated transitions, reactions and feed pulls continue.
Cancellation or connection loss leaves the marker for a later consumer, which captures a fresh
set before resuming. Each wait poll also checks the registration generation without holding a
row lock; deletion or replacement ends the obsolete consumer's wait and yields a closed iterator.

Backfill runs in transactions of at most 5,000 rows, one state at a time in alphabetical order
and ascending task-id order, with `FOR NO KEY UPDATE` on the registration so publishers and
pulls continue. Each chunk checks the permanent generation under that lock before reading the
marker, including when the replacement has no pending backfill. An old consumer therefore cannot
act on a deleted and recreated name. INFO logs report start/resume, states, boundary,
inserted rows, chunks and elapsed time; DEBUG logs report each chunk. `stats()` reports
`backfill_pending` while the marker exists, including during the initial wait.

For stable filters, matching task states present after the registration commit are covered by
backfill or live events, except rows purged before backfill reaches them; use a window
comfortably shorter than `retention_s` minus consumer lag. Backfill projects each retained
task's current state, not its full history: terminal snapshots carry the transition's state and
attempt; queued/running snapshots describe the row when scanned. Backfilled events have a fresh
`created_at`, so their retention starts at backfill time, and there is no ordering guarantee
between live and backfilled events. A transition during registration or backfill may produce
both a live event and a snapshot with identical `(id, attempt, state)` and distinct sequence
numbers: during this window dedupe by that tuple or make reactions idempotent per task.

| Failure | Result |
|---|---|
| Any subscription on a schema missing `subscriptions.backfill` or `generation` | `ConfigurationError` naming `fronta db init`; nothing registered |
| Older transaction stays open | Registration remains pending; only the starting consumer waits, with periodic blocker warnings |
| Connection lost or consumer cancelled | Committed chunks and progress remain; subscribe with the same name to resume |
| Chunk exceeds `statement_timeout_s` | The whole chunk, including its marker update, rolls back; the error surfaces; a later call resumes |
| Concurrent creators or resumers | One marker; chunks serialize and re-read progress, without duplicate inserts |
| Unsubscribe during backfill | Waits for the current chunk, deletes registration and events; the loop stops at its next chunk and the first pull ends |
| Name deleted and recreated while an old consumer is paused | Its next barrier poll, chunk or pull ends the old consumer without touching the replacement marker or events |
| Source task purged between chunks | Its snapshot is no longer available |

Backfill adds one event insert per retained match, once per registration: roughly completion
rate × retention for `True`, or completion rate × window for a duration, plus matching active
tasks. Each barrier poll scans PostgreSQL's lock table; cost depends on the cluster's lock count
and the number of starting consumers. Backfill does not replace application reconciliation for
retention expiry, poison batches or bugs.

## Sandboxed processes

- **Trust:** the application chooses the executable; only its JSON input comes from outside,
  and that input may be hostile. The sandbox contains hostile input, not hostile executables: a
  malicious binary can still burn CPU and memory within its rlimits, and the kernel attack
  surface of unprivileged user namespaces is the host's to manage (seccomp/AppArmor policies,
  kernel updates).
- **Contract:** the input model as JSON on stdin; cwd `/work`, a private writable tmpfs;
  `FRONTA_TASK_ID`, `FRONTA_ATTEMPT`, `FRONTA_WORKER_ID` and `FRONTA_SANDBOX_ID` in the
  environment. The result is `{"exit_code", "stdout", "stderr", "truncated"}`; streams are
  drained up to the cap and the rest discarded (`truncated` set); invalid UTF-8 and NUL become
  U+FFFD. Exit code 0 succeeds; anything else is a failed attempt with the same object as error
  metadata. Process tasks cannot signal a non-retryable failure.
- **Boundary:** new user, PID, mount, network and IPC namespaces; nested user namespaces
  disabled. Filesystem: only the read-only binds declared on the definition (default: the
  minimal system paths needed to execute a binary), a size-bounded tmpfs `/tmp` and the tmpfs
  workdir `/work`; nothing is written to the host. Environment cleared except PATH, HOME, LANG,
  `FRONTA_*` and the definition's explicit `env` (which may not set `FRONTA_*`). Only stdio is
  inherited. Network: none (isolated loopback only).
- **Stopping:** SIGTERM to every process of the sandbox, found by its `FRONTA_SANDBOX_ID`
  marker (so `setsid` descendants are reached too) from a thread bounded by the kill timeout,
  then SIGKILL to the sandbox after the grace period. Every signal goes through a pidfd opened
  before the marker is re-verified, so a recycled pid never receives a Fronta signal. Sandboxes
  die with the worker (`--die-with-parent`); one orphaned in bubblewrap's few-millisecond arming
  window is killed by the next worker with process tasks that starts on the host (at start and
  on the reaper interval).
- **Limits per definition:** attempt timeout (wall), CPU time and memory (per-process rlimits,
  best effort), PIDs (`RLIMIT_NPROC` inside the sandbox's user namespace, sandbox-wide), tmpfs
  size (`/work` and `/tmp` each, 256 MiB default), output size per stream (256 KiB default).
  There is no aggregate CPU or memory limit.
- **Backend:** bubblewrap (`bwrap`, packaged and unprivileged) with `prlimit` from util-linux
  applying the rlimits inside the sandbox. Hosts need unprivileged user namespaces; containers
  need explicit seccomp/AppArmor allowances. A worker with process tasks runs a startup probe
  per distinct sandbox configuration and fails closed if the sandbox does not work.

## Server

`fronta server` (needs `fronta[server]`) is one FastAPI application serving REST under
`/api/v1`, MCP (streamable HTTP, official `mcp` SDK) at `/mcp` and a static dashboard at `/`.
It needs only the database.

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

Enqueue validates the input against the published JSON schema and the caps. List returns
summaries without the JSON columns, newest first, keyset-paginated by id (`before`). Errors: 401
missing/invalid token, 404 unknown type or id, 409 not cancellable or not requeueable, 413 over
the cap, 422 invalid input (schema mismatch, a priority outside the 32-bit range, NUL or lone
surrogates, an oversized key, a naive `run_at`). MCP reports the same cases as tool errors.
Request bodies over `payload_cap + progress_cap + 64 KiB` are refused with 413 before parsing.

**Authentication:** `FRONTA_SERVER_TOKEN` is required; the server refuses to start without it,
so a page in a browser on the same host cannot enqueue or cancel tasks with a cross-origin
request. Every REST and MCP request needs `Authorization: Bearer <token>`. The dashboard HTML is
public, asks for the token once and keeps it in the browser's local storage. The server is
single-tenant, binds 127.0.0.1 by default and is meant for a trusted network or a reverse proxy
that terminates TLS.

**Dashboard:** task list with filters, task detail (input, result, error, progress, attempts),
cancel, task types and an enqueue form, hydrated with the vendored Alpine.js over the REST
endpoints. Every control is labelled and keyboard-operable, and the page fits a 375 px viewport.

### Reverse proxy

The MCP transport validates the `Host` (and `Origin`) header against an allowlist to defeat DNS
rebinding; with the default loopback bind only loopback names pass. A proxy that keeps the
public hostname needs it listed:

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

## Configuration

Everything is read from `FRONTA_*` environment variables (see `fronta.Settings`) and validated
at startup. Durations are seconds and must be finite; caps are bytes.

| Variable | Default | Meaning |
|---|---|---|
| `FRONTA_DSN` | — | PostgreSQL connection string; required wherever Fronta opens its own connections |
| `FRONTA_CONCURRENCY` | 10 | attempts a worker runs at once (both executors) |
| `FRONTA_POOL_SIZE` | 4 | ordinary connections per worker/SDK pool; renewals have a reserved connection |
| `FRONTA_CONNECT_TIMEOUT_S` / `FRONTA_STATEMENT_TIMEOUT_S` | 10 / 30 | connection/readiness and statement deadlines |
| `FRONTA_LEASE_S` / `FRONTA_HEARTBEAT_S` | 30 / 10 | lease length and heartbeat interval (at most half the lease) |
| `FRONTA_GRACE_S` | 30 | time a stopped attempt gets to end before it is killed (shutdown, cancel, timeout) |
| `FRONTA_KILL_TIMEOUT_S` | 5 | each hard-stop/verification wait |
| `FRONTA_REAPER_INTERVAL_S` / `FRONTA_POLL_INTERVAL_S` | 15 / 1 | reaper interval and maximum idle poll delay |
| `FRONTA_RETENTION_S` | 604800 | terminal tasks and unacknowledged events expire after 7 days |
| `FRONTA_PURGE_INTERVAL_S` / `FRONTA_PURGE_BATCH` | 600 / 1000 | cleanup interval and maximum rows per deletion batch |
| `FRONTA_PAYLOAD_CAP` / `FRONTA_RESULT_CAP` | 1 MiB | UTF-8 bytes of the JSON encoding |
| `FRONTA_PROGRESS_CAP` / `FRONTA_ERROR_CAP` | 64 KiB | metadata shares the progress cap |
| `FRONTA_LIST_PAGE_SIZE` / `FRONTA_LIST_PAGE_MAX` | 50 / 200 | default and maximum task-list page size |
| `FRONTA_SERVER_HOST` / `FRONTA_SERVER_PORT` / `FRONTA_SERVER_TOKEN` | 127.0.0.1 / 8000 / required | the server never runs without a token |
| `FRONTA_SERVER_ALLOWED_HOSTS` / `FRONTA_SERVER_ALLOWED_ORIGINS` | — | Host / Origin values the MCP endpoint accepts besides loopback (comma-separated) |
| `FRONTA_BWRAP_PATH` | `bwrap` | Linux workers containing process tasks only |
| `LOG_LEVEL_OURS` / `LOG_LEVEL_LIBS` | INFO / WARNING | log levels for Fronta and for libraries |

Fronta's connections carry an `application_name` (`fronta-worker`, `fronta-renewal`,
`fronta-listener`, `fronta-server`, `fronta-sdk`, `fronta-feed`, `fronta-hints`, `fronta-init`),
so `pg_stat_activity` tells them apart. A worker uses up to `pool_size + 3` connections (pool,
listener, renewal, hints); each feed consumer uses one.

## Database

### Initialization and upgrades

`fronta db init` installs tables, additive columns, the versioned claim function and
`fronta.meta.schema_version` atomically and idempotently. The 0.6.0 upgrade leaves queue and event
rows in place and assigns a permanent UUID to every existing subscription; assigning UUIDs can
rewrite the subscription table. It defaults to a five-minute statement deadline including lock
waits; `--timeout SECONDS` or `FRONTA_STATEMENT_TIMEOUT_S` overrides it. `fronta db sql` prints the
same transactional DDL for administrator review. Init never lowers a newer installed version.
Workers and servers refuse to start behind the required version and name the command;
subscriptions require `subscriptions.backfill` and `generation`, and statistics require `backfill`.

Upgrade procedure for every release: pause producers and gracefully stop old workers, letting
running attempts settle; install the new release and run `fronta db init`; start the new
workers and consumers, then resume producers. Mixed-version operation is unsupported. Once old
workers are gone, `fronta db init --prune` drops obsolete claim functions. Release-specific
notes (renamed calls, removed settings) are in the [changelog](CHANGELOG.md).

### Schema notes

`lease_until` is deliberately not indexed and `fronta.tasks` keeps 10% free space per page
(`fillfactor = 90`), so heartbeats are heap-only updates (measured 5000/5000 HOT, a third of the
WAL). `tasks_key_idx (key, id) WHERE key IS NOT NULL` serves listing by key over history. The
events table has an index on `created_at` so expiry checks never scan an unexpired backlog.

### PostgreSQL configuration

Use PostgreSQL 18 for new deployments. Keep task commits durable and run PostgreSQL on storage
with predictable write latency; Fronta does not change server settings. The profile below was
used for the physical Linux sustained-load test and the throughput tests. It is a starting point
for a server with at least 8 GiB available to PostgreSQL:

```ini
shared_buffers = 2GB
max_wal_size = 4GB
backend_flush_after = 256kB
bgwriter_flush_after = 256kB
fsync = on
synchronous_commit = on
full_page_writes = on
```

For a dedicated database host, start `shared_buffers` near 25% of RAM and leave room for the OS
cache, connections and application processes. The flush settings encourage smaller, continuous
writeback; benchmark them on your storage. `max_wal_size` is a checkpoint target, not a disk
limit: monitor free disk space and WAL retention, especially with replication slots. Keep
durability enabled when measuring capacity; turning it off changes the guarantees being tested.
Size `max_connections` for the whole deployment (see [configuration](#configuration)).
[PostgreSQL memory and writeback settings](https://www.postgresql.org/docs/18/runtime-config-resource.html),
[WAL settings](https://www.postgresql.org/docs/18/runtime-config-wal.html).

### Retention and transaction hygiene

Fronta uses ordinary task and event tables, without partitioning. Workers delete finished tasks
older than `retention_s` in bounded batches; queued and running tasks remain. This bounds
retained history during continuous operation: 3,000 completions/s with 20-minute retention keeps
roughly 3.6 million finished tasks, and seven-day retention at the same rate roughly 1.8 billion.
Choose retention and storage for the history you need. PostgreSQL vacuum makes deleted space
reusable, so database files stop growing without shrinking.

Keep caller-owned transactions and feed batch blocks short. An open write transaction anywhere in
the same database can prevent PostgreSQL from reclaiming obsolete task and event row versions,
even when it holds no locks on Fronta tables; claims and feed pulls then have more obsolete
entries to inspect. An operator can use `idle_in_transaction_session_timeout` to limit abandoned
transactions; Fronta does not terminate caller-owned transactions.

Monitor completion rate against arrival rate, queued backlog, oldest retained task/event,
relation size, old transactions, autovacuum and disk waits. Stable throughput with bounded
backlog and reusable storage is the capacity objective. If latency spikes, correlate PostgreSQL
wait events with disk writeback and memory pressure: a WAL flush can wait behind data-file writes
on the same device, and a host that swaps heavily cannot certify performance.
[Transaction timeouts](https://www.postgresql.org/docs/18/runtime-config-client.html),
[vacuum and space reuse](https://www.postgresql.org/docs/18/routine-vacuuming.html).

## Guarantees and their limits

- A task is delivered until it reaches a terminal state or exhausts its retry budget. Duplicate
  execution is possible after a lease loss: a stale worker's writes are rejected by the execution
  token, but its side effects are yours.
- A claim never starts with a consumed lease: lease timestamps are taken when the row is written,
  a claim skips busy type rows, and claimed rows dispatch as soon as the batch returns. A row that
  reaches the worker late renews its lease first and steps aside when the lease is gone. Renewals
  use a reserved connection and bounded statements; a worker stops an attempt it cannot confirm
  within its lease. Database outages can still cause lease loss.
- Every attempt belongs to its worker until settled, including when `Worker.run()` is cancelled
  or a background loop dies.
- Concurrency limits bound valid leases, not live processes.
- With an idle worker and a responsive database, hints wake claims promptly; polling and fenced
  heartbeat replies recover lost hints. A cancellation hint requests a durable check and cannot
  cancel a newer attempt by itself.
- The reaper requeues up to 100 expired leases per pass in every worker: after a whole fleet
  dies, a backlog of `n` expired leases is back in the queue within about `n / (100 × workers)`
  reaper intervals.
- Deterministic bad results (cycles, nesting beyond 200 levels, unstorable text) fail without
  retry; unencodable exception text is sanitized, so the outcome is always recorded.
- Run workers under a supervisor: exit status 70 means a handler ignored cancellation or blocked
  the event loop for a lease, 71 that an essential background loop died.
- On Linux, CPU time and memory rlimits are per process; the PID limit and the tmpfs bounds are
  sandbox-wide.

## Performance

Claims batch up to free slots, 256 rows and 512 KiB of stored input; one oversized input is
still admitted, and stored size does not bound decoded Python memory. Completion batches flush
immediately when idle and coalesce while a write is in flight. Completion and renewal rows lock
in ascending task-id order.

Wake, cancel and feed hints share a lazy connection per database and event loop. Only hint-only
transactions use asynchronous commit; task transitions and feed acknowledgements stay durable.
Hints use a five-second statement timeout and retry at 0.1–1 s; a graceful close allows five
seconds to flush, an immediate close discards them.

[Benchmark results](benchmarks/RESULTS.md) record measured capacity and fault recovery;
[replay instructions](benchmarks/README.md) describe the workloads.

## Security model

- **Database.** Fronta needs a role that can create the `fronta` schema (`fronta db init`) and
  read and write its tables. Task inputs, results, progress and error metadata are stored as JSONB
  in the application's database: treat them like any other application data (encryption at
  rest, backups, access control). Execution tokens never leave the worker and the database, and
  the server never exposes them.
- **Server.** Loopback bind and a mandatory bearer token by default; see [server](#server).
  Request bodies are capped before parsing.
- **Sandbox.** Contains hostile input, not hostile executables; see
  [sandboxed processes](#sandboxed-processes).
- **Handlers.** asyncio handlers run in the worker process with the worker's privileges and
  database access; they are trusted code.

Report vulnerabilities privately to the maintainer listed in `pyproject.toml` rather than in a
public issue.
