# Fronta — specification (V1)

Distributed task processing on PostgreSQL with sandboxed process execution.

## 1. Scope

- V1: Python SDK (enqueue, task lookup, live lifecycle events), asyncio and sandboxed-process executors, PostgreSQL queue, worker CLI, server CLI (REST + MCP + web dashboard), db CLI.
- Supported hosts: Linux and macOS for the SDK, server, and asyncio-only workers; Linux for workers containing process tasks.
- Pre-deployment: breaking changes to schema and contracts are preferred over compatibility paths.
- Non-goals: cron; workflows (no DAG engine, dependencies, or automatic triggers); non-PostgreSQL backends; non-Linux sandboxing; result streaming; multi-tenancy.

## 2. Architecture

- PostgreSQL 16+ is the only infrastructure. Fronta lives in the app's database, schema `fronta`.
- Rows are the durable truth. Every state change is one transaction with a state precondition. The first committed transition wins; a write whose precondition fails affects 0 rows and the writer discards its outcome.
- Claims use `SELECT ... FOR UPDATE SKIP LOCKED`. A running task holds a lease renewed by heartbeats. Every claim issues a fresh random execution token; every worker write (heartbeat, progress, completion, failure, release, cancel ack) requires `state = running` and the matching token. Advisory locks are not used. All timestamps (leases, run_at, finished_at) come from the database clock; lease timestamps (claim, heartbeat) use `clock_timestamp()` at the moment the row is written, after any lock wait, so a claim never returns a partly consumed lease. A claim waits at most half a lease for a type's row (`lock_timeout`) and otherwise returns nothing for that round.
- LISTEN/NOTIFY is only a wake-up hint; workers also poll. Channels: `fronta_wake` (new or requeued work, payload = task type), `fronta_cancel` (payload = task id), `fronta_events` (committed live state transitions for observers, JSON `{id,type,state}`, best-effort with no replay).
- Routing is by task type only; no named queues.
- Delivery: a task is redelivered after lease loss until it reaches a terminal state or exhausts its retry budget. Duplicate and overlapping executions are possible (a stalled worker past its lease may still be running). Handlers must be idempotent.
- Concurrency limits are enforced inside the claim transaction by counting running tasks under a per-type lock (section 5).

## 3. Task model

### Definitions

- `fronta.task(name, input=Model, output=Model | None, max_attempts, attempt_timeout, backoff, max_concurrency, max_concurrency_per_key)` decorates `async def handler(ctx, input) -> output`.
- `fronta.process_task(name, argv, input=Model, sandbox=Sandbox(...), <same policy>)` runs an executable (section 6).
- No global registry. `fronta.Worker(tasks=[...], lifespan=asynccontextmanager)` lists the accepted task types explicitly; the lifespan yields app resources (pools, clients), exposed as `ctx.state`.
- Process-global runtime for the SDK: `fronta.configure(settings)` is optional (defaults come from the environment); `fronta.open_pool()` / `fronta.close_pool()` manage the pool used by `enqueue()` without `conn`, one pool per event loop, opened lazily on first use. `FRONTA_DSN` is required only where Fronta opens its own connections; `enqueue(..., conn=conn)` needs none. A worker context enqueues with its own worker's caps and deadline. Every pool Fronta opens hands out autocommit connections; atomic operations open explicit transactions.
- On start, a worker publishes each definition to `task_types` (name, executor, JSON schemas, policy, fingerprint, updated_at). Same name with a different fingerprint: last writer wins, warning logged. Each task row snapshots its policy (max_attempts, attempt_timeout, backoff) at enqueue, so a newer worker version never changes the policy of an already queued task.

### Payload and result

- Input is a JSON object stored as JSONB: the model's JSON-mode dump by alias with `round_trip=True` (the shape of the published validation schema, which the server stores as given). Workers validate stored inputs in JSON mode accepting aliases and field names. `enqueue` validates the encoded input once more the way a worker will and raises `InvalidInput` when it would not round-trip. Output is any JSON value: object, array, string, finite number, boolean, null. Pickle is not supported.
- `InputValidationError` (input does not match the model at claim) and `ResultSerializationError` (non-JSON, schema-invalid, over-cap, or unstorable result — NUL characters and lone surrogates cannot live in JSONB; circular references and nesting beyond 200 levels count as unstorable) fail the task without retry. `NonRetryableError` raised by a handler does the same. NUL and lone surrogates in error metadata and in process output are replaced by U+FFFD; an exception whose formatting fails yields placeholder metadata, so an outcome is always recorded.
- Failures store structured error metadata (type, message, truncated traceback) separately from the result; V1 keeps only the last attempt's error on the row.
- Caps count UTF-8 bytes of the JSON encoding: payload and result 1024 KiB; progress and error 64 KiB. Over-cap payload is rejected at enqueue (`PayloadTooLarge`), over-cap `progress()` raises in the task, error metadata is truncated.

### Enqueue

- `await task.enqueue(input, *, conn=None, priority=0, run_at=None, key=None, concurrency_key=None) -> int` (task id, bigint identity, monotonic). `priority` is a 32-bit integer; names and keys are 1..255 / 1..1024 UTF-8 bytes without NUL or lone surrogates; `run_at` is timezone-aware. An invalid argument raises `InvalidInput` (a `ValueError`) before anything is written; the server answers 422 and MCP a tool error.
- With a non-autocommit `conn`, the insert joins the caller's transaction; Fronta never commits, rolls back, or closes it. With an autocommit `conn`, Fronta creates one transaction for the insert and notifications. Without `conn`, Fronta's pool creates and commits that transaction.
- Dedupe: `key` is unique per task type among queued/running tasks (partial unique index). A duplicate returns the existing id and never mutates the existing task. If the existing row disappears between conflict and lookup, the insert is retried.
- `ctx.enqueue()` is immediate and independent of the task's outcome; a retried task enqueues again. `key` dedupes only against queued/running tasks, so a retry after the child finished enqueues it again: make children idempotent or give them a business-level key of their own.

### Lifecycle events

- `async with fronta.subscribe_events(settings=None) as events` yields an async iterator of `TaskEvent(id, type, state)` after its dedicated connection is listening. `fronta.get_task(id, conn=None)` returns the latest durable `TaskRow` or raises `TaskNotFound`; it is not a snapshot at the event's transition.
- Every committed task state transition broadcasts once from its transaction: enqueue, claim, retry, success, failure, release, queued cancellation, cancellation acknowledgement, and reaping. A rejected fenced write, rolled-back transaction, dedupe hit, heartbeat, progress update, or running cancellation request emits no state-transition event.
- The event stream is a live best-effort notification feed, not a durable log: no replay, acknowledgement, or silent reconnect. Connection loss surfaces to the consumer. A singleton consumer avoids competing handlers but must still reconnect and reconcile its own durable state after startup, disconnection, or a crash.
- A deterministic child enqueue key prevents duplicates only while that child is queued or running. A workflow needing durable exactly-once handling records a source-event marker in its own table and enqueues the child through the same non-autocommit psycopg connection and transaction.

### ctx

`task_id`, `attempt`, `state` (from the lifespan), `log` (logger with task_id/attempt correlation), `enqueue()`, `cancelled` (asyncio.Event), `progress(value)`. Heartbeats are automatic: a fixed cadence anchored to the start of the last confirmed renewal, through one connection per worker reserved for renewals, each renewal bounded end to end (pool wait, statement, commit) by `min(statement timeout, (lease − heartbeat) / 2)`; `heartbeat_s` is at most half of `lease_s`.

## 4. Lifecycle

- States: `queued → running → succeeded | failed | cancelled`. Terminal rows keep result and error until purged.
- Counters: `attempt` counts claims (monotonic; `ctx.attempt`, `FRONTA_ATTEMPT`); `failures` counts attempts that ended failed. Retry while `failures < max_attempts`, else `failed`. A retry is the same row back to `queued` with `run_at = now + backoff(failures)`.
- Claim order is best-effort among unlocked, eligible rows (queued, `run_at <= now`, accepted and published type, limits not saturated): priority desc, `run_at` asc, id asc. A task whose limit is saturated is skipped and does not block lower-priority tasks; the saturated types and keys are computed once per claim, not per skipped row.
- Dispatch: a claimed row starts its attempt as soon as its own claim returns, never after a slower sibling claim. A row dispatched later than a heartbeat interval after its claim renews its lease first: it does not run when the lease is gone, and is released without a charge when the database cannot confirm it.
- Stopping a running task (timeout, cancel, shutdown) uses one mechanism: asyncio cancel, or SIGTERM to the sandbox's processes, then kill after the grace period. The worker records the cause before stopping and decides the outcome from it (table below); handlers see only `CancelledError`. A process attempt always ends: after the grace period the sandbox is SIGKILLed and verified dead before its slot is freed. Asyncio handlers must honor cancellation: a handler still running after the grace period and the kill timeout is fatal — the worker records the attempt's transition, releases its other tasks and exits with status 70 for its supervisor to restart it; a watchdog aborts the process (status 70) when the event loop stays blocked for a lease.
- Cancel: sets `cancel_requested_at`. A queued task becomes `cancelled` in the same statement. A running task learns of it by NOTIFY and by the heartbeat response, stops, and acks. Completion that commits first wins. While a request is pending, retry and release are replaced by `cancelled`; `succeeded` and `failed` stand.
- Crash recovery: every worker runs a reaper over `state = running and lease_until < now`: pending cancel → `cancelled`, otherwise a failed attempt (retry or `failed`). A stalled worker's later writes fail the token check, and it stops its task.
- Graceful shutdown (SIGTERM/SIGINT): stop claiming (a claim that lands after the signal is released, never started), wait up to the grace period, stop the remaining tasks, release them to `queued` without charging a failure. The worker exits only once every fenced transition has a definitive answer, so an unreachable database delays the exit rather than losing a recorded outcome; a second signal skips the grace period and, after the stop protocol's own timeouts, abandons a still-stuck transition to the reaper.
- Ownership: every attempt belongs to its worker until it is settled. Cancelling `Worker.run()` acts as an immediate shutdown (stop, settle or explicitly abandon each attempt, then close the pools); a repeated cancellation still cancels every attempt task before the pools close. An essential background loop (listener, reaper, purger, watchdog tick) that ends with an unexpected error shuts the worker down gracefully with exit status 71 for its supervisor.
- Retention: every worker purges terminal tasks with `finished_at` older than the retention in batches on an interval. Concurrent purges are harmless.

### Transitions (one transaction each; "token" = `state = running` and the token matches)

| Event | Requires | New state | Effects |
|---|---|---|---|
| enqueue | no queued/running row with the same type + key, else return its id | queued | policy snapshot; NOTIFY wake on commit |
| claim | queued, `run_at <= now`, accepted type, published type, limits not saturated | running | attempt+1, new token, lease set |
| heartbeat | token | running | lease extended; returns `cancel_requested_at`; 0 rows → worker stops the task and discards its outcome |
| progress | token | running | progress stored |
| succeed | token | succeeded | result, `finished_at` |
| fail (exception, non-zero exit, timeout, sandbox setup error) | token | queued if `failures+1 < max_attempts` else failed; cancelled if cancel pending | failures+1, error, `run_at = now + backoff` |
| fail without retry (NonRetryableError, InputValidationError, ResultSerializationError) | token | failed | failures+1, error |
| release (shutdown) | token | queued; cancelled if cancel pending | `run_at = now`, no charge |
| cancel request | queued | cancelled | `finished_at` |
| cancel request | running | running | `cancel_requested_at`; NOTIFY cancel |
| cancel ack | token, cancel pending | cancelled | `finished_at` |
| reap | running, lease expired | cancelled if cancel pending, else as fail | token cleared |
| purge | terminal, `finished_at < now - retention` | deleted | batched |

## 5. Concurrency limits

- A limit is `(task type, key)`: key null for the cluster-wide type limit (`max_concurrency`), or the enqueue `concurrency_key` for `max_concurrency_per_key`. Keys are scoped per task type. A task enqueued without a key is subject only to the type limit.
- The published values in `task_types` are authoritative; a worker's own definition only feeds the publish (last writer wins), so every worker enforces the same limits and a change takes effect at the next claim.
- The candidate query skips tasks whose type or key already has as many running tasks as the limit, so a saturated type never starves others. The claim transaction then locks the task row and the task type's row (claims of one type serialize; nothing else increases the running count), recounts the running tasks of the type and of the key against the current limits, and gives the task up when saturated. Types without limits skip the lock.
- The limit bounds valid leases, not live processes: after a false lease expiry, a stalled worker may still execute until its next token check.

## 6. Sandbox (process tasks)

- Trust: app code chooses the executable; its behavior is not trusted, because its input may be hostile. Enqueue supplies only JSON input, never a command.
- Contract: input model as JSON on stdin; cwd is `/work`, a private writable tmpfs; `FRONTA_TASK_ID`, `FRONTA_ATTEMPT`, `FRONTA_WORKER_ID` and `FRONTA_SANDBOX_ID` in the environment. Result `{"exit_code", "stdout", "stderr", "truncated"}`; streams are drained up to the cap and the rest discarded (`truncated` set); invalid UTF-8 and NUL become U+FFFD; exit code 0 = success, otherwise a failed attempt with the same object as error metadata (process tasks cannot signal a non-retryable failure in V1).
- Boundary: new user, PID, mount, network, and IPC namespaces; nested user namespaces disabled. Filesystem: only allowlisted read-only binds declared on the definition (default: the minimal system paths needed to execute a binary), a private size-bounded tmpfs `/tmp` and the tmpfs workdir `/work`; no other host paths, nothing written to the host. Environment cleared except PATH, HOME, LANG, `FRONTA_*`, and explicit `env` from the definition (which may not set `FRONTA_*`). Only stdio descriptors are inherited. Network: none (isolated loopback only). Stopping sends SIGTERM to every process of the sandbox (found by its `FRONTA_SANDBOX_ID` marker, so `setsid` descendants are reached too) from a thread bounded by the kill timeout (the `/proc` walk never stalls the event loop), then SIGKILL to the sandbox after the grace period; every signal is delivered through a pidfd. The sandbox dies with the worker (`--die-with-parent`); a sandbox orphaned in bubblewrap's few-millisecond arming window is killed by the next live worker on the host (scavenger, at start and on the reaper interval).
- Limits per definition: attempt timeout (wall), CPU time and memory (per-process rlimits, best effort), PIDs (`RLIMIT_NPROC` inside the sandbox's user namespace, sandbox-wide), tmpfs size (`/work` and `/tmp` each), output size per stream. No aggregate CPU or memory limit in V1.
- Backend: bubblewrap (packaged, unprivileged; `prlimit` from util-linux applies the rlimits inside the sandbox). Supported hosts: Linux with unprivileged user namespaces; containers need explicit seccomp/AppArmor allowances. A worker with process tasks runs a startup probe per distinct sandbox configuration and fails closed if the sandbox does not work.

## 7. Entrypoints

- SDK: section 3, including enqueue, task lookup, and live lifecycle events. Configuration via pydantic-settings, prefix `FRONTA_` (`FRONTA_DSN`, timeouts, and limits from section 10), validated at startup.
- `fronta db init`: applies `schema.sql` idempotently. No migration framework in V1.
- `fronta worker module:attr`: `attr` is a `Worker`. Start: publish definitions, sandbox probe, orphan scavenge, LISTEN, claim loop with `concurrency` slots shared by both executors. DB connections serve short transactions only (one LISTEN connection plus a small pool); none is held during execution. Structured logs with task_id/attempt correlation.
- `fronta server`: one FastAPI app serving REST, MCP (official `mcp` SDK, streamable HTTP), and a static dashboard hydrated with Alpine.js over the REST endpoints. Needs only the database.
  - Operations (REST and MCP tools 1:1): `list_task_types`; `enqueue` (type, input, priority, run_at, key, concurrency_key → id; validated against the published JSON schema and caps); `get_task` (row with result, error, progress); `list_tasks` (summaries without the JSON columns; filters type, state, key; keyset pagination by id, newest first); `cancel` (409 when terminal). Request bodies over the payload cap plus 64 KiB are refused with 413 before parsing.
  - Errors: 401 missing/invalid token, 404 unknown type or id, 409 not cancellable, 413 over cap, 422 invalid input.
  - Auth: `FRONTA_SERVER_TOKEN` is required; every REST and MCP request requires `Authorization: Bearer`; the dashboard HTML is public and asks for the token once. Single tenant, loopback/trusted network only; binds 127.0.0.1 by default. The MCP transport's DNS-rebinding allowlist is loopback plus `FRONTA_SERVER_ALLOWED_HOSTS` / `FRONTA_SERVER_ALLOWED_ORIGINS` (a reverse proxy that keeps the public hostname); an unlisted host gets 421.
  - Dashboard: task list with filters, task detail (input, result, error, progress, attempts), cancel, task types, enqueue form. One initialization and one refresh timer; a response superseded by a newer list or detail request is discarded; one request per submit; cancel polling follows the selected task only; every action is keyboard-operable and every control labelled; the page fits a 375px viewport (wide tables scroll inside their container).
  - Schema: `lease_until` is deliberately not indexed and `fronta.tasks` keeps 10% free space per page (`fillfactor = 90`), so heartbeats are heap-only updates; `tasks_key_idx (key, id) WHERE key IS NOT NULL` serves listing by key over history. `fronta db init` brings an older schema in place (drops the old lease index, adds the key index) without a table rewrite.

## 8. Dependencies

`psycopg[binary,pool]>=3`, `psycopg-pool`, `pydantic`, `pydantic-settings`, `click`, `fastapi`, `uvicorn`, `mcp`, `jsonschema`; Alpine.js vendored; bubblewrap and util-linux (`prlimit`) on worker hosts.

## 9. Tests (among others)

- Concurrent claim: 20 workers, 100 jobs, no injected failures → all succeed, no overlapping attempts.
- Crash recovery: a subprocess worker claims a job, SIGKILL → the reaper requeues within lease + reaper interval; `failures = 1`.
- Fencing: SIGSTOP a worker past its lease, let another worker finish the requeued task, SIGCONT → the stale completion is rejected; the stored result is the second worker's; `attempt = 2`.
- Concurrency limits: 20 workers, N per type and per key → never more than N running (measured in the handlers); a SIGKILLed holder's share is free after reaping; a limit shrunk below the running count admits nothing until the count drops.
- Cancellation: queued → cancelled at once; running → stopped within the grace period, cancelled; completed before ack → succeeded.
- Dedupe: 20 concurrent enqueues with one key → one row; after it terminates, a new enqueue creates a new row.
- Lifecycle events: a singleton subscriber can turn a successful source task into one deduplicated follow-up; retry and both cancellation paths produce the expected state sequences; rollback and dedupe emit nothing; leaving the subscription after an early break cannot hang.
- Sandbox: writes outside the workdir and network connections fail; host secrets (`FRONTA_DSN`) are not visible; a process tree that ignores SIGTERM is fully killed after the grace period; SIGKILL of the worker leaves no sandboxed process behind; a slow graceful stop runs off the event loop and is bounded by the kill timeout.
- Leases: a claim delayed by the type lock past most of a lease still returns a full lease; a claim gives up a lock wait past half a lease; a claimed row starts before a slower sibling claim returns; a row returned after its lease was reaped never runs; healthy attempts keep their leases while the ordinary pool is saturated and cancellation still reaches them.
- Ownership: cancelling `Worker.run()` with a cooperative handler releases its row; during a blocked claim, a stuck final write, and twice in a row it leaves no task behind; a failed background loop exits 71 after releasing running attempts.
- Inputs: aliased, strict datetime/UUID, `Json[T]` and nested inputs reach the handler intact through the SDK and REST; a legacy field-name row still validates; a non-round-trippable input is refused at enqueue; cyclic and over-deep results and unencodable exception text keep the normal failure transitions.
- Arguments: priority, key and text bounds through SDK, REST and MCP insert nothing when rejected; a proxied MCP host is accepted only when configured; the dashboard initializes once, discards stale responses, submits once, and works by keyboard at 375px.
- End-to-end: `fronta db init` → worker with one asyncio and one process task → enqueue via SDK, REST, and MCP → state and result via REST and the dashboard → cancel a running task; with a token set, unauthenticated requests get 401.
- Postgres: `FRONTA_TEST_DSN`, GitHub Actions service container.

## 10. Defaults

- Retry: max_attempts 3 (initial + 2 retries); attempt timeout 60 min; backoff for retry n (= failures so far) 1 s × 2ⁿ⁻¹, jittered to `[d/2, d]`, cap 1 h; priority 0. Bounds: base ≤ cap ≤ 30 days, factor in [1, 10], attempt timeout ≤ 30 days.
- Liveness: lease 30 s; heartbeat 10 s (at most half the lease); renewal budget `min(statement timeout, (lease − heartbeat) / 2)` = 10 s; claim lock wait = lease / 2; reaper interval 15 s; poll fallback 5 s; grace period 30 s (shutdown, cancel, timeout); kill timeout 5 s. Durations are finite.
- Worker: concurrency 10; pool size 4 plus one listener and one renewal connection; DB connect timeout 10 s; statement timeout 30 s.
- Retention: 7 days; purge every 10 min in batches of 1000.
- Caps: payload/result 1024 KiB; progress/error 64 KiB; list page 50, max 200; sandbox output 256 KiB per stream; sandbox tmpfs 256 MiB each.
- Server: 127.0.0.1:8000.
