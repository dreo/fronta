# Fronta: consolidated remediation plan

Reviewed 2026-09-05 against the current V1 capabilities in [SPEC.md](../SPEC.md).

The first priority is preventing avoidable failed or overlapping attempts: correct lease timing,
own worker cleanup, preserve valid inputs through serialization, and keep heartbeat renewal
independent of ordinary database contention. Then fix the existing API and dashboard behaviors.
Database optimizations should preserve the current fencing and transaction model and earn their
cost in comparative measurements.

This list contains 16 scoped items. **P1** means correctness or liveness first; **P2** means a
bounded improvement to an existing capability. **Measure first** means the problem mechanism is
identified, but the implementation should be adopted only if representative tests justify its
cost. A negative benchmark result can close such an item without a code change.

Evidence: the primary review passed the static checks and 201 functional/browser tests, with 27
platform-related skips, and ran additional targeted probes. Its Linux sandbox and stress tiers
were not exercised. Database performance numbers below were reported by the second review on
temporary PostgreSQL 16; they were not independently repeated. Source inspection confirmed the
relevant code paths.

## Outcome (2026-09-05)

All 16 items are implemented on this branch; the measure-first items were re-measured on a
throwaway PostgreSQL 16 (Docker) before the decision. Validation: `make checkall` on macOS
(static gate, 257 tests passed including the scale and browser tiers, 28 platform skips,
audit clean); the Linux tier (real bubblewrap sandboxes, `tests/test_sandbox.py`,
`test_cancellation.py`, `test_shutdown.py`, `test_e2e.py`, `test_platform.py`,
`test_executors.py`) in a privileged Debian container: 52 passed, 1 skipped, and 3 failures
that are properties of that container, not of the code: the `RLIMIT_NPROC` test cannot fail
a fork for the container's root (`INIT_USER` bypasses the limit; CI runs as a non-root user),
and two thread-count assertions saw the executor thread that psycopg uses to resolve the
hostname DSN, which an IP-literal DSN removes (rerun of both modules: 21 passed). Regression tests live next to the existing
suites: `tests/test_dispatch.py` (1), `tests/test_cancellation.py` (2), `tests/test_inputs.py` (3),
`tests/test_lifecycle.py` (4), `tests/test_heartbeat.py` (5), `tests/test_enqueue.py`,
`tests/test_server.py`, `tests/test_definitions.py` (6, 7, 8, 15), `tests/test_rollout.py` (9),
`tests/test_dashboard.py` (10, 11), `tests/test_executors.py` (12), `tests/test_scale.py` (13),
`tests/test_schema.py` (14, 16), `tests/test_runtime.py` (15).

| Item | Outcome |
|---|---|
| 1 | `clock_timestamp()` for claim and heartbeat leases (the heartbeat locks its row in a CTE first, since an UPDATE evaluates new values before waiting for a row lock); claims bound lock waits to half a lease (`set_config('lock_timeout')`) and give up the round; each claim dispatches its own row; a row dispatched later than a heartbeat interval renews first or steps aside (`Attempt._fresh_lease`). |
| 2 | `Worker._serve` settles attempts on every exit path (cancellation acts as an immediate shutdown), `_abandon_leftovers` runs before the pools close, and `_loop_ended` turns a dead background loop into an orderly shutdown with exit 71. |
| 3 | Stored inputs are `model_dump(mode="json", by_alias=True, round_trip=True)`; workers validate with `model_validate_json(..., by_alias=True, by_name=True)`; `encode_input` proves the round trip and raises `InvalidInput` otherwise. |
| 4 | The codec detects cycles and nesting beyond 200 levels without recursion, rejects lone surrogates, sanitizes them in error metadata, and `error_metadata` survives a broken `__str__`; `encode_result` maps `RecursionError`; the controller degrades unstorable metadata to the type instead of crashing. |
| 5 | A reserved renewal connection per worker (`fronta-renewal`, statement timeout = renewal budget), renewals on a fixed cadence with an end-to-end `asyncio.timeout`; `heartbeat_s <= lease_s / 2` is validated. Measured: with the ordinary pool saturated for three leases nothing is reaped; batching was not needed (a renewal costs ~0.5 ms). |
| 6 | `InvalidInput` for priority range, NUL / lone surrogates, byte lengths, naive `run_at` in the shared checks; `psycopg.DataError` mapped to 422 as a backstop; non-finite durations rejected. |
| 7 | `FRONTA_SERVER_ALLOWED_HOSTS` / `_ORIGINS` feed `TransportSecuritySettings` (loopback always allowed); documented with an nginx example. |
| 8 | `Settings.dsn` optional, required by `runtime.dsn_of()` only where connections are opened; `TaskDefinition.enqueue_with(settings, ...)` carries the worker's caps into `ctx.enqueue()`. |
| 9 | README and reference document the versioned-name procedure; `tests/test_rollout.py` demonstrates routing, draining and the old worker's restart. |
| 10 | No `x-init`, one timer, generation counters for list and detail responses, in-flight cursor guard, submit guard, cancel polling scoped to the selection. |
| 11 | Row open button with an accessible name, labels for every control, wrapping header, tables in scroll containers, 375px layout verified in Chromium. |
| 12 | `ProcessExecution.stop()` runs `terminate()` in a thread under `asyncio.timeout(kill_timeout_s)`; `_abort` offloads its scan the same way. |
| 13 | Measured on 50k queued rows of a saturated type: 120 ms → 28 ms; 30 saturated keys: 73 → 19 ms; unique keys, future rows, rare types and ordinary queues within ±1 ms. A `NOT EXISTS` formulation regressed the unique-key case to 59 ms (anti-join), so the adopted query uses materialized CTEs with `NOT IN` hashed subplans. |
| 14 | Measured with full pages: shipped 0/5000 HOT updates, 2.9 MiB WAL; index dropped 4990/5000, 0.9 MiB; dropped + fillfactor 90 5000/5000, 0.9 MiB, no table growth; reaper 2.2 → 3.2 ms at 200 running rows and 2.4 → 3.4 ms at 2000. Adopted: drop the index, `fillfactor = 90` (no rewrite; `db init` applies it in place). |
| 15 | Pools use `autocommit=True` (heartbeat 3 → 1 statement); `_start_checks` publishes in one explicit transaction; `open_pool()` documents the contract. NOTIFY folding was not pursued. |
| 16 | Measured over 600k terminal rows: 24 ms sequential scans → 0.3 ms with `tasks_key_idx (key, id) WHERE key IS NOT NULL` (24 MiB, built in 0.9 s); no measurable write cost. Adopted. |

1. **P1 — Claims can start with an expired lease.**

   - **What and why:** `_START` and `_HEARTBEAT` use transaction-start `now()`. A claim blocked on a
     lock can consume its lease before returning; this was reproduced. The claim loop also waits
     for every concurrent claim before starting any returned attempt. Both delays can cause an
     avoidable reap and overlapping execution.
   - **Fix:** Set lease timestamps from `clock_timestamp()` after the relevant lock is acquired.
     Dispatch each committed claim promptly instead of waiting for its slower siblings. Bound
     claim waits and recheck ownership/renew before dispatch if a returned claim has been delayed.
     Fresh SQL timestamps alone cannot protect against an arbitrarily delayed commit or dispatch.
   - **Validate:** Hold the type lock longer than a short test lease; the eventual claim must have
     a fresh lease or be rejected/released without executing. Delay one of several claims and
     confirm another starts and heartbeats promptly. Run a concurrent reaper during both tests.
   - **Check side effects:** Preserve priority among eligible rows, concurrency limits, token
     fencing, database-clock scheduling, and release of claims arriving after shutdown. Do not
     change every use of `now()` indiscriminately.
   - **Sources:** [store.py](../src/fronta/store.py) (`_START`, `_HEARTBEAT`, `claim`);
     [worker.py](../src/fronta/worker.py) (`_claim_loop`).

2. **P1 — Worker cancellation does not own the lifetime of its attempts.**

   - **What and why:** Cancelling `Worker.run()` was reproduced leaving a handler alive and an
     attempt unsettled after the pool closed. `_serve()` drains only on its normal path.
     Unexpected background-loop failure is logged without an orderly worker shutdown.
   - **Fix:** Put attempt shutdown and settlement in the worker's cleanup path, before closing
     resources. Retain the existing stop causes and grace/kill protocol. Make unexpected essential
     loop termination trigger orderly failure and a nonzero exit for the existing supervisor.
     This needs explicit task ownership, not a replacement execution framework.
   - **Validate:** Cancel `Worker.run()` with a cooperative handler, during claim, and during a
     pending final write. Inject a background-loop exception. Verify no orphan runner, heartbeat,
     listener, or watchdog remains and each owned row is settled or explicitly abandoned through
     the existing immediate-shutdown path. Exercise process attempts on Linux too.
   - **Check side effects:** Normal signals, repeated cancellation, the second-signal escape,
     database-outage retries, cancellation-resistant handlers, exit status 70, and lifespan cleanup
     must retain their documented behavior. A generic task-group cancellation must not discard a
     pending outcome or kill heartbeats before settlement.
   - **Sources:** [worker.py](../src/fronta/worker.py) (`_run`, `_serve`, `_drain`, `_report_loop_end`).

3. **P1 — Valid Pydantic inputs do not reliably survive the queue round trip.**

   - **What and why:** Valid aliased fields and strict datetime inputs were accepted by SDK
     serialization but rejected by worker validation. The resulting `InputValidationError` is
     terminal, so useful work is lost before its handler runs.
   - **Fix:** Establish a round-trippable stored representation and validate stored values in
     Pydantic's JSON mode. Decide the alias policy explicitly and use round-trip serialization
     where needed; switching only `by_alias` does not cover distinct validation/serialization
     aliases or `Json[T]`. First build a small model matrix to select the minimal consistent
     implementation. If a custom serializer inherently cannot round-trip, reject that unsupported
     representation early with a clear error.
   - **Validate:** Enqueue through SDK and REST/MCP, claim, and compare the handler's typed values
     for aliases, strict datetime/UUID fields, `Json[T]`, nested models, and defaults. Test process
     stdin as well as asyncio handlers. Invalid values must still be rejected.
   - **Check side effects:** Published JSON schemas, byte caps, custom validators/serializers, and
     already queued representations. Test both old and new representations where the chosen fix
     changes the wire format; do not silently weaken external validation.
   - **Sources:** [definitions.py](../src/fronta/definitions.py) (`spec`, `encode_input`);
     [executors.py](../src/fronta/executors.py) (`validate_input`, `ProcessExecution.run`);
     [Pydantic JSON validation](https://docs.pydantic.dev/latest/concepts/json/).

4. **P1 — Result/error encoding can bypass the intended failure transition.**

   - **What and why:** A cyclic result raises `RecursionError` outside the serialization-error
     catch and becomes retryable. An unpaired surrogate in exception metadata raises
     `UnicodeEncodeError` while the controller is preparing its outcome. Both were reproduced;
     they can rerun a handler that has already performed side effects.
   - **Fix:** Convert recursion/cycle/depth failures into `ResultSerializationError`, sanitize
     invalid Unicode in error metadata, and provide a small guaranteed-serializable fallback if
     formatting an exception itself fails. Keep the existing codec instead of adding another
     serialization layer.
   - **Validate:** Return cyclic and excessively deep containers; raise exceptions containing
     surrogates or broken string formatting. Assert the correct terminal/retry policy, bounded
     storable error metadata, no controller crash, and continued processing of the next task.
   - **Check side effects:** Preserve normal retryable exceptions, JSON null/scalars, tuples,
     non-string-key rejection, valid Unicode, NUL handling, and traceback-tail truncation. Do not
     catch cancellation or process-exit exceptions as ordinary serialization failures.
   - **Sources:** [codec.py](../src/fronta/codec.py); [executors.py](../src/fronta/executors.py)
     (`encode_result`); [worker.py](../src/fronta/worker.py) (`_finish_completed`).

5. **P1 — Healthy attempts can miss heartbeats through shared-pool contention.**

   - **What and why:** Each attempt renews separately through the same small pool used by claims,
     progress, completion, reaping, and purging. The heartbeat sleeps between completed calls;
     pool waits and SQL/commit delays extend that interval. The default 30-second statement
     timeout alone can consume the entire 30-second lease. This is distinct from item 1.
   - **Fix/decision:** Reproduce bounded pool contention first. Reserve renewal capacity, using a
     dedicated bounded connection if that is the simplest reliable arrangement, and give renewal
     an end-to-end deadline with lease headroom. If per-attempt calls remain costly at realistic
     concurrency, use bounded worker-level batches fenced by each `(id, token)` pair. Batching is
     conditional on evidence, not a prerequisite for fixing starvation.
   - **Validate:** Occupy ordinary pool connections with controlled queries while another worker
     reaps. Within the supported latency budget, healthy tasks must retain their leases and
     cancellation must arrive promptly. Test outage/reconnect, completed attempts, stale tokens,
     and a locked row within any proposed batch.
   - **Check side effects:** Connection count and deployment limits, shutdown settlement, and
     lock ordering. One blocked row must not stall an unbounded batch; a failed batch must not be
     interpreted as every token being lost. Budget pool acquisition, query, commit, and scheduling,
     rather than validating only `heartbeat_s + statement_timeout_s < lease_s`.
   - **Sources:** [worker.py](../src/fronta/worker.py) (`_heartbeats`, `_claim_loop`);
     [runtime.py](../src/fronta/runtime.py) (`make_pool`); [config.py](../src/fronta/config.py).

6. **P2 — Invalid API arguments escape as internal errors.**

   - **What and why:** Out-of-int32 priorities, NUL-containing keys, and malformed Unicode input
     produced HTTP 500. They are user validation errors. Separately, positive infinity is accepted
     by several duration settings and fails later in runtime/database setup.
   - **Fix:** Enforce database numeric bounds, byte lengths, NUL restrictions, valid text encoding,
     and finite durations in the shared validation paths. REST should return 422 and MCP an input
     tool error. Reject invalid SDK arguments before SQL; preserve the existing distinct payload
     cap exception. Map only known input-related database errors as a backstop.
   - **Validate:** Test the exact valid numeric/UTF-8 boundaries and values immediately outside
     them through SDK, REST, and MCP; include NUL in both keys and malformed text in payloads.
     Test non-finite configuration at startup. No invalid request may insert a row or emit an event.
   - **Check side effects:** Unicode byte-versus-character lengths, 413 for oversized payloads,
     404/409 behavior, and rollback of caller-owned transactions. Do not turn database outages or
     programming errors into misleading 422 responses or redesign the entire exception hierarchy.
   - **Sources:** [store.py](../src/fronta/store.py) (`check_name`, `check_key`, `enqueue`);
     [server/service.py](../src/fronta/server/service.py); [server/api.py](../src/fronta/server/api.py);
     [config.py](../src/fronta/config.py).

7. **P2 — MCP rejects the documented reverse-proxy deployment.**

   - **What and why:** With the default loopback bind, the MCP transport permits loopback hosts
     only. A public Host header was reproduced returning 421 while loopback succeeded, preventing
     MCP clients from using a proxy that preserves the public hostname.
   - **Fix:** Add explicit allowed-host and allowed-origin configuration independent of the bind
     address, pass it to MCP transport security, and document a working proxy example. Keep
     restrictive local defaults and DNS-rebinding protection enabled.
   - **Validate:** Perform the MCP initialize/tool sequence through a proxy-like Host/Origin
     combination and directly over loopback. Unlisted hosts/origins and absent/invalid bearer
     tokens must remain rejected. Test the exact `/mcp` path without redirect dependence.
   - **Check side effects:** REST, dashboard, TLS-origin matching, ports, and IPv6. Changing the
     root fall-through routing solely to change unknown-path 401s to 404s is not needed for this fix.
   - **Source:** [server/app.py](../src/fronta/server/app.py) (`create_app`, `streamable_http_app`).

8. **P2 — Enqueue with a caller connection still requires unrelated global connection settings.**

   - **What and why:** `TaskDefinition.enqueue(conn=...)` resolves global `Settings` before using
     the supplied connection, requiring a DSN it never uses. Worker-context enqueue also obtains
     caps/deadlines through this global path instead of its owning worker's settings.
   - **Fix:** Separate enqueue validation options from connection/pool resolution. Require a DSN
     only when Fronta must acquire a connection; pass the worker's caps/deadline through its
     internal enqueue path. Keep the documented default SDK singleton. Choose the smallest
     internal helper or optional configuration parameter that supports these paths.
   - **Validate:** Enqueue through a supplied connection with no `FRONTA_DSN` and no initialized
     global settings. Exercise default and explicitly configured caps and a worker context whose
     settings differ from the SDK defaults.
   - **Check side effects:** Caller transaction ownership, autocommit insert/event atomicity,
     lazy pool opening, default environment configuration, and cap enforcement. This does not
     require a general multi-client runtime or a silent change to all global configuration APIs.
   - **Sources:** [definitions.py](../src/fronta/definitions.py) (`enqueue`);
     [worker.py](../src/fronta/worker.py) (`TaskContext.enqueue`);
     [runtime.py](../src/fronta/runtime.py) (`get_settings`).

9. **P2 — Breaking task-definition changes need a safe deployment procedure.**

   - **What and why:** Claims route by name; publishing a newer schema does not stop an older
     worker from claiming its input. Such input can fail permanently. Restarting an older worker
     can also restore older published limits. Last-writer-wins is documented, but it is not a safe
     procedure for rolling out incompatible task contracts.
   - **Fix:** Document and demonstrate the existing low-complexity solution: use a new task name
     for an incompatible contract, start its workers, switch producers, and retain old workers
     until all old queued/scheduled/retry work drains. Same-name changes require compatibility
     throughout overlap or a coordinated drained deployment. Clarify local SDK policy snapshots
     versus published server policy and fleet limits.
   - **Validate:** Add an integration example with old/new schemas and versioned names. Confirm
     each worker receives only its supported inputs, old work finishes, and restarting the old
     worker cannot alter the new type's schema or limits.
   - **Check side effects:** Dedupe and concurrency keys are scoped by task name: versioning splits
     their domains and can increase combined concurrency. Document that rollout constraint and
     avoid double-producing business work. No revision registry, automatic compatibility engine,
     or new routing dimension is required.
   - **Sources:** [store.py](../src/fronta/store.py) (`_CANDIDATE`, `_PUBLISH`);
     [definitions.py](../src/fronta/definitions.py); [README.md](../README.md).

10. **P2 — Dashboard request handling produces duplicate work and stale views.**

    - **What and why:** Initialization runs twice, creating four initial API calls and two timers.
      Out-of-order responses were reproduced displaying queued tasks under a failed-state filter.
      Repeated submit calls issue multiple enqueues. These affect what operators see and execute.
    - **Fix:** Remove the explicit `x-init="init()"`; Alpine already calls `init`. Own one refresh
      timer. Discard superseded list/detail responses using generation IDs or cancellation, and
      prevent concurrent submission and duplicate requests for the same pagination cursor. Scope
      cancellation polling to the selected task so it cannot reopen an older selection.
    - **Validate:** In browser tests, count initialization requests/timers; deliberately resolve
      filter/detail requests in reverse order; overlap refresh and pagination; change selection
      during cancellation polling; double-click enqueue while its request is pending. The selected
      view must stay current and only one enqueue may be sent. Verify recovery after errors.
    - **Check side effects:** Manual refresh, auto-refresh, token changes, load-more ordering,
      cancellation feedback, and deliberate subsequent submissions. A UI in-flight flag does not
      provide durable idempotency after an uncertain network result; do not introduce automatic
      retries or silently replace the user's dedupe key.
    - **Source:** [server/static/index.html](../src/fronta/server/static/index.html)
      (`init`, `loadTasks`, `openTask`, `cancelTask`, `submit`);
      [Alpine initialization](https://alpinejs.dev/directives/init).

11. **P2 — Existing dashboard actions are inaccessible by keyboard and overflow small screens.**

    - **What and why:** Opening a task depends on clicking a table row, several form controls lack
      associated labels, and the page measured 429px wide in a 375px viewport. These obstruct
      existing inspect/enqueue/cancel workflows rather than being visual-style preferences.
    - **Fix:** Put a native link/button in the task row, associate labels with controls, let the
      header wrap, and contain wide tables within horizontal scrolling regions. Keep the existing
      lightweight page and visual design.
    - **Validate:** Complete sign-in, filtering, task inspection, enqueue, and cancel with keyboard
      only. Check accessible names/focus and verify viewport containment at 375px and with long
      task names/keys; tables may scroll inside their container. Recheck desktop layout.
    - **Check side effects:** Avoid double actions from bubbling row/button clicks, accidental form
      submission, clipped focused controls, or hiding task fields to make the page fit.
    - **Source:** [server/static/index.html](../src/fronta/server/static/index.html).

12. **P2 — Graceful sandbox stopping performs blocking host-wide filesystem work on the event loop.**

    - **What and why:** `ProcessExecution.stop()` synchronously scans readable `/proc/*/environ`
      files. Its cost grows with unrelated host processes and can delay every handler and heartbeat
      during cancellation. The periodic scavenger already runs in a thread; this stop path does not.
    - **Fix:** Offload the existing discovery/signalling operation and keep it owned by the attempt
      controller with bounded waiting. Preserve marker rechecks, pidfds, and namespace-init hard
      killing. A new discovery algorithm or sandbox backend is unnecessary.
    - **Validate:** Inject a slow scan and verify that the event loop and other attempts' heartbeats
      keep advancing. On Linux, test graceful exit, ignored SIGTERM, `setsid` descendants,
      disappearing processes, startup cancellation, and final verified death.
    - **Check side effects:** A timed-out thread can still run, so its lifetime must not outlive
      required cleanup or race teardown unsafely. Preserve grace/kill deadlines and PID-reuse
      protection. Do not substitute `/proc/.../children` as a complete live-process inventory:
      Linux documents omissions during concurrent exits.
    - **Sources:** [executors.py](../src/fronta/executors.py) (`ProcessExecution.stop`);
      [sandbox.py](../src/fronta/sandbox.py) (`terminate`, `signal_marked`);
      [Linux children-file limitations](https://man7.org/linux/man-pages/man5/proc_tid_children.5.html).

13. **P2 — Saturated high-priority types make claims repeatedly recount the same running tasks.**

    - **What and why:** The candidate query contains correlated running-count subqueries evaluated
      while rejecting queued rows. The second review reported 130ms median and about 300k buffer
      hits with 50k queued rows of a saturated higher-priority type, delaying eligible work.
    - **Fix/decision:** Compare the current plan with counts/saturated groups computed once per
      claim, using a materialized CTE or another plan that demonstrably avoids repeated counts.
      Keep the existing locked recount as the authoritative admission check. Computing counts once
      does not by itself eliminate traversal of rejected queue entries.
    - **Validate:** Use `EXPLAIN (ANALYZE, BUFFERS)` plus repeated claim timings for saturated types,
      saturated keys, mostly unique keys, future high-priority work, rare accepted types, and an
      ordinary unsaturated queue. Confirm materially reduced repeated work without regressing the
      common path, then run concurrent claims and changing-limit tests.
    - **Check side effects:** Priority/run-at/id order, `SKIP LOCKED`, null concurrency keys, type
      scoping, newly enabled limits, and memory used by precomputed groups. Cached counts are only
      an eligibility hint, never a replacement for the locked check.
    - **Source:** [store.py](../src/fronta/store.py) (`_CANDIDATE`, `_within_limits`).

14. **P2, measure first — The lease index amplifies heartbeat writes.**

    - **What and why:** Indexing the column changed by each heartbeat prevents HOT updates. The
      second review reported 0 HOT updates out of 501, versus 500/500 after both removing the lease
      index and lowering fillfactor. This creates index/WAL work and vacuum pressure; dead tuples
      alone do not prove permanent or unbounded bloat.
    - **Fix/decision:** Compare the shipped schema, removal of only the lease index, and removal
      plus a lower fillfactor. Prefer the simple index change if the reaper can efficiently find
      running rows through existing indexes. Tune table vacuum settings only from sustained-load
      measurements. A separate leases table is not justified by the present evidence.
    - **Validate:** Measure HOT ratio, WAL, index/table size, vacuum behavior, and heartbeat latency
      through sustained traffic and cleanup. Measure reaper latency with both typical and high
      running-task counts; rerun claim and retention tests. Record the read/write tradeoff.
    - **Check side effects:** Reaper sorting/scanning after index removal, additional reserved space
      from fillfactor, and the fact that changing fillfactor does not repack existing pages. Avoid
      a blocking table rewrite without demonstrating its necessity and planning deployment.
    - **Sources:** [schema.sql](../src/fronta/schema.sql) (`tasks_lease_idx`);
      [PostgreSQL HOT](https://www.postgresql.org/docs/16/storage-hot.html),
      [vacuuming](https://www.postgresql.org/docs/16/routine-vacuuming.html).

15. **P2 — Single-statement operations pay unnecessary transaction round trips.**

    - **What and why:** Non-autocommit pools wrap a heartbeat in BEGIN/UPDATE/COMMIT; reads and
      progress writes have similar overhead. The second review counted 3 statements per heartbeat
      and 7 per claim. This matters with database latency and multiplies heartbeat contention.
    - **Fix/decision:** Use autocommit for appropriate Fronta-owned operations while keeping
      explicit transactions around every atomic multi-statement operation. Audit all pool users:
      publishing several definitions currently relies on an implicit transaction and needs an
      explicit block if its atomicity is preserved. Start with this small change; folding NOTIFY
      into DML or rewriting claims needs additional measured benefit. Autocommit alone does not
      remove the claim's explicit transaction or its seven statements.
    - **Validate:** Count statements and compare throughput/latency locally and with representative
      database round-trip delay. Re-run commit/rollback, dedupe, events, cancel, recovery, and
      shutdown tests, including supplied autocommit and non-autocommit connections.
    - **Check side effects:** The public `open_pool()` returns a usable pool: changing its connection
      mode can change caller expectations. Preserve or explicitly document that contract, and
      never change a caller-owned connection's mode. Row changes and notifications must remain
      atomic; failed publishing must not leave an unintended partial definition set.
    - **Sources:** [runtime.py](../src/fronta/runtime.py) (`make_pool`, `open_pool`);
      [worker.py](../src/fronta/worker.py) (`_start_checks`);
      [Psycopg transaction management](https://www.psycopg.org/psycopg3/docs/basic/transactions.html).

16. **P2, measure first — Historical key filtering lacks an index matching the existing API.**

    - **What and why:** The active-key index excludes terminal rows and cannot efficiently support
      general historical `key` filtering. The second review observed a sequential scan over 100k
      terminal rows taking 11ms. That latency alone is acceptable; the concern is growth with
      retained history on the existing lookup path.
    - **Fix/decision:** Benchmark realistic retention-sized data with rare and missing keys. Add
      `(key, id DESC) WHERE key IS NOT NULL` only if it materially improves the required latency
      without disproportionate write/storage cost. Retain the current API and dedupe index.
    - **Validate:** Compare query plans and latency for present/absent keys, terminal/active tasks,
      type/state combinations, and deep pagination. Measure enqueue, heartbeat, transition, and
      purge costs with the extra index. If the current plan meets the workload budget and the
      benefit is negligible, close this item without adding the index.
    - **Check side effects:** Additional index maintenance and disk usage, particularly alongside
      item 14. Confirm identical result ordering and cursor behavior; a performance fix must not
      remove the useful key filter or change active-task deduplication.
    - **Sources:** [schema.sql](../src/fronta/schema.sql);
      [store.py](../src/fronta/store.py) (`list_tasks`).

For implementation, add focused regression cases with controlled barriers/fault injection for the
affected behavior, preserving the existing PostgreSQL and Linux sandbox tests. A separate fake
database framework or removal of the current CI stress checks is not required. Run the relevant
checks per change and the repository's full gate on the combined implementation; compare database
optimizations separately before measuring their combined effect.
