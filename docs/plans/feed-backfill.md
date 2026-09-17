# Feed backfill on subscription creation

Status: implemented and validated, 2026-09-17; release unpublished · Target release: 0.6.0 · Owner: maintainer
Scope: SDK event feed. One additive column, no new tables, processes or settings.

## 0. Problem, decision and rationale

A subscription only receives transitions whose statements ran after its registration existed.
Restarts and disconnections are already covered: the registration is a row, the backlog is kept
until acknowledged, and an in-flight batch rolls back for redelivery. The gap is narrower and
happens once per subscription: the first deploy of a consumer, or a re-registration after
`unsubscribe`. A workflow reactor deployed after some of its source tasks finished waits forever
for events that were never written. That defeats the goal that workflows built on the feed
survive any restart or outage inside retention without manual repair.

Two designs were compared: keep per-subscription event rows and add a backfill at creation, or
replace the feed with a snapshot cursor over the task table. The backfill was chosen because it
keeps the cost on subscribers only, leaves the transition path untouched, preserves competing
consumers and per-event failure isolation, and reuses tested code. The comparison and its
reasoning were settled in the design discussion of 2026-09-17 and are summarised here; this
plan implements the backfill.

Implementation clarifications (2026-09-17): backward compatibility with older Fronta releases or
schemas is not supported. Run `fronta db init` before using any 0.6.0 client. All subscriptions
require the new column and report the upgrade command when it is missing; `stats()` reads the
column directly. Registration is one upsert returning the winning marker, followed by a virtual
transaction barrier when backfill is pending. Each marker has a generation ID, checked under
every chunk lock. Duplicates may overlap the full backfill, not just registration. Storage
validation records heap and index bytes separately: ordinary vacuum makes index pages reusable,
but does not shrink them to the original empty-file size. The storage gate checks zero dead tuples
and a stable heap/index data footprint after two explicit `VACUUM (ANALYZE, INDEX_CLEANUP ON)`
cycles. The default AUTO mode may deliberately leave small numbers of dead line pointers;
the benchmark requests index cleanup and records the command, free-space-map allocation and
the original-size comparison separately. No forced rewrite or `VACUUM FULL` is introduced.
Validation results and the exact platform/runner qualifications are recorded under
[Feed backfill](../../benchmarks/RESULTS.md#feed-backfill), with machine-readable capacity reports.
Earlier mixed-version reports are historical; that deployment mode is unsupported and its
backfill rehearsal has been removed. The package version and changelog are prepared for 0.6.0;
merging and publishing the release remain maintainer actions.

The backfill projects the current state of retained task rows into a new subscription, once,
without a gap, in short transactions, and resumes after a crash. Everything else about the feed
stays as released in 0.5.0.

## 1. Decisions

Each decision states what was chosen, the alternative rejected, and why.

- **D1 · Opt-in argument, default off.** `subscribe(..., backfill=None)`. Rejected: default on.
  A default on would surprise 0.5.0 users with a one-time insert proportional to retained rows
  and would change the cost model of a released feature. The reaction runner planned as a
  follow-up can default it on because reactions are idempotent by contract.
- **D2 · Backfill runs only when this call creates the registration, or resumes one still
  pending.** Rejected: backfill on every call. Restarts must never replay history; a re-registered
  name has its backlog intact. Filter changes on an existing name do not backfill either; the
  workaround is `unsubscribe` then `subscribe(backfill=...)`.
- **D3 · Gap-freeness through virtual transaction locks.** After registration commits, capture
  the granted exclusive virtual-XID locks owned by other backends in this database. Wait for
  that fixed set to disappear before scanning. Virtual IDs precede snapshots and permanent
  XIDs and survive idle transactions; this covers ordinary repeatable-read callers too.
  Rejected: the exclusive table lock, which queues every transition/pull behind an open
  reaction batch and still misses callers using older repeatable-read snapshots. Waiting on
  permanent XIDs plus active statements misses a statement that acquires its XID after capture
  and goes idle before commit. Activity timestamps add clock, permission and tracking hazards.
- **D4 · Poll without retaining locks.** Every 250 ms, query the captured virtual IDs through
  `pg_locks` in autocommit. Do not add later transactions to the set. No overall deadline,
  lock timeout, retries or jitter. Any older transaction in this database may delay startup,
  including this process's own batches; it must finish before the caller awaits the new feed.
  Warn every five seconds with virtual ID, PID, user, application name and transaction age.
  Activity details may be hidden: they are diagnostics only. Database scoping uses the public
  PID/datname columns of `pg_stat_activity`; virtual-ID locks have no `pg_locks.database`.
- **D5 · The backfill runs after the registration commits, outside any table lock, in chunks of
  5,000 rows, one short transaction per chunk, before the feed is yielded.** Rejected: one
  transaction under the lock, which would stall all transitions for the whole backfill; or a
  background task, which complicates the iterator and error reporting. A consumer's start is
  delayed by its own one-time backfill, with progress in its log.
- **D6 · Resumable through a marker on the registration row.** `fronta.subscriptions.backfill
  jsonb`, non-null while a backfill is pending, written in the registration transaction and
  advanced by each chunk. Any later `subscribe` with that name finishes a pending backfill,
  whatever its own `backfill` argument. Its generation UUID distinguishes a replacement with the
  same name; every chunk checks it under the row lock. Resumers capture a fresh virtual-ID set
  before their first chunk, so no transaction IDs or synchronization timestamps are persisted.
  Rejected: no marker. A crash in a multi-minute backfill
  would leave a silently incomplete subscription, the exact failure mode this feature removes.
- **D7 · Backfillers serialize per chunk with `FOR NO KEY UPDATE` on the registration row.** That
  lock is compatible with the publishers' and pullers' `FOR KEY SHARE`, so transitions and pulls
  continue, while a second consumer resuming the same backfill waits for the chunk and re-reads
  the progress. `unsubscribe` waits for the chunk in flight and the next chunk finds no row.
- **D8 · Window semantics.** `backfill=True` projects every retained matching row.
  A `timedelta` or seconds value projects terminal rows with `finished_at` inside the window and
  every matching queued or running row, since those describe current state. The window is stored
  as an absolute timestamp in the marker so a resume uses the same boundary.
- **D9 · Duplicates for transitions concurrent with registration are accepted.** A transition
  after registration can write a live event and also leave a row in the backfill set. Excluding
  it would need transaction-id arithmetic on system columns, the complexity of the rejected
  cursor design. Delivery is already at least once; the documentation names the dedupe key.
- **D10 · Required initialization, no compatibility paths.** Run `fronta db init` before any
  0.6.0 client. It adds the column with `ADD COLUMN IF NOT EXISTS`; the schema version and claim
  functions remain unchanged. Every `subscribe` call maps a missing column to `ConfigurationError`
  naming `fronta db init`, regardless of its backfill argument. `stats()` names the column directly.
  Old-schema and mixed-version operation are unsupported; stop old clients before upgrading.
- **D11 · Constants, not settings.** Chunk size, polling interval and warning interval are module
  constants in `feed.py`. Batching is behaviour, not configuration.
- **D12 · Per-state loops.** The backfill iterates the requested states one at a time and chunks
  by ascending id inside each, which `tasks_state_idx (state, id)` serves in order without a
  sort. A single `state = ANY(...)` loop would need a sort over the remaining range per chunk.

## 2. Specification

### 2.1 API

```python
async with fronta.subscribe(
    name,
    *,
    states=(State.SUCCEEDED, State.FAILED, State.CANCELLED),
    types=None,
    settings=None,
    batch_size=256,
    backfill: bool | float | timedelta | None = None,
) as feed: ...
```

- `None` or `False`: no creation-time backfill and no transaction barrier unless resuming.
- `True`: project every retained row matching `states` and `types`.
- `float` seconds or `timedelta`: as `True`, but terminal rows only when
  `finished_at >= now() - window`. Queued and running rows are always included when requested.
- Invalid values (negative, NaN, infinite) raise `ValueError` before any connection is opened,
  matching the `batch_size` check.
- `stats()` gains `backfill_pending: bool` per subscription.

### 2.2 Registration protocol

The consumer connection is in autocommit mode, after `LISTEN fronta_feed`.

1. Upsert the registration with one `INSERT ... ON CONFLICT (name) DO UPDATE ... RETURNING
   backfill`. The insert branch installs a marker only when requested; the conflict branch
   updates only the filters and returns the existing marker. No pre-read, table lock or retry.
2. If the returned marker is null, yield the feed. Otherwise save its generation and, after
   this statement commits, capture the other connections' owned virtual IDs:

```sql
SELECT l.virtualxid, l.pid, a.usename, a.application_name, clock_timestamp() - a.xact_start
FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
WHERE l.locktype = 'virtualxid' AND l.mode = 'ExclusiveLock' AND l.granted
  AND l.pid <> pg_backend_pid() AND a.datname = current_database();
```

3. Poll that fixed set every 250 ms with the additional predicate
   `l.virtualxid = ANY(%(captured)s::text[])`. Each poll is a separate autocommit statement.
   Never filter by activity state, backend type, timestamps or permanent XIDs. Stop waiting
   when the result is empty; warn about remaining owners every five seconds.
4. Run chunks (§2.3), checking the saved generation under each chunk's row lock.

Correctness rests on the lock manager, not activity tracking: a transaction that could have
read subscriptions before registration committed either ended before capture or still owns a
captured lock. Waiting for those owners to end makes their committed rows visible to backfill.
New transactions see registration. A fresh capture after restart or by another consumer is a
safe superset for the same registration. A replacement registration must use its own barrier.

Prepared transactions and imported snapshots are outside this guarantee: PREPARE releases the
virtual lock, and a new transaction can explicitly import an older snapshot. No monitoring
role grant or activity setting is required. This uses the same lock primitive as concurrent
index creation, not PostgreSQL's complete internal index-building algorithm.
See [transaction IDs](https://www.postgresql.org/docs/18/transaction-id.html) and
[`pg_locks`](https://www.postgresql.org/docs/18/view-pg-locks.html).

Marker shape:

```json
{"generation": "<UUID>", "since": "2026-09-17T10:00:00+00:00", "pending": {"succeeded": 0, "failed": 0, "cancelled": 0}}
```

`since` is null for `True`; durations use the database clock at creation. `pending` stores the
last backfilled ID per requested state; exhausted states are removed and the column becomes
null when empty. A new marker always receives a new generation. There is no compatibility
branch for an older marker format.

### 2.3 Backfill loop

Runs after registration, before the feed is yielded, while the marker is non-null:

```sql
BEGIN;
SELECT types, backfill FROM fronta.subscriptions WHERE name = %(name)s FOR NO KEY UPDATE;
-- no row: log registration removed and stop (the first pull ends the iterator)
-- null: another consumer finished it; stop
-- different generation: log registration replaced and stop without changing its marker
-- else pick the alphabetically first state in pending and its after cursor
WITH ins AS (
    INSERT INTO fronta.events (subscription, task_id, type, state, attempt)
    SELECT %(name)s, t.id, t.type, t.state, t.attempt
    FROM fronta.tasks t
    WHERE t.state = %(state)s
      AND (%(types)s::text[] IS NULL OR t.type = ANY(%(types)s::text[]))
      AND t.id > %(after)s
      AND (%(since)s::timestamptz IS NULL OR t.finished_at >= %(since)s)  -- terminal states only
    ORDER BY t.id
    LIMIT 5000
    RETURNING task_id)
SELECT count(*), max(task_id) FROM ins;
UPDATE fronta.subscriptions SET backfill = %(marker)s::jsonb WHERE name = %(name)s;
COMMIT;
```

- Fewer than 5,000 rows inserted exhausts that state; otherwise `after` advances to the
  maximum id. Pass a null `since` for queued and running so the same query includes all ages.
- `types` is read from the registration in the same transaction, so a filter update during a
  backfill applies to the remaining chunks.
- Each chunk is bounded by `statement_timeout_s` like any other statement.
- Log at INFO when a backfill starts or resumes (name, states, window) and when it finishes
  (rows, chunks, seconds); log removal/replacement as stopped, not finished; DEBUG per chunk.

### 2.4 Guarantees a consumer may rely on

- After a creating `subscribe(name, backfill=X)` yields its feed, every task row that matched the
  filters and window at any instant after the registration commit is delivered at least once,
  as a live or a backfilled event. Rows purged before the backfill reached them are the only
  retention exception, so a window shorter than `retention_s` minus consumer lag is the safe
  choice. Prepared transactions and imported snapshots are explicitly outside the guarantee.
- A `subscribe` with an existing name never creates events for past transitions, except to finish
  a pending backfill.
- A transition concurrent with registration can be delivered twice, with distinct sequence
  numbers. Reactions running during a backfill dedupe by `(id, attempt, state)` or are idempotent
  per task; `(subscription, seq)` alone is not sufficient for that window.
- Backfilled events for terminal states equal the transition event. For queued and running they
  are a snapshot at backfill time; `get_task` returns the latest row as always.
- Backfilled events carry `created_at` of the backfill, so their retention clock starts then.
- No ordering between backfilled and live events, as for the feed in general.
- The transaction barrier holds no registration locks between polls; unrelated transitions and
  feed pulls continue while the consumer waits. Normal row conflicts and database resource
  contention still apply. Caller-owned enqueue needs read committed for subsequent statements
  to see filter updates and unsubscribe, independently of creation-time backfill.

### 2.5 Failure behaviour

| Situation | Behaviour |
|---|---|
| Invalid `backfill` value | `ValueError` before connecting |
| Column missing (any subscription) | `ConfigurationError` naming `fronta db init`; nothing registered |
| Older transaction stays open | Registration is pending; consumer waits without stalling unrelated work, with blocker warnings |
| Connection lost mid-backfill | Registration and marker remain; the next `subscribe` with the name resumes from the marker |
| Two consumers create the same name | One registration, one marker; both may run chunks, serialized, no duplicate rows |
| `unsubscribe` during backfill | Waits for the chunk in flight; the loop stops at the next chunk; events are deleted with the registration; the feed ends on first pull |
| Name deleted and recreated | An old consumer stops at its next chunk without changing the new marker; the replacement uses its own barrier |
| Statement timeout in a chunk | The chunk rolls back; error propagates; a later `subscribe` resumes |
| Task purged between chunks | Not backfilled; consistent with retention |

### 2.6 Cost model

One-time inserts equal to the matching retained rows. Estimate as completion rate × retention
for `True`, or rate × window. Each chunk of 5,000 is one short transaction. No changes on
transitions beyond today's per-match insert. During startup, one lock-table scan every 250 ms
per waiting consumer costs work proportional to cluster lock count. Measure query durations
and concurrent worker progress; polling is not assumed free.

### 2.7 Documentation

- `README.md`, event feed: one paragraph on `backfill`, when to use it, and the dedupe key.
- `docs/reference.md`, event feed: replace "Registration does not backfill…" with sections 2.1,
  2.4, 2.5 and 2.6 in reference form; require `fronta db init` before any 0.6.0 client and the
  stop/init/restart upgrade procedure, with no old-schema or mixed-version support.
- `SPEC.md` §3 event feed bullets, §9 tests, §10 defaults (constants).
- `CHANGELOG.md` 0.6.0: Added (backfill, `backfill_pending` in stats), Changed (column).
- Operator guidance: register subscriptions at deploy time with a backfill and never
  `unsubscribe` as part of a restart.

### 2.8 Non-goals

- Backfill on filter change of an existing subscription.
- A reaction runner hosted in the worker, batch row prefetch, poison-batch bisection and the
  fan-in recipe: follow-up plan, see §8.
- REST or MCP exposure of the feed.
- A cursor feed over the task table.
- Any new `FRONTA_*` setting.

## 3. Work plan by phase

### Phase 1 · Schema and store (½ day)

- `schema.sql`: `ALTER TABLE fronta.subscriptions ADD COLUMN IF NOT EXISTS backfill jsonb;`
- `store.py`: `backfill_pending` in `_STATS`; helper statements for registration, chunk insert
  and marker update; map `UndefinedColumn` to `ConfigurationError` at the first read.
- `fronta db sql` output reviewed for the new statement.

### Phase 2 · Feed (1 day)

- `feed.py`: argument validation; marker construction; the single registration upsert and virtual-ID barrier;
  the per-state chunk loop with `FOR NO KEY UPDATE`; logging; yield after completion.
- Constants: `_BACKFILL_CHUNK = 5_000`, `_BACKFILL_POLL_S = 0.25`,
  `_BACKFILL_WARN_S = 5.0`. No overall wait deadline.
- `model.py` and public exports unchanged apart from the new keyword.

### Phase 3 · Tests (1–1½ days)

Section 6, all files. The gap-freeness soak and the two workflow scenarios are mandatory.

### Phase 4 · Documentation and contract (½ day)

Section 2.7.

### Phase 5 · Validation and release (½ day)

- Upgrade validation: subscriptions reject the uninitialized schema with an actionable error;
  stop old clients, run `fronta db init`, then verify retained history and live events using the
  current release.
- Stress run of section 7 recorded in `benchmarks/RESULTS.md`.
- Release 0.6.0.

## 4. Definition of done

- Phases 1–5 merged on `main`; `make checkall` passes on macOS and on the Linux tier.
- Every acceptance criterion in §5 has at least one passing test named in §6.
- `fronta db init` on a fresh database and on a 0.5.0 database both yield the column; the
  upgrade note requires initialization before any 0.6.0 client and excludes mixed-version use.
- No new settings, no new tables, no change to transition statements or claim functions.
- README, reference, SPEC and CHANGELOG updated; the dedupe guidance for the backfill window
  appears in the reference.
- The stress run of §7 is recorded with its numbers and passes its thresholds.
- §0 still states the comparison with the cursor design accurately after implementation.

## 5. Acceptance criteria

Each is verifiable by the tests in §6.

- **AC1** `subscribe` without `backfill` starts no historical work or transaction barrier,
  except when resuming an existing pending marker; registration takes no exclusive table lock.
- **AC2** A creating `subscribe(backfill=True)` delivers one event per retained row matching
  states and types, with the row's state and attempt, and none for non-matching rows.
- **AC3** A window excludes terminal rows finished before it and includes queued and running
  rows regardless of age when those states are requested.
- **AC4** A second `subscribe` with the same name, with any `backfill` value or changed filters,
  creates no events for past transitions and leaves the backlog intact.
- **AC5** Under continuous completions, a creating `subscribe(backfill=True)` misses no task:
  every task terminal at the end of the run appears in the delivered events at least once. Zero
  gaps across repeated runs with randomized timing.
- **AC6** Duplicates, when they occur, have identical `(id, state, attempt)` and distinct `seq`,
  and only for tasks whose transition overlapped registration or backfill.
- **AC7** A backfill larger than one chunk completes; each row is inserted exactly once; the
  marker is null at the end; `stats()` reports `backfill_pending` true during and false after.
- **AC8** A connection loss mid-backfill leaves the registration and marker; the next `subscribe`
  with that name, with or without `backfill`, finishes it without duplicating rows.
- **AC9** Two consumers creating the same new name produce one registration and exactly one set
  of backfilled rows; two consumers resuming the same pending backfill produce no duplicates.
- **AC10** A pre-registration enqueue held open prevents chunks until commit/rollback/disconnect,
  without preventing registration. Commit is backfilled; rollback/disconnect produces no event.
- **AC11** While an open reaction batch holds the barrier, unrelated tasks continue completing
  and another feed continues receiving and acknowledging events. Assert progress, not a brittle
  wall-clock latency comparison.
- **AC12** `unsubscribe` during a backfill leaves no events for that name and the consumer's
  feed ends on its first pull.
- **AC13** Backfilled events survive `purge_events` until `retention_s` after the backfill even
  when the source task finished earlier, and are purged after it.
- **AC14** Against a schema missing the new column, every `subscribe` call raises
  `ConfigurationError` mentioning `fronta db init`, including calls without backfill. After
  initialization, subscriptions and statistics work normally.
- **AC15** Invalid `backfill` values raise `ValueError` before any connection is opened;
  `types=[]` with backfill inserts nothing; `types=None` covers every type.
- **AC16** A fan-in workflow whose consumer starts after its children finished completes with
  `backfill=True` and does not complete without it; a consumer killed mid-backfill and restarted
  completes it with exactly one finalize task.
- **AC17** At the benchmark's sustained completion rate, a `backfill=True` over about one million
  retained rows completes; claim latency and lock-poll cost are recorded; zero gaps; worker
  progress continues throughout a 30-second blocker; the event heap/dead tuples are cleared
  and index data stabilizes after consumption and two ordinary vacuum cycles.
- **AC18** The forced old-snapshot/late-real-XID/idle-transaction race is closed under read
  committed and repeatable read, with activity tracking on/off, cross-role without monitoring
  grants, for both commit and rollback. Same-role ordinary users also need no grant.
- **AC19** Cancellation before capture or during the barrier leaves the marker resumable.
  Two resumers capture independently; transactions started after capture do not extend either wait.
- **AC20** Deleting and recreating a name before the old consumer's first chunk or between chunks
  cannot let the old consumer advance/clear the replacement marker or lose its pending event.
- **AC21** A long wait warns with the blocking session's identity and visible diagnostics;
  missing diagnostics do not weaken the wait. Other databases' transactions are excluded.
- **AC22** When ordinary and backfilling creators race, the first registration determines whether
  history is projected; the losing upsert preserves its marker and only updates filters.

## 6. Test plan

Fixtures from `tests/conftest.py`: `conn`, `dsn`, `settings`, `sdk`, `run_worker`, `wait_until`.
Helpers from `tests/test_feed.py`: `seed`, `claim`, `pull`, `backlog`. New tests go to
`tests/test_feed.py` unless stated. Every test asserts on the events table through `backlog`
or through delivered batches, never on log text alone.

### 6.1 Feed behaviour (`tests/test_feed.py`)

| Test | Setup | Steps | Expected | AC |
|---|---|---|---|---|
| `test_backfill_off_matches_release_semantics` | Seed and finish a task | `subscribe("late")` | Backlog empty; `pg_locks` shows no `ExclusiveLock` on the table during registration (observed from a second connection) | AC1 |
| `test_backfill_true_projects_matching_rows` | Rows in all five states, two types, differing attempts | `subscribe("all", states=all, types=None, backfill=True)` then `subscribe("some", states=[succeeded], types=["sleep"], backfill=True)` | Backlog of "all" equals every row with its state and attempt; "some" equals only succeeded sleep rows | AC2 |
| `test_backfill_window_bounds_terminal_rows` | Terminal rows with `finished_at` set to 3 h and 30 min ago via SQL; queued and running rows created 3 h ago | `subscribe(states=all, backfill=timedelta(hours=1))` | The 30-minute row and every queued and running row present; the 3-hour terminal row absent | AC3 |
| `test_existing_name_never_backfills` | Create with backfill, ack the backlog, finish more tasks | `subscribe` again with `backfill=True`, then with changed filters | No new events for the earlier rows; live events for the later ones only | AC4 |
| `test_backfill_chunks_and_marker_lifecycle` | Monkeypatch chunk to 7; 20 matching rows | `subscribe(backfill=True)`; poll `stats()` and the marker from a second connection during the loop (use a barrier by patching the chunk function) | Three chunks; 20 events exactly once; `backfill_pending` true during, false after; column null | AC7 |
| `test_backfill_resumes_after_connection_loss` | Chunk 7; 20 rows; patch the chunk function to close the connection after chunk one | First `subscribe` raises an operational error | Registration and marker present with `after` of chunk one; second `subscribe()` without backfill resumes; 20 events once; marker null | AC8 |
| `test_concurrent_creators_backfill_once` | 50 rows | `asyncio.gather` of two `subscribe(new, backfill=True)` | One registration; 50 events; both feeds usable | AC9 |
| `test_concurrent_resumers_do_not_duplicate` | Pending marker left by the loss test's technique | Two `subscribe(name)` in parallel | Events once; no serialization error surfaces | AC9 |
| `test_unsubscribe_during_backfill_leaves_no_orphans` | Chunk 7; 20 rows; patch a pause after chunk one | `unsubscribe(name)` during the pause | `unsubscribe` waits for chunk one, then removes rows; events for the name are zero afterwards; the consumer's first pull ends the iterator | AC12 |
| `test_backfilled_events_get_fresh_retention` | Terminal rows finished 2 h ago | `subscribe(backfill=True)`; `purge_events(retention_s=3600)` | Nothing purged; set `created_at` back 2 h via SQL; purge again removes them with the existing warning path | AC13 |
| `test_missing_column_is_actionable` | Drop the column in the test schema | `subscribe` with `None`, `False`, `True`, or `0` | Every call raises `ConfigurationError` mentioning `fronta db init` and registers nothing; after initialization, subscriptions and stats work | AC14 |
| `test_backfill_argument_validation` | None | `backfill=-1`, `nan`, `inf`, `timedelta(-1)` | `ValueError`; no connection opened (assert through `pg_stat_activity` application name count) | AC15 |
| `test_backfill_type_filters` | Rows of two types | `types=[]`, `types=None`, `types=["a"]` | Zero, all, only "a" | AC15 |
| `test_backfill_reads_current_types_between_chunks` | Chunk 7; 20 rows of two types; barrier after chunk one | Update filters through a second `subscribe(name, types=[other])` during the barrier | Remaining chunks honour the new filter | D7, 2.3 |

### 6.1a Transaction barrier (`tests/test_feed_barrier.py`)

| Test | Contract |
|---|---|
| `test_old_snapshot_late_xid_then_idle_transaction` | A trigger/advisory gate forces the actual race; 8 isolation/tracking/outcome cases, ordinary cross-role consumer (AC10, AC18) |
| `test_single_role_read_only_snapshot_then_enqueue` | Ordinary same-role repeatable-read transaction acquires its snapshot before registration, enqueues afterward (AC18) |
| `test_cancelled_barrier_resumes_with_independent_fixed_sets` | Cancel before capture or during wait; two consumers recapture; later writer does not extend either barrier (AC8, AC9, AC19) |
| `test_recreated_name_cannot_use_previous_generations_barrier` | Pause before first chunk or between chunks, recreate while a writer is open, resume old consumer before writer commit (AC20) |
| `test_open_reaction_batch_does_not_stall_workers_or_other_feeds` | Ten tasks complete and their events are acknowledged while the old batch is held (AC11) |
| `test_long_blocker_warning_is_diagnostic` | Visible and hidden/untracked blocker metadata; pending marker until rollback (AC21) |
| `test_other_database_transactions_do_not_delay_backfill` | Maintenance-database transaction is excluded (AC21) |
| `test_disconnected_blocker_releases_barrier` | Disconnect rolls back the old enqueue and releases wait (AC10) |
| `test_racing_plain_and_backfill_creators_keep_winners_marker` | Both orders of ordinary/backfilling upserts, with an uncommitted winning insert (AC22) |

### 6.2 Gap-freeness soak (`tests/test_feed.py`, marked slow; helper in `tests/stress/backfill.py`)

`test_creating_registration_has_no_gap_under_load`

- Setup: two workers running a trivial `sleep` task with concurrency 16; a producer task
  enqueueing at a steady rate for 6 s.
- Step: at a uniformly random moment between 1 s and 4 s, `subscribe("fanin", backfill=True)`,
  drain the feed until the producer stops and the queue empties, acknowledging every batch and
  recording `(id, state, attempt, seq)`.
- Assertion: the set of ids of terminal tasks in the table equals the set of delivered ids;
  every duplicate has identical `(id, state, attempt)` and distinct `seq`; the duplicate count
  is reported. Parametrize over 10 seeds; the slow marker runs 50 on the Linux tier.
- Fails the build on a single missing id.

### 6.3 Workflow scenarios (`tests/test_e2e.py`)

`test_fan_in_completes_when_consumer_starts_late`

- Tasks: `listing` fans out N=8 `subtask` children in one caller-owned transaction with a
  `listing_jobs(listing_id, remaining)` row; `finalize` records completion in `listing_done`.
- Steps: run workers; enqueue one listing; wait until all children succeeded; only then start
  the reactor: `subscribe("fanin", types=["subtask"], backfill=True)`, per event insert into
  `listing_parts` with `ON CONFLICT DO NOTHING`, decrement `remaining` with `RETURNING`, enqueue
  `finalize` on `batch.conn` at zero, ack.
- Expected: exactly one `finalize` task; `listing_done` has one row. Control run with
  `backfill=None`: no `finalize` within 5 s.

`test_fan_in_survives_consumer_restart_mid_backfill`

- Same tasks, N=40, chunk patched to 5.
- Steps: start the reactor with a fault injected after the second chunk (close its connection);
  restart the reactor with the same name and no `backfill` argument.
- Expected: the resumed backfill delivers the remaining children; exactly one `finalize`;
  `remaining` reaches zero once; no negative counter.

### 6.4 CLI and upgrade (`tests/test_cli.py`, `tests/test_rollout.py`)

- `fronta db sql` includes the additive column statement once.
- `fronta db init` twice is idempotent; on a schema created by the 0.5.0 DDL fixture it adds the
  column and reports no version change.
- With workers stopped, initialize the database, restart the current worker and subscribe with
  `backfill=True`. Retained historical completions and new live completions are delivered.

## 7. Validation runs

Run on the Linux tier with the benchmark matrix in `benchmarks/matrix.json`, recorded in
`benchmarks/RESULTS.md` under a "Feed backfill" heading.

1. **Sustained backfill.** Retention tuned so about one million terminal rows are retained at
   the matrix's completion rate. Create `subscribe("bench", backfill=True)` while the load runs.
   Record: backfill duration, chunks, rows per second, claim latency p50 and p99 in the 10 s
   around registration, worst sampled operation. Requirements: zero worker errors; zero gaps
   by the §6.2 method. Latency is reported, not compared against the removed lock timeout.
2. **Cleanup after consumption.** Drain the subscription, then run two explicit ordinary
   `VACUUM (ANALYZE, INDEX_CLEANUP ON)` cycles. Require zero dead tuples and a stable heap/index
   data footprint; report the original-size comparison separately. No `VACUUM FULL` or rewrite.
3. **Transaction barrier under load.** Hold a caller-owned enqueue open for 30 s while creating
   with backfill. Registration must stay pending until it ends, then finish. Every complete
   five-second interval must have worker completions. Record poll count, mean/max query time,
   total query time, and worker latency during the wait. No lock retries or latency floor.

## 8. Follow-ups outside this plan

- Reaction runner hosted in the worker: `fronta.reaction(name, types=, states=)`,
  `Worker(tasks, reactions=[...])`, registration at worker start with `backfill=True` by
  default, ack on return, rollback and redelivery on exception, reconnect with backoff, poison
  batch bisection, per-batch deadline.
- `get_tasks(ids, conn=)` batch lookup, used by the runner to hand out event and row pairs.
- The fan-in and DAG recipe in the reference, backed by the §6.3 tests, including the four traps:
  dedupe keys cover only queued and running children; `ctx.enqueue` is not transactional with
  application bookkeeping; a requeued child yields two terminal events; retention purges both
  events and rows.
- A test helper that drains a subscription until its backlog is empty.
