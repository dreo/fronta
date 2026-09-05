# Changelog

All notable changes to Fronta are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Pre-1.0, minor versions may change the schema and the
contracts (there is no migration tooling yet: run `fronta db init` on a fresh schema).

## Unreleased

## [0.4.0] - 2026-09-05

### Changed

- Leases: claim and heartbeat timestamps come from `clock_timestamp()` when the row is written,
  claims wait at most half a lease for a type's row and give up the round instead of returning a
  consumed lease, each claimed row starts as soon as its own claim returns, and a row that reaches
  the worker late renews its lease first or steps aside. The schema's default of `now()` for
  `started_at`/`run_at` is unchanged.
- Lease renewals go through one connection per worker reserved for them (`fronta-renewal`), on a
  fixed cadence, each bounded end to end by `Settings.renew_timeout_s`; `heartbeat_s` must be at
  most half of `lease_s`.
- Cancelling `Worker.run()` settles or explicitly abandons every attempt before the pools close;
  an essential background loop that dies shuts the worker down in order with exit status 71.
- Stored inputs are the model's JSON-mode dump by alias with `round_trip=True`, validated in JSON
  mode (aliases and field names) at claim, so strict `datetime`/`UUID`, aliased and `Json[T]`
  fields survive the queue; an input that would not round-trip is refused at enqueue.
- Cyclic, over-deep and otherwise unstorable results, unencodable exception text and exceptions
  with a broken `__str__` all take the normal failure transitions (no retry for deterministic bad
  results, no controller crash).
- `InvalidInput` (also a `ValueError`) is raised for every invalid enqueue argument: priority
  outside the 32-bit range, NUL or lone surrogates in keys or inputs, oversized keys or names,
  a naive `run_at`. REST answers 422, MCP a tool error, and nothing is inserted. `Settings`
  rejects non-finite durations.
- `FRONTA_SERVER_ALLOWED_HOSTS` / `FRONTA_SERVER_ALLOWED_ORIGINS` extend the MCP endpoint's
  DNS-rebinding allowlist for a reverse proxy that keeps the public hostname.
- `Settings.dsn` is optional: only opening Fronta's own connections requires it, so
  `enqueue(..., conn=conn)` works without `FRONTA_DSN`; a worker context enqueues with its own
  worker's caps and deadline (`TaskDefinition.enqueue_with`).
- Graceful sandbox stops walk `/proc` in a thread, bounded by the kill timeout.
- The claim query computes the saturated types and keys once per claim (~4x faster behind a
  saturated backlog, unchanged on ordinary queues).
- Fronta's pools hand out autocommit connections (a single statement is one round trip; atomic
  operations use explicit transactions); a worker publishes its definitions in one transaction.
- Schema: `tasks_lease_idx` is dropped and `fronta.tasks` gets `fillfactor = 90`, so heartbeats
  are heap-only updates (measured 5000/5000 HOT, a third of the WAL); `tasks_key_idx` serves
  listing by key over history (0.3 ms instead of 24 ms over 600k rows). `fronta db init` upgrades
  an existing schema in place.
- Dashboard: one initialization and refresh timer, stale list/detail responses discarded, one
  request per submit, cancel polling scoped to the selected task; labelled controls, keyboard-
  operable rows, and a layout that fits a 375px viewport.

### Added

- `docs/reference.md`: reverse proxy example, the stored input representation, and the procedure
  for rolling out an incompatible task contract (versioned names).

## [0.3.0] - 2026-08-29

### Added

- Public live lifecycle subscriptions (`subscribe_events()` and `TaskEvent`) plus SDK task lookup
  (`get_task()`) for singleton external workflow consumers.

### Fixed

- Enqueueing through an autocommit caller connection now commits the task row and its wake/event
  notifications atomically.

## [0.2.0] - 2026-08-29

### Added

- macOS support for the SDK, asyncio-only workers, server, and development test suite. Sandboxed
  process tasks remain Linux-only.

### Changed

- The dashboard is now a static packaged asset; the server extra no longer depends on Jinja.
- Expensive scale and browser tests run once per CI change instead of on every compatibility leg.

### Fixed

- REST and MCP now handle an explicit zero page limit consistently, and MCP authentication
  failures include the standard bearer challenge header.
- Cancelling a sandbox while bubblewrap is starting now kills every marked descendant, including
  processes created before bubblewrap arms its parent-death protection.

## [0.1.0] - 2026-08-29

First release.

- PostgreSQL 16+ task queue: fenced transitions with execution tokens, `SKIP LOCKED` claims,
  leases with heartbeats, reaper, retries with jittered exponential backoff, dedupe keys,
  priorities, scheduled `run_at`, cancellation, graceful shutdown, retention purge.
- Executors: asyncio handlers (`@fronta.task`) and bubblewrap-sandboxed processes
  (`fronta.process_task`) with per-definition limits.
- Concurrency limits per task type and per concurrency key, enforced exactly at claim time.
- `fronta db init`, `fronta worker module:attr`, and — with `fronta[server]` — `fronta server`:
  REST + MCP (streamable HTTP) + dashboard, mandatory bearer-token auth, body limits,
  JSON-Schema validation.
- Typed configuration from `FRONTA_*` environment variables.
- Python 3.12–3.14, Linux; `fronta[server]` extra keeps the SDK/worker install light.
