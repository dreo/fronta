# Changelog

All notable changes to Fronta are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/). Pre-1.0, minor versions may change the schema and the
contracts (there is no migration tooling yet: run `fronta db init` on a fresh schema).

## Unreleased

### Added

- macOS support for the SDK, asyncio-only workers, server, and development test suite. Sandboxed
  process tasks remain Linux-only.

### Changed

- The dashboard is now a static packaged asset; the server extra no longer depends on Jinja.
- Expensive scale and browser tests run once per CI change instead of on every compatibility leg.

### Fixed

- REST and MCP now handle an explicit zero page limit consistently, and MCP authentication
  failures include the standard bearer challenge header.

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
