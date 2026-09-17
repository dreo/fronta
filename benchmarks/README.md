# Benchmarks

[Results](RESULTS.md) record measured capacity and fault recovery. These opt-in workloads use real
worker processes and production queue paths. Each run creates a random `fronta_*` database
and drops only that database afterwards; the maintenance role needs `CREATEDB`.

## Run

Use an isolated PostgreSQL 18 instance, without other benchmarks sharing its WAL counters:

```bash
docker run -d --name fronta-bench-pg18 --shm-size=2g \
  -e POSTGRES_USER=fronta -e POSTGRES_PASSWORD=fronta -e POSTGRES_DB=fronta \
  -p 127.0.0.1:55439:5432 postgres:18 \
  -c shared_buffers=2GB -c max_wal_size=4GB -c max_connections=300 \
  -c backend_flush_after=256kB -c bgwriter_flush_after=256kB \
  -c track_io_timing=on -c track_wal_io_timing=on -c track_commit_timestamp=on
export FRONTA_STRESS_DSN=postgresql://fronta:fronta@127.0.0.1:55439/fronta
mkdir -p .scratch/benchmarks
uv run --locked python -m tests.stress --matrix benchmarks/matrix.json \
  --output .scratch/benchmarks/throughput.json --repeats 3
uv run --locked python -m tests.stress.grade .scratch/benchmarks/throughput.json \
  --output .scratch/benchmarks/throughput-grade.json
```

This profile assumes enough memory for the buffers; see
[PostgreSQL configuration](../REFERENCE.md#postgresql-configuration).
Existing output paths are refused. `--cases REGEX`, `--jobs N`, `--label TEXT`, and `--seed N`
control selection and run order. The matrix controls workloads, including worker/concurrency
counts, types, payload sizes, producer counts, progress, limits and retention. It does not change
Fronta's internal batching or durability.

## Measurements

- **Preloaded drain:** startup and warmup precede timing. Due tasks are held behind an unpublished
  type, then admitted by one publication transaction. SQL seeding prevents producer speed from
  limiting the measured worker rate.
- **Live producers:** ordinary SDK enqueue calls run concurrently with workers. Their commits,
  startup and final drain are timed. A terminal subscription adds a consumer that acknowledges
  every completion and checks task identity and attempt.
- **Enqueue/claim exploration:** the harness also supports producer transaction sizes and claim
  distributions. Use `python -m tests.stress --help` and copy/edit the workload matrix.

Every drain checks counts, results, `attempt=1`, `failures=0` and clean worker exits. Reports record
source hashes, versions/settings, rates, operation and feed latency, CPU/RSS, pool waits and WAL
counters. WAL counters are instance-wide; optional `pg_stat_statements` profiling is used when
available. Latency measured from PostgreSQL's commit timestamp includes its durable flush wait.

The three-run throughput grader retains fixed comparison bounds and the compact historical
[payload baseline](payload-baseline.json). It requires `fsync`, `synchronous_commit` and
`full_page_writes` to be on. Results from earlier source revisions remain labelled as such.

## Feed backfill

The feed-backfill validation uses the `feed` matrix's worker/producer/concurrency counts,
one million retained terminal rows and sustained SDK producers at 3,000 tasks/s:

```bash
uv run --locked python -m tests.stress.acceptance backfill --output .scratch/benchmarks/backfill.json
```

The backfill run records chunk rate, claim latency in the ten seconds around registration,
identity reconciliation of acknowledged events, duplicates, worker exits, and worker progress
throughout a 30-second caller transaction while the new consumer waits on virtual transaction
locks: every complete five-second interval must contain completions, and the consumer must stay
pending until the blocker ends, then finish. The report records lock poll count and client query
durations during the wait; these are measured costs, not a claim that polling is free.

The storage gate runs two `VACUUM (ANALYZE, INDEX_CLEANUP ON) fronta.events` cycles after
consumption and requires zero dead tuples and a stable heap/index footprint; heap and index
sizes are reported separately. Ordinary vacuum makes index space reusable but does not shrink
files to their original size, and default `AUTO` index cleanup may leave a few dead line
pointers, which is why cleanup is requested explicitly. `--history`, `--rate`, `--seconds` and
`--matrix` allow smaller diagnostics; only the default million-row run is the capacity check.
Run on an otherwise idle instance and retain the JSON report with the measured source hash.

## Sustained load and faults

```bash
uv run --locked python -m tests.stress.acceptance hint-crash --output .scratch/benchmarks/hints.json
uv run --locked python -m tests.stress.acceptance renewals --seconds 300 --output .scratch/benchmarks/renewals.json
uv run --locked python -m tests.stress.acceptance contention --seconds 600 --output .scratch/benchmarks/contention.json
uv run --locked python -m tests.stress.endurance --mode loss --seconds 600 --output .scratch/benchmarks/loss.json
uv run --locked python -m tests.stress.endurance --mode soak --seconds 10800 --output .scratch/benchmarks/soak.json
```

The three-hour soak targets 3,000 tasks/s with 20-minute retention. An unrelated write transaction
is held from minute 60 to 90. The grader checks exact producer/completion receipts, overall and
20-minute completion rates, backlog, cleanup age and storage stability after recovery. Latency
is diagnostic and has no fixed pass/fail cutoff. A rate deficit during the injected fault is
reported separately from later recovery; an overall average does not hide it.

`--diagnostic` permits short exploratory runs without calling them acceptance passes. For example,
`--mode soak --seconds 600 --diagnostic --hold-at 60 --hold-for 300` checks a shorter transaction
hold. `--history N` preloads retained terminal rows. Reports keep completed per-minute samples
if a run fails. Short runs cannot establish multi-hour storage stability.

Keep experimental output in `.scratch` or outside the checkout. Curated reports in `results/`
include provenance hashes; large temporary logs, telemetry archives and abandoned experiments
are excluded from the release source tree.
