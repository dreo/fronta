# Measured capacity

These measurements describe specific workloads and hardware, with synchronous task commits.
They are not throughput guarantees for arbitrary handlers. [Workloads and replay](README.md).

## Release validation

Release source `a6c68e292ae7` passed the [CI matrix](https://github.com/dreo/fronta/actions/runs/35064552795)
across Python 3.12–3.14, PostgreSQL 16/18 and Linux/macOS, including real Linux sandboxes and
clean wheel/source installs. The physical Linux suite passed 374 tests with one platform skip.
The v0.4.0 upgrade rehearsal completed 50,000 tasks with no bad outcomes and 12 clean worker exits.
A [five-minute renewal run](results/release-renewals.json) held 2,000 concurrent one-minute tasks:
78 renewal batches, no expired leases and no reaped tasks.

## Final-source physical diagnostic

The same physical host and durable profile used below ran source `a6c68e292ae7` for ten minutes
at 3,000 arrivals/s, with an unrelated write transaction held from minute one to minute three.
It processed **1,800,000 tasks**, reconciled producer/completion identities, drained task/event
backlogs to zero and exited all eight workers cleanly. Overall completion rate, including final
drain, was **2,994.5/s**. Sampled backlog peaked at
**4,311** and returned to its ordinary range after the hold.

This is a short diagnostic, so its report deliberately does not pass the three-hour acceptance
grade. It verifies capacity and recovery on the final code; the earlier long run below provides
the retention/storage evidence. [Final physical report](results/release-physical.json).

## Release throughput

Three interleaved runs per workload, macOS clients and PostgreSQL 18 in Docker, source
`a6c68e292ae7`. The host had about 38 GB of swap in use during this run.

| Workload | Median tasks/s |
|---|---:|
| No-op, Fronta defaults | 7,244 |
| One type, concurrency 256 | 32,887 |
| Three types, concurrency 256 | 31,161 |
| Live SDK producers | 3,051 |
| Live producers with terminal feed | 3,063 |
| Preloaded backlog with terminal feed | 23,077 |
| 512 KiB input | 242 |

All 21 trials reconciled 3,156,000 tasks with durability enabled. **Nine of ten comparison checks
passed:** live enqueue was below the historical 4,400/s comparison bound. Memory, payload latency,
feed overhead/lag and commit-batching checks passed. This is not an all-green throughput grade.
[Release report](results/release-throughput.json), [grade](results/release-throughput-grade.json).

An earlier source (`8b3fb5405fd8`) passed all comparison checks, including 5,739 live enqueues/s.
Different host conditions and the later correctness fixes make those rates a baseline, not a
promise for this release. [Earlier report](results/durable-throughput.json),
[earlier grade](results/durable-throughput-grade.json).

A same-server old/new comparison caught a completion-plan regression during review. Removing a
redundant state filter restored indexed updates while preserving ordered locks and token fencing;
concurrent-heartbeat and stale-token probes verified the correction.
The [final old/release/release/old comparison](results/release-comparison.json) measured 43.9k/38.8k
single-type tasks/s and 3.84k/3.50k live enqueues/s: roughly 12%/9% overhead for the correctness
fixes. Both versions missed the historical live-enqueue bound on this host.

## Three-hour pre-release physical Linux soak

Physical i5-13500T host, 61 GiB RAM, NVMe storage, PostgreSQL 18.6, 2 GiB shared buffers,
4 GiB WAL target, 256 KiB backend/background writeback settings, full durability. Eight workers
processed 32.4 million tasks at 3,000 arrivals/s with 20-minute retention.

| Measure | Result |
|---|---|
| Completion rate over the full run | 2,999.90/s |
| Producer/completion identity accounting | Exact match |
| Final task/event backlog | 0 / 0 |
| Worker exits | All eight clean |
| Oldest retained terminal task at finish | 1,202.4 s |
| Final-hour task/index footprint | Stable at 6.08 GB |
| Final-hour event/index footprint | Stable at 187 MB |

The injected write transaction from minute 60 to 90 prevented normal vacuum reclamation.
Completion rate fell to 649/s in the worst 20-minute window; backlog peaked at 4.45 million tasks.
After the transaction ended, the backlog recovered without intervention and returned to ordinary
levels 22 minutes later. Both final 30-minute storage windows had identical maximum sizes.

**The strict every-window throughput check failed during the injected fault.** Accounting,
overall capacity, backlog recovery, retention and storage checks passed. This establishes recovery
and bounded steady-state storage on that host, not uninterrupted 3,000/s through any database fault.
A 50 ms latency threshold is not a sustained-capacity requirement.

![Completion rate, backlog and retained storage](results/physical-soak.svg)

This run used source hash `afbd72f512a2`, before the later claim-search refinement. A separate
paired ten-minute diagnostic found that refinement reduced the held-transaction peak backlog from
477,094 to 180,080 tasks; both runs reconciled 1.8 million tasks. It does not make long transactions
harmless. [Physical report](results/physical-soak.json), [grade](results/physical-soak-grade.json),
[provenance](results/provenance.json).

Fronta uses ordinary tables with batched retention deletes. The stable recovered footprint did
not justify an active/history split or partitioning. See [operating configuration](../docs/postgresql.md)
for retention sizing, transaction hygiene and durable-write diagnostics.
