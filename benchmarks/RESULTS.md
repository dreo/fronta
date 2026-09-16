# Measured capacity

These are **pre-release overhaul measurements**, not a throughput guarantee for arbitrary tasks
or hardware. All rates below retain synchronous task commits. The functional release suite
separately covers cancellation, retries, fencing, limits, feeds, API behavior and Linux sandboxes.

## Throughput

Three interleaved runs per workload, macOS clients and tuned PostgreSQL 18 in Docker:

| Workload | Median tasks/s |
|---|---:|
| No-op, Fronta defaults | 11,640 |
| One type, concurrency 256 | 58,150 |
| Three types, concurrency 256 | 51,737 |
| Live SDK producers | 5,739 |
| Live producers with terminal feed | 5,440 |
| Preloaded backlog with terminal feed | 37,362 |
| 512 KiB input | 332 |

The 21 trials reconciled 3,156,000 tasks. All throughput, memory, durability and batching checks
passed. Source SHA-256 starts `8b3fb5405fd8`; it includes the claim-search locking refinement but
predates the final adversarial release fixes. [Raw report](results/durable-throughput.json),
[grade](results/durable-throughput-grade.json), [workloads and replay](README.md).

## Three-hour physical Linux soak

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
