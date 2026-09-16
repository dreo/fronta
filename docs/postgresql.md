# PostgreSQL configuration

Use PostgreSQL **18** for new deployments; Fronta supports 16 and later. Keep task commits durable
and run PostgreSQL on storage with predictable write latency. Fronta does not change server settings.

## Starting configuration

The following profile was used for the physical Linux sustained-load test and the durable
throughput tests. It is a starting point for a server with at least 8 GiB available to PostgreSQL:

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
writeback; benchmark them on your storage. Larger buffers and WAL allowance reduce pressure from
buffer eviction and checkpoints. [PostgreSQL memory and writeback settings](https://www.postgresql.org/docs/18/runtime-config-resource.html).

`max_wal_size` is a checkpoint target, not a hard disk limit. Monitor free disk space and WAL
retention, especially with replication slots. Keep durability enabled when measuring capacity;
turning it off changes the guarantees being tested. [PostgreSQL WAL settings](https://www.postgresql.org/docs/18/runtime-config-wal.html).

Size `max_connections` for the whole deployment: each worker needs up to `pool_size + 3`
connections, each feed consumer one, plus SDK/server pools and administration. Hints can share a
connection on the same event loop. Start with Fronta's default pool size 4; increase concurrency
according to handler duration and resource use, then measure.

## Sustained operation

Monitor completion rate against arrival rate, queued backlog, oldest retained task/event,
relation size, old transactions, autovacuum and disk waits. Stable throughput with bounded backlog
and reusable storage is the capacity objective; a universal 50 ms latency cutoff is not used.

Keep transactions and feed batch processing short. A long open write transaction can prevent
vacuum from reclaiming old row versions across the database, slowing claims even without a lock
on a Fronta table. Retention deletes finished tasks and expired events in batches; vacuum makes
that space reusable. Tables are not partitioned, and their files need not shrink after cleanup.
[PostgreSQL vacuum and space reuse](https://www.postgresql.org/docs/18/routine-vacuuming.html).

At 3,000 completions/s, 20-minute retention retains roughly 3.6 million completed rows. Seven-day
retention at the same rate implies roughly 1.8 billion rows; choose retention and storage for the
history you actually need. Fronta's default retention is seven days.

If latency spikes, correlate PostgreSQL wait events with disk writeback and memory pressure.
A WAL flush can wait behind data-file writes on the same device. Avoid certifying performance
on a host that is swapping heavily. Separate WAL storage can reduce contention when the devices
are physically independent, but does not replace measurement of the real workload.
