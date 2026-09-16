"""Report per-task counters from complete saved runs (never infer them from peak rates)."""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def summarize(paths):
    lines = [
        "| Run / case | n | Tasks/s median (range) | WAL fsync/task | WAL bytes/task |"
        " xact commits/task | Worker CPU ms/task | Object wait samples |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for path in paths:
        report = json.loads(Path(path).read_text())
        groups = defaultdict(list)
        for run in report["runs"]:
            if "error" not in run and "db_after" in run and "tasks_per_s" in run:
                groups[run["config"]["name"]].append(run)
        for name, runs in sorted(groups.items()):
            values = defaultdict(list)
            for run in runs:
                n = run["correctness"]["succeeded"]
                before, after = run["db_before"], run["db_after"]
                values["rate"].append(run["tasks_per_s"])
                for key in ("wal_sync", "wal_bytes"):
                    values[key].append((after["wal"][key] - before["wal"][key]) / n)
                values["xact"].append(
                    (after["database"]["xact_commit"] - before["database"]["xact_commit"]) / n
                )
                values["cpu"].append(1000 * run["worker_cpu_s"] / n)
                waits = run["active_wait_samples"]
                values["object"].append(
                    100 * waits.get("Lock:object", 0) / max(1, sum(waits.values()))
                )
            mid = {key: statistics.median(v) for key, v in values.items()}
            rates = values["rate"]
            lines.append(
                f"| {report['label']} / {name} | {len(runs)} | {mid['rate']:.0f}"
                f" ({min(rates):.0f}-{max(rates):.0f})"
                f" | {mid['wal_sync']:.3f} | {mid['wal_bytes']:.0f}"
                f" | {mid['xact']:.2f} | {mid['cpu']:.2f}"
                f" | {mid['object']:.1f}% |"
            )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    sys.stdout.write(summarize(sys.argv[1:]))
