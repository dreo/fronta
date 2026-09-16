"""Print compact Markdown tables from saved stress results."""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def summarize(files):
    lines = [
        "| Run / case | Repeats | Tasks/s (median, min-max) | Claim p50 / p95 ms | Connections |",
        "|---|---:|---:|---:|---:|",
    ]
    for file in files:
        report = json.loads(Path(file).read_text())
        groups = defaultdict(list)
        for run in report["runs"]:
            groups[run["config"]["name"]].append(run)
        for name, runs in sorted(groups.items()):
            errors = [r["error"] for r in runs if "error" in r]
            if errors:
                lines.append(f"| {report['label']} / {name} | {len(runs)} | ERROR: {errors} | | |")
                continue
            rates = [r["tasks_per_s"] for r in runs if "tasks_per_s" in r]
            rate = (
                f"{statistics.median(rates):.0f} ({min(rates):.0f}-{max(rates):.0f})"
                if rates
                else "—"
            )
            claims = [
                r.get("claim_latency", r.get("operations", {}).get("claim", {})) for r in runs
            ]
            claims = [c for c in claims if c.get("n")]
            latency = (
                f"{statistics.median(c['p50_ms'] for c in claims):.2f} / "
                f"{statistics.median(c['p95_ms'] for c in claims):.2f}"
                if claims
                else "—"
            )
            peak = max(r["peak_connections"] for r in runs)
            lines.append(
                f"| {report['label']} / {name} | {len(runs)} | {rate} | {latency} | {peak} |"
            )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    sys.stdout.write(summarize(sys.argv[1:]))
