"""Grade three interleaved simplification throughput trials against the plan's fixed bounds."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path


def grade(report, baseline):
    settings = {row["name"]: row["setting"] for row in report.get("postgres_settings", [])}
    durability = {
        name: settings.get(name) == "on"
        for name in ("fsync", "synchronous_commit", "full_page_writes")
    }
    names = ("defaults", "single-type", "multi-type", "live", "feed", "payload", "feed-drain")
    groups = {name: [r for r in report["runs"] if r["config"]["name"] == name] for name in names}
    assert all(len(runs) == 3 for runs in groups.values()), "three trials per workload required"
    assert all("error" not in r for r in report["runs"]), "a workload failed correctness checks"
    summary = {
        name: {
            "runs": len(runs),
            "median_tasks_per_s": statistics.median(r["tasks_per_s"] for r in runs),
            "range_tasks_per_s": [
                min(r["tasks_per_s"] for r in runs),
                max(r["tasks_per_s"] for r in runs),
            ],
        }
        for name, runs in groups.items()
    }
    rate = {name: value["median_tasks_per_s"] for name, value in summary.items()}
    multi_ratio = rate["multi-type"] / rate["single-type"]
    feed_ratio = rate["feed"] / rate["live"]
    feed_p99 = max(r["feed"]["event_age_at_delivery"]["p99_ms"] for r in groups["feed"])
    payload_rss = max(r["worker_peak_rss_sum_bytes"] for r in groups["payload"])
    payload_p99 = max(r["correctness"]["service_ms"][2] for r in groups["payload"])
    rss_limit = baseline["worker_peak_rss_sum_bytes"] * 1.5
    p99_limit = baseline["correctness"]["service_ms"][2] * 2
    feed_fsync = max(
        (r["db_after"]["wal"]["wal_sync"] - r["db_before"]["wal"]["wal_sync"]) / r["config"]["jobs"]
        for r in groups["feed-drain"]
    )
    feed_object_waits = max(
        r["active_wait_samples"].get("Lock:object", 0)
        / max(1, sum(r["active_wait_samples"].values()))
        for r in groups["feed-drain"]
    )
    checks = {
        "defaults": rate["defaults"] >= 3000,
        "single-type": rate["single-type"] >= 15300,
        "multi-type": multi_ratio >= 0.8,
        "live": rate["live"] >= 4400,
        "feed-throughput": feed_ratio >= 0.9,
        "feed-lag": feed_p99 < 100,
        "payload-rss": payload_rss <= rss_limit,
        "payload-p99": payload_p99 <= p99_limit,
        "feed-drain-fsync": feed_fsync < 0.1,
        "feed-drain-object-waits": feed_object_waits < 0.01,
    }
    return {
        "grading_version": "durable-throughput-v2",
        "source_sha256": report["source_sha256"],
        "durability": durability,
        "summary": summary,
        "ratios": {"multi_to_single": multi_ratio, "feed_to_live": feed_ratio},
        "worst_feed_p99_ms": feed_p99,
        "worst_payload_rss_bytes": payload_rss,
        "worst_payload_p99_ms": payload_p99,
        "payload_limits": {"rss_bytes": rss_limit, "p99_ms": p99_limit},
        "worst_feed_drain_fsync_per_task": feed_fsync,
        "worst_feed_drain_object_wait_fraction": feed_object_waits,
        "checks": checks,
        "passed": all(durability.values()) and all(checks.values()),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    baseline = json.loads(Path("benchmarks/payload-baseline.json").read_text())
    result = grade(json.loads(args.report.read_text()), baseline)
    result["source"] = str(args.report)
    with args.output.open("x") as output:
        output.write(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))  # noqa: T201  # command output
    sys.exit(0 if result["passed"] else 1)
