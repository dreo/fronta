"""Capacity and recovery grading; latency is measured without a fixed pass/fail cutoff."""

from __future__ import annotations

from statistics import median

ACCEPTANCE_VERSION = "sustained-load-v2"


def completion_windows(samples, window_s):
    previous = {"seconds": 0, "terminal_events": 0}
    windows = []
    for sample in samples:
        stamp = sample.get("completion_seconds", sample["seconds"])
        elapsed = stamp - previous["seconds"]
        if elapsed >= window_s - 1:  # tolerate sampling either side of a minute boundary
            windows.append(
                {
                    "begin_s": previous["seconds"],
                    "end_s": stamp,
                    "tasks_per_s": (sample["terminal_events"] - previous["terminal_events"])
                    / elapsed,
                }
            )
            previous = {"seconds": stamp, "terminal_events": sample["terminal_events"]}
    return windows


def grade(report):
    config, samples = report["config"], report["samples"]
    rate = config["rate_s"] * config["producers"]
    duration = report["requested_duration_s"]
    retention = config["retention_s"]
    windows = completion_windows(samples, retention)
    baseline = [s for s in samples if 2400 <= s["seconds"] < 3600]
    before = [s for s in samples if 7200 <= s["seconds"] < 8400]
    after = [s for s in samples if duration - retention <= s["seconds"] <= duration + 1]
    # A fleet's in-flight batches are ordinary snapshot variation, not accumulating work.
    backlog_allowance = 8 * config["concurrency"] + config["producers"] * config["clients"] + 256
    backlog = {
        "pre_transaction_median": (
            median(s["backlog"]["total"] for s in baseline) if baseline else None
        ),
        "recovery_median": median(s["backlog"]["total"] for s in before) if before else None,
        "final_median": median(s["backlog"]["total"] for s in after) if after else None,
        "allowance_tasks": backlog_allowance,
    }
    # Allow 30 minutes after the held transaction ends, then compare two 30-minute periods.
    early = [s for s in samples if 7200 <= s["seconds"] < 9000]
    late = [s for s in samples if 9000 <= s["seconds"] <= 10801]
    storage = {}
    for name in ("tasks", "events"):
        sizes = [
            [next(r["bytes"] for r in s["relation_bytes"] if r["relname"] == name) for s in group]
            for group in (early, late)
        ]
        storage[name] = {
            "early_max_bytes": max(sizes[0]) if sizes[0] else None,
            "late_max_bytes": max(sizes[1]) if sizes[1] else None,
        }
    oldest = samples[-1]["backlog"]["oldest_terminal_age_s"] if samples else None
    cleanup_limit = retention + 2 * 1.2 * config["purge_interval_s"]
    identity = report.get("identity")
    completed_rate = report.get("received_terminal_identity", {}).get("count", 0) / max(
        report["elapsed_s"], 0.001
    )
    checks = {
        "accounting": bool(identity)
        and identity["count"] == int(rate * duration)
        and identity == report.get("received_terminal_identity")
        and report.get("final_backlog") == {"tasks": 0, "events": 0}
        and bool(report.get("worker_exit_codes"))
        and all(code == 0 for code in report["worker_exit_codes"])
        and not report.get("error"),
        "producer_rate": report.get("producer_tasks_per_s", 0) >= rate * 0.99,
        "completion_rate": completed_rate >= rate * 0.99,
        "completion_windows": bool(windows)
        and all(w["tasks_per_s"] >= rate * 0.99 for w in windows),
        "backlog": len(before) >= 19
        and len(after) >= 19
        and backlog["final_median"] <= backlog["recovery_median"] + backlog_allowance,
        "retention": bool(samples) and (oldest is None or oldest <= cleanup_limit),
        "storage_plateau": len(early) >= 29
        and len(late) >= 29
        and all(
            v["late_max_bytes"] <= v["early_max_bytes"] * 1.01 + 8192 for v in storage.values()
        ),
        "open_transaction": report.get("open_transaction", {}).get("begin_s", -1) >= 3600
        and report.get("open_transaction", {}).get("end_s", 0)
        - report.get("open_transaction", {}).get("begin_s", 0)
        >= 1800,
    }
    return {
        "acceptance_version": ACCEPTANCE_VERSION,
        "checks": checks,
        "sustainability": {
            "completion_tasks_per_s": completed_rate,
            "completion_windows": windows,
            "backlog": backlog,
            "storage": storage,
            "storage_growth_tolerance_fraction": 0.01,
            "oldest_terminal_age_s": oldest,
            "cleanup_age_limit_s": cleanup_limit,
        },
        "passed": not report["diagnostic"] and duration >= 10800 and all(checks.values()),
    }
