"""The capacity gate accepts slow-but-steady operation and rejects accumulating work."""

from __future__ import annotations

import pytest

from tests.stress.sustain import completion_windows, grade


@pytest.fixture
def sustained():
    return {
        "config": {
            "rate_s": 750,
            "producers": 4,
            "clients": 8,
            "concurrency": 256,
            "retention_s": 1200,
            "purge_interval_s": 60,
        },
        "requested_duration_s": 10800,
        "elapsed_s": 10800.5,
        "diagnostic": False,
        "producer_tasks_per_s": 3000,
        "identity": {"count": 32400000, "sum": 9, "xor": 7},
        "received_terminal_identity": {"count": 32400000, "sum": 9, "xor": 7},
        "final_backlog": {"tasks": 0, "events": 0},
        "worker_exit_codes": [0] * 8,
        "open_transaction": {"begin_s": 3600.1, "end_s": 5400.2},
        "claim_minutes": {"3": {"p99_ms": 750}},
        "samples": [
            {
                "seconds": minute * 60,
                "terminal_events": minute * 180000,
                "feed_lag": {"max_ms": 2500},
                "backlog": {"total": 500, "oldest_terminal_age_s": min(minute * 60, 1230)},
                "relation_bytes": [
                    {"relname": name, "bytes": (8 if minute >= 60 else 2) * 1024**3}
                    for name in ("tasks", "events")
                ],
            }
            for minute in range(1, 181)
        ],
    }


def test_latency_and_reused_high_water_storage_do_not_block_sustained_load(sustained):
    result = grade(sustained)
    assert result["passed"]
    assert all(result["checks"].values())


def test_a_higher_backlog_that_settles_after_recovery_is_not_continued_growth(sustained):
    for sample in sustained["samples"]:
        if sample["seconds"] >= 5400:
            sample["backlog"]["total"] += 10000
    assert grade(sustained)["passed"]


@pytest.mark.parametrize("problem", ["slow_producer", "slow_drain", "missing_task", "worker_died"])
def test_rate_and_correctness_remain_required(sustained, problem):
    if problem == "slow_producer":
        sustained["producer_tasks_per_s"] = 2700
    elif problem == "slow_drain":
        sustained["elapsed_s"] = 12000
    elif problem == "missing_task":
        sustained["received_terminal_identity"]["count"] -= 1
    else:
        sustained["worker_exit_codes"][0] = 71
    assert not grade(sustained)["passed"]


def test_a_final_catchup_does_not_hide_a_sustained_completion_deficit(sustained):
    for sample in sustained["samples"]:
        if sample["seconds"] < 10800:
            sample["terminal_events"] = int(sample["terminal_events"] * 0.9)
    result = grade(sustained)
    assert result["checks"]["completion_rate"]
    assert not result["checks"]["completion_windows"]
    assert not result["passed"]


def test_growing_backlog_fails_even_when_overall_rate_is_within_tolerance(sustained):
    for sample in sustained["samples"]:
        sample["backlog"]["total"] += int(sample["seconds"])
    result = grade(sustained)
    assert result["checks"]["completion_rate"]
    assert not result["checks"]["backlog"]


def test_continued_storage_growth_and_expired_history_are_reported(sustained):
    for sample in sustained["samples"]:
        if sample["seconds"] >= 9000:
            sample["relation_bytes"][0]["bytes"] += 1024**3
    sustained["samples"][-1]["backlog"]["oldest_terminal_age_s"] = 2000
    result = grade(sustained)
    assert not result["checks"]["storage_plateau"]
    assert not result["checks"]["retention"]


@pytest.mark.parametrize("problem", ["short", "diagnostic", "missing_window", "missing_hold"])
def test_incomplete_or_exploratory_evidence_cannot_certify_a_soak(sustained, problem):
    if problem == "short":
        sustained["requested_duration_s"] = 1800
    elif problem == "diagnostic":
        sustained["diagnostic"] = True
    elif problem == "missing_window":
        sustained["samples"] = [s for s in sustained["samples"] if s["seconds"] < 9000]
    else:
        sustained.pop("open_transaction")
    assert not grade(sustained)["passed"]


def test_completion_rate_uses_the_counter_timestamp():
    windows = completion_windows(
        [{"seconds": 1200, "completion_seconds": 1260, "terminal_events": 3600000}], 1200
    )
    assert windows[0]["tasks_per_s"] == pytest.approx(3600000 / 1260)
