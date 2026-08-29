"""Value objects shared by every layer. No I/O, no dependencies."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime
    from uuid import UUID

type JSON = dict[str, Any] | list[Any] | str | int | float | bool | None
"""Any JSON value. `Any` inside containers is the honest type of decoded JSON."""

MAX_DURATION_S = 30 * 86400.0
"""Upper bound for timeouts and backoff caps (30 days) so SQL interval math never overflows."""

MAX_BACKOFF_FACTOR = 10.0
"""With the exponent capped at 64 in SQL, factor <= 10 keeps `power()` inside float8 range."""

MAX_NAME_BYTES = 255
"""Task type names travel in NOTIFY payloads and indexes."""

MAX_KEY_BYTES = 1024
"""Dedupe and concurrency keys are indexed; B-tree entries must stay small."""


class State(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATES = frozenset({State.SUCCEEDED, State.FAILED, State.CANCELLED})


class Executor(StrEnum):
    ASYNCIO = "asyncio"
    PROCESS = "process"


def _check_finite(name: str, value: float, low: float, high: float) -> None:
    if not (isinstance(value, int | float) and math.isfinite(value) and low <= value <= high):
        msg = f"{name} must be a finite number in [{low}, {high}], got {value!r}"
        raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class Backoff:
    """Retry n waits `min(cap_s, base_s * factor**(n-1))` seconds, jittered to `[d/2, d]`."""

    base_s: float = 1.0
    factor: float = 2.0
    cap_s: float = 3600.0

    def __post_init__(self) -> None:
        _check_finite("backoff.base_s", self.base_s, 0.0, MAX_DURATION_S)
        _check_finite("backoff.factor", self.factor, 1.0, MAX_BACKOFF_FACTOR)
        _check_finite("backoff.cap_s", self.cap_s, 0.0, MAX_DURATION_S)
        if self.base_s > self.cap_s:
            msg = f"backoff.base_s ({self.base_s}) must not exceed backoff.cap_s ({self.cap_s})"
            raise ValueError(msg)

    def delay_bounds(self, retry: int) -> tuple[float, float]:
        """Inclusive `[min, max]` delay for retry `retry` (1-based), as the SQL computes it."""
        nominal = min(self.cap_s, self.base_s * self.factor ** min(retry - 1, 64))
        return nominal / 2, nominal

    def to_json(self) -> dict[str, float]:
        return {"base_s": self.base_s, "factor": self.factor, "cap_s": self.cap_s}

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Backoff:
        return cls(
            base_s=float(data["base_s"]), factor=float(data["factor"]), cap_s=float(data["cap_s"])
        )


@dataclass(frozen=True, slots=True)
class Policy:
    """Retry, timeout and concurrency policy of a task type. Snapshotted per task at enqueue."""

    max_attempts: int = 3
    attempt_timeout_s: float = 3600.0
    backoff: Backoff = field(default_factory=Backoff)
    max_concurrency: int | None = None
    max_concurrency_per_key: int | None = None

    def __post_init__(self) -> None:
        if not (isinstance(self.max_attempts, int) and self.max_attempts >= 1):
            msg = f"max_attempts must be an integer >= 1, got {self.max_attempts!r}"
            raise ValueError(msg)
        _check_finite("attempt_timeout_s", self.attempt_timeout_s, 0.001, MAX_DURATION_S)
        for name in ("max_concurrency", "max_concurrency_per_key"):
            value = getattr(self, name)
            if value is not None and not (isinstance(value, int) and value >= 1):
                msg = f"{name} must be None or an integer >= 1, got {value!r}"
                raise ValueError(msg)

    def to_json(self) -> dict[str, Any]:
        return {
            "max_attempts": self.max_attempts,
            "attempt_timeout_s": self.attempt_timeout_s,
            "backoff": self.backoff.to_json(),
            "max_concurrency": self.max_concurrency,
            "max_concurrency_per_key": self.max_concurrency_per_key,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Policy:
        return cls(
            max_attempts=int(data["max_attempts"]),
            attempt_timeout_s=float(data["attempt_timeout_s"]),
            backoff=Backoff.from_json(data["backoff"]),
            max_concurrency=data.get("max_concurrency"),
            max_concurrency_per_key=data.get("max_concurrency_per_key"),
        )


@dataclass(frozen=True, slots=True)
class TaskRow:
    """One row of `fronta.tasks`."""

    id: int
    type: str
    state: State
    priority: int
    key: str | None
    concurrency_key: str | None
    input: dict[str, Any]
    result: JSON
    error: dict[str, Any] | None
    progress: JSON
    attempt: int
    failures: int
    max_attempts: int
    attempt_timeout_s: float
    backoff: Backoff
    token: UUID | None
    lease_until: datetime | None
    worker: str | None
    cancel_requested_at: datetime | None
    created_at: datetime
    run_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True, slots=True)
class TaskSummary:
    """List row: everything except the potentially large JSON columns."""

    id: int
    type: str
    state: State
    priority: int
    key: str | None
    concurrency_key: str | None
    attempt: int
    failures: int
    max_attempts: int
    worker: str | None
    cancel_requested_at: datetime | None
    created_at: datetime
    run_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True, slots=True)
class TaskTypeRow:
    """One row of `fronta.task_types`."""

    name: str
    executor: Executor
    input_schema: dict[str, Any]
    output_schema: dict[str, Any] | None
    policy: Policy
    fingerprint: str
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class TaskTypeSpec:
    """What a worker publishes for one definition."""

    name: str
    executor: Executor
    input_schema: dict[str, Any]
    output_schema: dict[str, Any] | None
    policy: Policy

    @property
    def fingerprint(self) -> str:
        """sha256 of the canonical JSON of the published fields; stable across processes."""
        canonical = json.dumps(
            {
                "executor": self.executor.value,
                "input_schema": self.input_schema,
                "output_schema": self.output_schema,
                "policy": self.policy.to_json(),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class NewTask:
    """An enqueue request whose input is already encoded and cap-checked."""

    type: str
    input_json: str
    policy: Policy
    priority: int = 0
    run_at: datetime | None = None
    key: str | None = None
    concurrency_key: str | None = None


@dataclass(frozen=True, slots=True)
class TaskFilter:
    """List filters; `before` is the keyset cursor (last id of the previous page)."""

    type: str | None = None
    state: State | None = None
    key: str | None = None
    before: int | None = None
    limit: int = 50


KIB = 1024
MIB = 1024 * KIB

DEFAULT_RO_BINDS = ("/usr", "/etc/alternatives", "/etc/ld.so.cache")
"""The minimal host paths needed to execute a binary on a merged-usr Linux."""


@dataclass(frozen=True, slots=True)
class Sandbox:
    """Boundary and limits of a process task.

    `cpu_time_s` and `memory_bytes` are per-process rlimits (`RLIMIT_CPU`, `RLIMIT_AS`), best
    effort; `max_pids` bounds the whole sandbox (`RLIMIT_NPROC` inside its user namespace);
    `tmpfs_bytes` bounds `/work` and `/tmp` each; `max_output_bytes` bounds stdout and stderr each.
    """

    ro_binds: tuple[str, ...] = DEFAULT_RO_BINDS
    env: Mapping[str, str] = field(default_factory=dict)
    cpu_time_s: float | None = None
    memory_bytes: int | None = None
    max_pids: int | None = None
    tmpfs_bytes: int = 256 * MIB
    max_output_bytes: int = 256 * KIB

    def __post_init__(self) -> None:
        for path in self.ro_binds:
            if not PurePosixPath(path).is_absolute():
                msg = f"ro_binds entries must be absolute paths, got {path!r}"
                raise ValueError(msg)
        for name in ("cpu_time_s", "memory_bytes", "max_pids", "tmpfs_bytes", "max_output_bytes"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                msg = f"{name} must be positive, got {value!r}"
                raise ValueError(msg)
        for key in self.env:
            if key.startswith("FRONTA_") or "=" in key or not key:
                msg = f"invalid sandbox env name {key!r} (FRONTA_* names are reserved)"
                raise ValueError(msg)

    def identity(self) -> tuple[Any, ...]:
        """Hashable configuration key: one startup probe per distinct value."""
        return (
            self.ro_binds,
            tuple(sorted(self.env.items())),
            self.cpu_time_s,
            self.memory_bytes,
            self.max_pids,
            self.tmpfs_bytes,
        )
