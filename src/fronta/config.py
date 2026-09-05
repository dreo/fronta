"""Typed configuration: parsed once from `FRONTA_*` environment variables, validated at startup."""

from __future__ import annotations

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

KIB = 1024
MIB = 1024 * KIB


def _split(value: str | None) -> list[str]:
    return [] if value is None else [item.strip() for item in value.split(",") if item.strip()]


class Settings(BaseSettings):
    """Runtime settings. Durations are seconds; caps are UTF-8 bytes of the JSON encoding.

    `dsn` is required wherever Fronta opens its own connections (workers, the server, the SDK pool,
    event subscriptions) and is checked there, so enqueueing through a caller-supplied connection
    needs no DSN at all.
    """

    model_config = SettingsConfigDict(env_prefix="FRONTA_", extra="ignore", frozen=True)

    dsn: str | None = Field(None, min_length=1, description="PostgreSQL connection string")

    # Liveness
    lease_s: float = Field(30.0, gt=0, allow_inf_nan=False)
    heartbeat_s: float = Field(10.0, gt=0, allow_inf_nan=False)
    reaper_interval_s: float = Field(15.0, gt=0, allow_inf_nan=False)
    poll_interval_s: float = Field(5.0, gt=0, allow_inf_nan=False)
    grace_s: float = Field(30.0, ge=0, allow_inf_nan=False)
    kill_timeout_s: float = Field(5.0, gt=0, allow_inf_nan=False)

    # Worker
    concurrency: int = Field(10, ge=1)
    pool_size: int = Field(4, ge=1)
    connect_timeout_s: float = Field(10.0, ge=1, allow_inf_nan=False)
    statement_timeout_s: float = Field(30.0, ge=0.001, allow_inf_nan=False)

    # Retention
    retention_s: float = Field(7 * 86400.0, ge=0, allow_inf_nan=False)
    purge_interval_s: float = Field(600.0, gt=0, allow_inf_nan=False)
    purge_batch: int = Field(1000, ge=1)

    # Caps
    payload_cap: int = Field(MIB, ge=KIB)
    result_cap: int = Field(MIB, ge=KIB)
    progress_cap: int = Field(64 * KIB, ge=64)
    error_cap: int = Field(64 * KIB, ge=KIB)
    list_page_size: int = Field(50, ge=1)
    list_page_max: int = Field(200, ge=1)

    # Server
    server_host: str = "127.0.0.1"
    server_port: int = Field(8000, ge=1, le=65535)
    server_token: str | None = Field(None, min_length=1)  # an empty secret is a misconfiguration
    server_allowed_hosts: str | None = Field(
        None,
        description="Comma-separated Host header values (host or host:port; `host:*` for any"
        " port) the MCP endpoint accepts in addition to loopback, e.g. behind a reverse proxy.",
    )
    server_allowed_origins: str | None = Field(
        None,
        description="Comma-separated Origin header values (scheme://host[:port]) the MCP"
        " endpoint accepts in addition to loopback.",
    )

    # Sandbox
    bwrap_path: str = "bwrap"

    @property
    def renew_timeout_s(self) -> float:
        """End-to-end budget of one lease renewal: pool wait, statement and commit.

        Half of the slack between a heartbeat and the lease end, so a renewal that hits its budget
        can be retried at least once before the lease expires; never longer than a statement.
        """
        return min(self.statement_timeout_s, max(0.05, (self.lease_s - self.heartbeat_s) / 2))

    @property
    def claim_lock_timeout_s(self) -> float:
        """How long a claim may wait for a `task_types` lock: half a lease.

        A returned claim then always carries a usable lease, and the claim loop stays responsive
        to shutdown.
        """
        return self.lease_s / 2

    def allowed_hosts(self) -> list[str]:
        return _split(self.server_allowed_hosts)

    def allowed_origins(self) -> list[str]:
        return _split(self.server_allowed_origins)

    @model_validator(mode="after")
    def _check_relations(self) -> Settings:
        if self.heartbeat_s * 2 > self.lease_s:
            msg = (
                f"heartbeat_s ({self.heartbeat_s}) must be at most half of lease_s"
                f" ({self.lease_s}) so a failed renewal can be retried before the lease ends"
            )
            raise ValueError(msg)
        if self.list_page_size > self.list_page_max:
            msg = "list_page_size must not exceed list_page_max"
            raise ValueError(msg)
        return self
