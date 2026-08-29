"""Typed configuration: parsed once from `FRONTA_*` environment variables, validated at startup."""

from __future__ import annotations

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

KIB = 1024
MIB = 1024 * KIB


class Settings(BaseSettings):
    """Runtime settings. Durations are seconds; caps are UTF-8 bytes of the JSON encoding."""

    model_config = SettingsConfigDict(env_prefix="FRONTA_", extra="ignore", frozen=True)

    dsn: str = Field(min_length=1, description="PostgreSQL connection string")

    # Liveness
    lease_s: float = Field(30.0, gt=0)
    heartbeat_s: float = Field(10.0, gt=0)
    reaper_interval_s: float = Field(15.0, gt=0)
    poll_interval_s: float = Field(5.0, gt=0)
    grace_s: float = Field(30.0, ge=0)
    kill_timeout_s: float = Field(5.0, gt=0)

    # Worker
    concurrency: int = Field(10, ge=1)
    pool_size: int = Field(4, ge=1)
    connect_timeout_s: float = Field(10.0, ge=1)
    statement_timeout_s: float = Field(30.0, ge=0.001)

    # Retention
    retention_s: float = Field(7 * 86400.0, ge=0)
    purge_interval_s: float = Field(600.0, gt=0)
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

    # Sandbox
    bwrap_path: str = "bwrap"

    @model_validator(mode="after")
    def _check_relations(self) -> Settings:
        if self.heartbeat_s >= self.lease_s:
            msg = f"heartbeat_s ({self.heartbeat_s}) must be shorter than lease_s ({self.lease_s})"
            raise ValueError(msg)
        if self.list_page_size > self.list_page_max:
            msg = "list_page_size must not exceed list_page_max"
            raise ValueError(msg)
        return self
