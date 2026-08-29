"""`fronta` command line: `db init`, `worker`, `server`."""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import sys
from pathlib import Path

import click
import psycopg
from pydantic import ValidationError

from fronta import store
from fronta.config import Settings
from fronta.errors import ConfigurationError
from fronta.worker import Worker

log = logging.getLogger(__name__)

SERVER_EXTRA_MODULES = frozenset({"fastapi", "starlette", "uvicorn", "jinja2", "mcp", "jsonschema"})
"""Top-level modules provided by the `server` extra."""


def configure_logging() -> None:
    """Our loggers follow `LOG_LEVEL_OURS` (default INFO), libraries `LOG_LEVEL_LIBS` (WARNING)."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL_LIBS", "WARNING").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    logging.getLogger("fronta").setLevel(os.environ.get("LOG_LEVEL_OURS", "INFO").upper())


def load_object(target: str, expected: type) -> object:
    """Import `module:attr` (the current directory is importable, as with uvicorn)."""
    module_name, sep, attr = target.partition(":")
    if not sep or not module_name or not attr:
        msg = f"expected module:attr, got {target!r}"
        raise click.BadParameter(msg)
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        msg = f"cannot import {module_name!r}: {exc}"
        raise click.BadParameter(msg) from exc
    try:
        obj = getattr(module, attr)
    except AttributeError as exc:
        msg = f"{module_name!r} has no attribute {attr!r}"
        raise click.BadParameter(msg) from exc
    if not isinstance(obj, expected):
        msg = f"{target} is {type(obj).__name__}, expected {expected.__name__}"
        raise click.BadParameter(msg)
    return obj


@click.group()
@click.version_option(package_name="fronta")
def main() -> None:
    """Fronta: distributed task processing on PostgreSQL."""


@main.group()
def db() -> None:
    """Database administration."""


@db.command("init")
@click.option("--dsn", envvar="FRONTA_DSN", required=True, help="PostgreSQL DSN (or FRONTA_DSN).")
def db_init(dsn: str) -> None:
    """Create the `fronta` schema (idempotent)."""

    async def run() -> None:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            await store.init_schema(conn)

    try:
        asyncio.run(run())
    except psycopg.Error as exc:
        raise click.ClickException(f"database error: {exc}") from exc
    click.echo("fronta schema is ready")


@main.command()
@click.argument("target")
def worker(target: str) -> None:
    """Run the worker TARGET (`module:attr`, a `fronta.Worker`) until SIGTERM/SIGINT."""
    configure_logging()
    instance = load_object(target, Worker)
    assert isinstance(instance, Worker)  # noqa: S101  # narrowed by load_object
    try:
        settings = instance.settings  # FRONTA_* is read here, not when the module was imported
    except ValidationError as exc:
        raise click.ClickException(f"invalid settings: {exc}") from exc
    log.info("worker target %s, concurrency %d", target, settings.concurrency)
    try:
        sys.exit(asyncio.run(instance.run()))
    except psycopg.OperationalError as exc:  # could not reach the database at start
        raise click.ClickException(f"database unavailable: {exc}") from exc


@main.command()
@click.option(
    "--host", default=None, help="Bind address (default FRONTA_SERVER_HOST or 127.0.0.1)."
)
@click.option("--port", default=None, type=int, help="Port (default FRONTA_SERVER_PORT or 8000).")
def server(host: str | None, port: int | None) -> None:
    """Serve the REST API, the MCP endpoint and the dashboard (needs `fronta[server]`)."""
    configure_logging()
    try:
        from fronta.server import serve  # noqa: PLC0415  # optional dependency, loaded on demand
    except ImportError as exc:
        if (exc.name or "").split(".")[0] not in SERVER_EXTRA_MODULES:
            raise  # a genuine broken import, not a missing extra
        msg = f"the server needs the optional dependencies: pip install 'fronta[server]' ({exc})"
        raise click.ClickException(msg) from exc
    overrides = {
        k: v for k, v in {"server_host": host, "server_port": port}.items() if v is not None
    }
    try:
        settings = Settings()  # type: ignore[call-arg]  # pydantic-settings reads FRONTA_DSN
        if overrides:
            settings = Settings(**{**settings.model_dump(), **overrides})  # re-validated
    except ValidationError as exc:
        raise click.ClickException(f"invalid settings: {exc}") from exc
    try:
        serve(settings)
    except ConfigurationError as exc:
        raise click.ClickException(str(exc)) from exc
