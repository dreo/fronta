"""MCP tools (streamable HTTP), 1:1 with the REST operations."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import Field

from fronta.errors import FrontaError
from fronta.model import JSON, State, TaskFilter
from fronta.server.service import task_to_dict, task_type_to_dict

if TYPE_CHECKING:
    from collections.abc import Iterator

    from fronta.server.service import Service


@contextmanager
def _tool_errors() -> Iterator[None]:
    try:
        yield
    except FrontaError as exc:
        raise ToolError(f"{exc.__class__.__name__}: {exc}") from exc


def _parse_run_at(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        msg = f"run_at must be an ISO 8601 timestamp, got {value!r}"
        raise ToolError(msg) from exc
    if parsed.tzinfo is None:
        msg = "run_at must carry a timezone offset"
        raise ToolError(msg)
    return parsed


def make_mcp(service: Service) -> MCPServer[Any]:
    """The MCP server exposing the control plane over `service`."""
    mcp: MCPServer[Any] = MCPServer(
        "fronta",
        instructions="Enqueue, inspect and cancel Fronta tasks. Task types list the input schema.",
    )

    @mcp.tool(name="list_task_types", description="Published task types with their input schemas.")
    async def list_task_types() -> list[dict[str, Any]]:
        return [task_type_to_dict(row) for row in await service.list_task_types()]

    @mcp.tool(name="enqueue", description="Enqueue a task; returns its id (dedupe by key).")
    async def enqueue(  # noqa: PLR0913  # the operation's parameters, as specified
        type: str,
        input: dict[str, Any],
        *,
        priority: Annotated[int, Field(strict=True)] = 0,
        run_at: str | None = None,
        key: str | None = None,
        concurrency_key: str | None = None,
        metadata: JSON = None,
    ) -> dict[str, int]:
        with _tool_errors():
            task_id = await service.enqueue(
                type,
                input,
                priority=priority,
                run_at=_parse_run_at(run_at),
                key=key,
                concurrency_key=concurrency_key,
                metadata=metadata,
            )
        return {"id": task_id}

    @mcp.tool(name="get_task", description="One task with its input, result, error and progress.")
    async def get_task(id: int) -> dict[str, Any]:
        with _tool_errors():
            return task_to_dict(await service.get_task(id))

    @mcp.tool(name="list_tasks", description="Task summaries, newest first; keyset by `before`.")
    async def list_tasks(
        type: str | None = None,
        state: str | None = None,
        key: str | None = None,
        before: int | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        page = service.settings.list_page_size if limit is None else limit
        try:
            flt = TaskFilter(
                type,
                None if state is None else State(state),
                key,
                before,
                page,
            )
        except ValueError as exc:
            raise ToolError(f"invalid state {state!r}") from exc
        return await service.list_tasks(flt)

    @mcp.tool(name="cancel", description="Cancel a queued task at once or a running one soon.")
    async def cancel(id: int) -> dict[str, Any]:
        with _tool_errors():
            state = await service.cancel(id)
        return {"id": id, "state": state.value}

    @mcp.tool(name="pause", description="Pause new admissions of a task type.")
    async def pause(type: str) -> dict[str, bool]:
        with _tool_errors():
            await service.pause(type)
        return {"paused": True}

    @mcp.tool(name="resume", description="Resume admissions of a task type.")
    async def resume(type: str) -> dict[str, bool]:
        with _tool_errors():
            await service.resume(type)
        return {"paused": False}

    @mcp.tool(
        name="requeue",
        description="Requeue a failed or cancelled task with a fresh failure budget.",
    )
    async def requeue(id: int) -> dict[str, Any]:
        with _tool_errors():
            await service.requeue(id)
        return {"id": id, "state": "queued"}

    @mcp.tool(name="stats", description="Queue counts, oldest due work and subscription backlog.")
    async def stats() -> dict[str, Any]:
        return await service.stats()

    return mcp
