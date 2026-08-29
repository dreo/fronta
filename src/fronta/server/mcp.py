"""MCP tools (streamable HTTP), 1:1 with the REST operations."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from fronta.errors import FrontaError
from fronta.model import State, TaskFilter
from fronta.server.service import summary_to_dict, task_to_dict, task_type_to_dict

if TYPE_CHECKING:
    from fronta.server.service import Service


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
        priority: int = 0,
        run_at: str | None = None,
        key: str | None = None,
        concurrency_key: str | None = None,
    ) -> dict[str, int]:
        try:
            task_id = await service.enqueue(
                type,
                input,
                priority=priority,
                run_at=_parse_run_at(run_at),
                key=key,
                concurrency_key=concurrency_key,
            )
        except FrontaError as exc:
            raise ToolError(f"{exc.__class__.__name__}: {exc}") from exc
        return {"id": task_id}

    @mcp.tool(name="get_task", description="One task with its input, result, error and progress.")
    async def get_task(id: int) -> dict[str, Any]:
        try:
            return task_to_dict(await service.get_task(id))
        except FrontaError as exc:
            raise ToolError(f"{exc.__class__.__name__}: {exc}") from exc

    @mcp.tool(name="list_tasks", description="Task summaries, newest first; keyset by `before`.")
    async def list_tasks(
        type: str | None = None,
        state: str | None = None,
        key: str | None = None,
        before: int | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        try:
            flt = TaskFilter(
                type,
                None if state is None else State(state),
                key,
                before,
                limit or service.settings.list_page_size,
            )
        except ValueError as exc:
            raise ToolError(f"invalid state {state!r}") from exc
        items = await service.list_tasks(flt)
        page = min(max(flt.limit, 1), service.settings.list_page_max)
        return {
            "items": [summary_to_dict(row) for row in items],
            "next": items[-1].id if len(items) == page else None,
        }

    @mcp.tool(name="cancel", description="Cancel a queued task at once or a running one soon.")
    async def cancel(id: int) -> dict[str, Any]:
        try:
            state = await service.cancel(id)
        except FrontaError as exc:
            raise ToolError(f"{exc.__class__.__name__}: {exc}") from exc
        return {"id": id, "state": state.value}

    return mcp
