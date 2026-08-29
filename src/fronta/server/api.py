"""REST API under `/api/v1`, 1:1 with the MCP tools."""

from __future__ import annotations

import secrets
from datetime import datetime  # noqa: TC003  # pydantic evaluates the annotation at runtime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from fronta.model import State, TaskFilter
from fronta.server.service import Service, summary_to_dict, task_to_dict, task_type_to_dict


def get_service(request: Request) -> Service:
    service: Service = request.app.state.service
    return service


def bearer_ok(authorization: str | None, token: str | None) -> bool:
    """True when the header carries exactly the configured token; nothing passes without one."""
    if token is None or authorization is None:
        return False
    scheme, _, value = authorization.partition(" ")
    return scheme.lower() == "bearer" and secrets.compare_digest(
        value.strip().encode(), token.encode()
    )


def require_auth(request: Request) -> None:
    settings = request.app.state.settings
    if not bearer_ok(request.headers.get("authorization"), settings.server_token):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "missing or invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_auth)])


class EnqueueRequest(BaseModel):
    type: str = Field(min_length=1, max_length=255)
    input: dict[str, Any]
    priority: int = 0
    run_at: datetime | None = None
    key: str | None = Field(None, min_length=1, max_length=1024)
    concurrency_key: str | None = Field(None, min_length=1, max_length=1024)


@router.get("/task-types")
async def list_task_types(
    service: Annotated[Service, Depends(get_service)],
) -> list[dict[str, Any]]:
    return [task_type_to_dict(row) for row in await service.list_task_types()]


@router.post("/tasks", status_code=status.HTTP_201_CREATED)
async def enqueue(
    body: EnqueueRequest, service: Annotated[Service, Depends(get_service)]
) -> dict[str, int]:
    task_id = await service.enqueue(
        body.type,
        body.input,
        priority=body.priority,
        run_at=body.run_at,
        key=body.key,
        concurrency_key=body.concurrency_key,
    )
    return {"id": task_id}


@router.get("/tasks/{task_id}")
async def get_task(
    task_id: int, service: Annotated[Service, Depends(get_service)]
) -> dict[str, Any]:
    return task_to_dict(await service.get_task(task_id))


@router.get("/tasks")
async def list_tasks(  # noqa: PLR0913  # the filters, as specified
    *,
    service: Annotated[Service, Depends(get_service)],
    request: Request,
    type: Annotated[str | None, Query(alias="type")] = None,
    state: State | None = None,
    key: str | None = None,
    before: int | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    settings = request.app.state.settings
    page = settings.list_page_size if limit is None else limit
    items = await service.list_tasks(TaskFilter(type, state, key, before, page))
    page = min(max(page, 1), settings.list_page_max)
    return {
        "items": [summary_to_dict(row) for row in items],
        "next": items[-1].id if len(items) == page else None,
    }


@router.post("/tasks/{task_id}/cancel")
async def cancel(task_id: int, service: Annotated[Service, Depends(get_service)]) -> dict[str, Any]:
    state = await service.cancel(task_id)
    return {"id": task_id, "state": state.value}
