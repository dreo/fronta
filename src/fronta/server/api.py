"""REST API under `/api/v1`, 1:1 with the MCP tools."""

from __future__ import annotations

import secrets
from datetime import datetime  # noqa: TC003  # pydantic evaluates the annotation at runtime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from fronta.model import JSON, State, TaskFilter
from fronta.server.service import Service, task_to_dict, task_type_to_dict
from fronta.store import MAX_PRIORITY, MIN_PRIORITY


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
    priority: int = Field(0, strict=True, ge=MIN_PRIORITY, le=MAX_PRIORITY)
    run_at: datetime | None = None
    key: str | None = Field(None, min_length=1, max_length=1024)
    concurrency_key: str | None = Field(None, min_length=1, max_length=1024)
    metadata: JSON = None


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
        metadata=body.metadata,
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
    type: Annotated[str | None, Query(alias="type")] = None,
    state: State | None = None,
    key: str | None = None,
    before: int | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    settings = service.settings
    page = settings.list_page_size if limit is None else limit
    return await service.list_tasks(TaskFilter(type, state, key, before, page))


@router.post("/tasks/{task_id}/cancel")
async def cancel(task_id: int, service: Annotated[Service, Depends(get_service)]) -> dict[str, Any]:
    state = await service.cancel(task_id)
    return {"id": task_id, "state": state.value}


@router.post("/task-types/{task_type:path}/pause")
async def pause(
    task_type: str, service: Annotated[Service, Depends(get_service)]
) -> dict[str, bool]:
    await service.pause(task_type)
    return {"paused": True}


@router.post("/task-types/{task_type:path}/resume")
async def resume(
    task_type: str, service: Annotated[Service, Depends(get_service)]
) -> dict[str, bool]:
    await service.resume(task_type)
    return {"paused": False}


@router.post("/tasks/{task_id}/requeue")
async def requeue(
    task_id: int, service: Annotated[Service, Depends(get_service)]
) -> dict[str, Any]:
    await service.requeue(task_id)
    return {"id": task_id, "state": "queued"}


@router.get("/stats")
async def stats(service: Annotated[Service, Depends(get_service)]) -> dict[str, Any]:
    return await service.stats()
