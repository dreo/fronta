"""FastAPI application: REST + MCP + dashboard, with bearer auth and a request body limit."""

from __future__ import annotations

import contextlib
from importlib import resources
from typing import TYPE_CHECKING

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from fronta.errors import (
    ConfigurationError,
    InvalidInput,
    NotCancellable,
    PayloadTooLarge,
    TaskNotFound,
    UnknownTaskType,
)
from fronta.server.api import bearer_ok, router
from fronta.server.mcp import make_mcp
from fronta.server.service import Service

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Mapping

    from starlette.types import ASGIApp, Message, Receive, Scope, Send

    from fronta.config import Settings

BODY_LIMIT_MARGIN = 64 * 1024
"""Bytes allowed on top of the payload cap for the JSON envelope around the input."""

_STATUS_BY_ERROR: dict[type[Exception], int] = {
    UnknownTaskType: 404,
    TaskNotFound: 404,
    NotCancellable: 409,
    PayloadTooLarge: 413,
    InvalidInput: 422,
}


class BodyLimit:
    """Reject request bodies over `limit` bytes with 413 before the application parses them.

    A declared `Content-Length` over the limit is refused at once. A streamed body is counted as
    it arrives; past the limit the middleware answers 413 itself, reports a disconnect to the
    application (which then aborts its body parsing) and drops whatever the application sends.
    """

    def __init__(self, app: ASGIApp, limit: int) -> None:
        self.app = app
        self.limit = limit

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers", []))
        declared = headers.get(b"content-length")
        if declared is not None and declared.isdigit() and int(declared) > self.limit:
            await _reply(send, 413, "request body too large")
            return
        seen = 0
        rejected = False

        async def limited_receive() -> Message:
            nonlocal seen, rejected
            message = await receive()
            if message["type"] == "http.request":
                seen += len(message.get("body", b""))
                if seen > self.limit and not rejected:
                    rejected = True
                    await _reply(send, 413, "request body too large")
                    return {"type": "http.disconnect"}
            return message

        async def guarded_send(message: Message) -> None:
            if not rejected:
                await send(message)

        try:
            await self.app(scope, limited_receive, guarded_send)
        except Exception:
            if not rejected:
                raise


class BearerGate:
    """Bearer check for a mounted ASGI app (the MCP endpoint)."""

    def __init__(self, app: ASGIApp, token: str | None) -> None:
        self.app = app
        self.token = token

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            headers = dict(scope.get("headers", []))
            authorization = headers.get(b"authorization")
            value = None if authorization is None else authorization.decode("latin-1")
            if not bearer_ok(value, self.token):
                await _reply(
                    send,
                    401,
                    "missing or invalid bearer token",
                    headers={"WWW-Authenticate": "Bearer"},
                )
                return
        await self.app(scope, receive, send)


async def _reply(
    send: Send, status: int, detail: str, *, headers: Mapping[str, str] | None = None
) -> None:
    body = JSONResponse({"detail": detail}, status_code=status, headers=headers)
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": list(body.raw_headers),
        }
    )
    await send({"type": "http.response.body", "body": body.body})


def create_app(settings: Settings) -> FastAPI:
    if settings.server_token is None:
        # Never open, not even on loopback: a page in a browser on the same host could enqueue
        # or cancel tasks with a cross-origin request; a bearer header cannot be forged that way.
        msg = "FRONTA_SERVER_TOKEN is required: the server does not run without authentication"
        raise ConfigurationError(msg)
    service = Service(settings)
    mcp = make_mcp(service)

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await service.start()
        try:
            async with mcp.session_manager.run():
                yield
        finally:
            await service.stop()

    app = FastAPI(title="Fronta", version="1", lifespan=lifespan, docs_url="/api/docs")
    app.state.settings = settings
    app.state.service = service
    app.include_router(router)

    for error, status in _STATUS_BY_ERROR.items():
        app.add_exception_handler(error, _handler(status))

    package = resources.files("fronta.server")
    page = (package / "static" / "index.html").read_text(encoding="utf-8")
    app.mount("/static", StaticFiles(directory=str(package / "static")), name="static")

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def dashboard() -> HTMLResponse:
        return HTMLResponse(page)

    # The MCP app answers exactly `/mcp` (a `Mount("/mcp")` would redirect to `/mcp/`, which
    # MCP clients do not follow), so it is the root fall-through behind the bearer gate.
    mcp_app = mcp.streamable_http_app(
        streamable_http_path="/mcp",
        max_request_body_size=settings.payload_cap + BODY_LIMIT_MARGIN,
        host=settings.server_host,
    )
    app.mount("/", BearerGate(mcp_app, settings.server_token), name="mcp")
    app.add_middleware(BodyLimit, limit=settings.payload_cap + BODY_LIMIT_MARGIN)
    return app


def _handler(status: int) -> Callable[[Request, Exception], Awaitable[JSONResponse]]:
    async def handle(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=status)

    return handle


def serve(settings: Settings) -> None:
    """Run the server with uvicorn until SIGTERM/SIGINT."""
    uvicorn.run(
        create_app(settings),
        host=settings.server_host,
        port=settings.server_port,
        log_config=None,
        timeout_graceful_shutdown=int(settings.grace_s),
    )
