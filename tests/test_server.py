"""REST, MCP and dashboard: every operation and error code, auth, pagination, body limits."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import httpx
import pytest
import pytest_asyncio
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from fronta import Settings, State, Worker, store
from fronta.model import NewTask
from fronta.server import create_app
from tests.conftest import FAST, free_port, serve, wait_until
from tests.workers import sleep_task, timed_task

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

TOKEN = "s3cret"  # noqa: S105  # test fixture value
FAST_LIMIT = 1024 * 1024 + 64 * 1024  # payload cap + the envelope margin
INITIALIZE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "t", "version": "0"},
    },
}


@pytest_asyncio.fixture
async def server(dsn) -> AsyncIterator[str]:
    port = free_port()
    settings = Settings(
        dsn=dsn,
        **{
            **FAST,
            "server_token": TOKEN,
            "server_port": port,
            "list_page_size": 3,
            "list_page_max": 5,
        },
    )
    async for base in serve(create_app(settings), port):
        yield base


@pytest_asyncio.fixture
async def api(server) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=server + "/api/v1", headers={"Authorization": f"Bearer {TOKEN}"}
    ) as client:
        yield client


@pytest_asyncio.fixture
async def published(conn):
    await store.publish_task_type(conn, sleep_task.spec)


@pytest.mark.usefixtures("published")
async def test_requests_without_a_valid_token_get_401(server):
    async with httpx.AsyncClient(base_url=server) as anon:
        for method, path in [
            ("GET", "/api/v1/task-types"),
            ("POST", "/api/v1/tasks"),
            ("GET", "/api/v1/tasks"),
            ("GET", "/api/v1/tasks/1"),
            ("POST", "/api/v1/tasks/1/cancel"),
        ]:
            response = await anon.request(method, path, json={})
            assert response.status_code == 401, path
            assert response.headers["www-authenticate"] == "Bearer"
        wrong = await anon.get("/api/v1/task-types", headers={"Authorization": "Bearer nope"})
        assert wrong.status_code == 401
        basic = await anon.get("/api/v1/task-types", headers={"Authorization": f"Basic {TOKEN}"})
        assert basic.status_code == 401
        # The dashboard and its assets stay public.
        assert (await anon.get("/")).status_code == 200
        assert (await anon.get("/static/alpine.min.js")).status_code == 200


@pytest.mark.usefixtures("published")
async def test_list_task_types_returns_the_published_definitions(api):
    response = await api.get("/task-types")
    assert response.status_code == 200
    (row,) = response.json()
    assert row["name"] == "sleep"
    assert row["executor"] == "asyncio"
    assert row["input_schema"]["properties"]["n"]["type"] == "integer"
    assert row["output_schema"]["properties"]["n"]["type"] == "integer"
    assert row["policy"]["max_attempts"] == 3
    assert row["fingerprint"] == sleep_task.spec.fingerprint


@pytest.mark.usefixtures("published")
async def test_enqueue_get_and_cancel_through_rest(api, conn):
    created = await api.post(
        "/tasks",
        json={
            "type": "sleep",
            "input": {"n": 5},
            "priority": 2,
            "key": "k",
            "concurrency_key": "c",
        },
    )
    assert created.status_code == 201
    task_id = created.json()["id"]
    got = await api.get(f"/tasks/{task_id}")
    assert got.status_code == 200
    body = got.json()
    assert body["state"] == "queued"
    assert body["input"] == {"n": 5}  # server-side enqueue stores the JSON as given
    assert body["priority"] == 2
    assert body["key"] == "k"
    assert body["concurrency_key"] == "c"
    assert body["max_attempts"] == 3
    assert body["token"] is None
    assert body["result"] is None
    duplicate = await api.post("/tasks", json={"type": "sleep", "input": {"n": 6}, "key": "k"})
    assert duplicate.status_code == 201
    assert duplicate.json()["id"] == task_id
    cancelled = await api.post(f"/tasks/{task_id}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json() == {"id": task_id, "state": "cancelled"}
    again = await api.post(f"/tasks/{task_id}/cancel")
    assert again.status_code == 409
    assert (await store.get_task(conn, task_id)).state is State.CANCELLED


@pytest.mark.usefixtures("published")
async def test_enqueue_validates_against_the_published_schema(api):
    bad = await api.post("/tasks", json={"type": "sleep", "input": {"n": "text"}})
    assert bad.status_code == 422
    assert "n: 'text' is not of type 'integer'" in bad.json()["detail"]
    not_object = await api.post("/tasks", json={"type": "sleep", "input": [1]})
    assert not_object.status_code == 422
    naive = await api.post(
        "/tasks", json={"type": "sleep", "input": {}, "run_at": "2030-01-01T00:00:00"}
    )
    assert naive.status_code == 422
    unknown = await api.post("/tasks", json={"type": "nope", "input": {}})
    assert unknown.status_code == 404
    long_key = await api.post("/tasks", json={"type": "sleep", "input": {}, "key": "k" * 1025})
    assert long_key.status_code == 422
    nul = await api.post("/tasks", json={"type": "sleep", "input": {"key": "a\x00b"}})
    assert nul.status_code == 422
    aware = await api.post(
        "/tasks",
        json={
            "type": "sleep",
            "input": {},
            "run_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        },
    )
    assert aware.status_code == 201


@pytest.mark.usefixtures("published")
async def test_unknown_task_ids_are_404(api):
    assert (await api.get("/tasks/424242")).status_code == 404
    assert (await api.post("/tasks/424242/cancel")).status_code == 404


@pytest.mark.usefixtures("published")
async def test_oversized_input_and_bodies_are_413(api):
    over_cap = await api.post(
        "/tasks", json={"type": "sleep", "input": {"key": "x" * (1024 * 1024 + 1)}}
    )
    assert over_cap.status_code == 413
    assert "cap" in over_cap.json()["detail"]
    huge = await api.post(
        "/tasks",
        content=b"{" + b" " * (3 * 1024 * 1024) + b"}",
        headers={"Content-Type": "application/json"},
    )
    assert huge.status_code == 413
    chunked = await api.post(  # an async generator body is sent chunked, without Content-Length
        "/tasks", content=_chunks(3 * 1024 * 1024), headers={"Content-Type": "application/json"}
    )
    assert chunked.status_code == 413


async def _chunks(total, size=65536):
    sent = 0
    while sent < total:
        yield b" " * min(size, total - sent)
        sent += size


@pytest.mark.usefixtures("published")
async def test_list_returns_summaries_with_keyset_pagination_and_filters(api, conn):
    ids = [
        await store.enqueue(conn, NewTask("sleep", "{}", sleep_task.policy, key=f"k{i}"))
        for i in range(7)
    ]
    page = await api.get("/tasks")
    assert page.status_code == 200
    body = page.json()
    assert [t["id"] for t in body["items"]] == ids[::-1][:3]  # newest first, page size 3
    assert body["next"] == ids[-3]
    assert "input" not in body["items"][0]
    assert "result" not in body["items"][0]
    assert set(body["items"][0]) >= {
        "id",
        "type",
        "state",
        "priority",
        "key",
        "attempt",
        "failures",
        "created_at",
        "run_at",
    }
    second = (await api.get("/tasks", params={"before": body["next"]})).json()
    assert [t["id"] for t in second["items"]] == ids[::-1][3:6]
    third = (await api.get("/tasks", params={"before": second["next"]})).json()
    assert [t["id"] for t in third["items"]] == ids[:1]
    assert third["next"] is None
    capped = (await api.get("/tasks", params={"limit": 100})).json()
    assert len(capped["items"]) == 5  # list_page_max
    clamped = (await api.get("/tasks", params={"limit": 0})).json()
    assert [t["id"] for t in clamped["items"]] == ids[-1:]
    assert clamped["next"] == ids[-1]
    by_key = (await api.get("/tasks", params={"key": "k2"})).json()
    assert [t["id"] for t in by_key["items"]] == [ids[2]]
    by_state = (await api.get("/tasks", params={"state": "running"})).json()
    assert by_state["items"] == []
    by_type = (await api.get("/tasks", params={"type": "other"})).json()
    assert by_type["items"] == []
    bad_state = await api.get("/tasks", params={"state": "bogus"})
    assert bad_state.status_code == 422


@pytest.mark.usefixtures("published")
async def test_rest_reflects_a_running_workers_results(api, settings, run_worker):
    async with run_worker(Worker([sleep_task], settings=settings)):
        created = await api.post("/tasks", json={"type": "sleep", "input": {"n": 9}})
        task_id = created.json()["id"]
        await wait_until(lambda: _done(api, task_id))
    body = (await api.get(f"/tasks/{task_id}")).json()
    assert body["state"] == "succeeded"
    assert body["result"]["n"] == 9
    assert body["attempt"] == 1
    assert body["worker"]


async def _done(api, task_id):
    return (await api.get(f"/tasks/{task_id}")).json()["state"] == "succeeded"


@pytest.mark.usefixtures("published")
async def test_mcp_tools_mirror_the_rest_operations(server, conn):
    async with (
        httpx.AsyncClient(headers={"Authorization": f"Bearer {TOKEN}"}) as http,
        streamable_http_client(server + "/mcp", http_client=http) as streams,
        ClientSession(streams[0], streams[1]) as session,
    ):
        await session.initialize()
        tools = await session.list_tools()
        assert [t.name for t in tools.tools] == [
            "list_task_types",
            "enqueue",
            "get_task",
            "list_tasks",
            "cancel",
        ]
        types = await session.call_tool("list_task_types", {})
        assert not types.is_error
        assert types.structured_content["result"][0]["name"] == "sleep"
        created = await session.call_tool(
            "enqueue", {"type": "sleep", "input": {"n": 1}, "key": "mcp"}
        )
        assert not created.is_error
        task_id = created.structured_content["id"]
        duplicate = await session.call_tool(
            "enqueue", {"type": "sleep", "input": {"n": 2}, "key": "mcp"}
        )
        assert duplicate.structured_content["id"] == task_id
        got = await session.call_tool("get_task", {"id": task_id})
        assert got.structured_content["state"] == "queued"
        assert got.structured_content["input"] == {"n": 1}
        listed = await session.call_tool("list_tasks", {"state": "queued"})
        assert [t["id"] for t in listed.structured_content["items"]] == [task_id]
        clamped = await session.call_tool("list_tasks", {"state": "queued", "limit": 0})
        assert [t["id"] for t in clamped.structured_content["items"]] == [task_id]
        assert clamped.structured_content["next"] == task_id
        cancelled = await session.call_tool("cancel", {"id": task_id})
        assert cancelled.structured_content == {"id": task_id, "state": "cancelled"}
        for name, args, text in [
            ("cancel", {"id": task_id}, "NotCancellable"),
            ("cancel", {"id": 424242}, "TaskNotFound"),
            ("get_task", {"id": 424242}, "TaskNotFound"),
            ("enqueue", {"type": "nope", "input": {}}, "UnknownTaskType"),
            ("enqueue", {"type": "sleep", "input": {"n": "x"}}, "InvalidInput"),
            (
                "enqueue",
                {"type": "sleep", "input": {}, "run_at": "2030-01-01T00:00:00"},
                "timezone",
            ),
            ("list_tasks", {"state": "bogus"}, "invalid state"),
        ]:
            result = await session.call_tool(name, args)
            assert result.is_error, name
            assert text in result.content[0].text
    assert (await store.get_task(conn, task_id)).state is State.CANCELLED


@pytest.mark.usefixtures("published")
async def test_mcp_without_a_token_is_rejected(server):
    async with httpx.AsyncClient() as http:
        response = await http.post(
            server + "/mcp",
            json=INITIALIZE,
            headers={"Accept": "application/json, text/event-stream"},
        )
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


async def test_dashboard_serves_the_shell_and_alpine(server):
    async with httpx.AsyncClient(base_url=server) as anon:
        page = await anon.get("/")
        assert page.status_code == 200
        assert "text/html" in page.headers["content-type"]
        assert 'x-data="dashboard()"' in page.text
        assert 'data-testid="token-input"' in page.text
        assert "authRequired" not in page.text
        assert "/api/v1/tasks" in page.text
        alpine = await anon.get("/static/alpine.min.js")
        assert alpine.status_code == 200
        assert "javascript" in alpine.headers["content-type"]
        assert len(alpine.content) > 10000


async def test_enqueue_enforces_json_schema_formats(api, conn):
    await store.publish_task_type(conn, timed_task.spec)
    bad = await api.post("/tasks", json={"type": "timed", "input": {"at": "not-a-date"}})
    assert bad.status_code == 422
    assert "date-time" in bad.json()["detail"]
    good = await api.post("/tasks", json={"type": "timed", "input": {"at": "2030-01-02T03:04:05Z"}})
    assert good.status_code == 201


@pytest.mark.usefixtures("published")
async def test_body_limit_boundary_is_exact(api):
    limit = FAST_LIMIT
    prefix = b'{"type": "sleep", "input": {}'
    at_limit = prefix + b" " * (limit - len(prefix) - 1) + b"}"
    assert len(at_limit) == limit
    json_type = {"Content-Type": "application/json"}
    accepted = await api.post("/tasks", content=at_limit, headers=json_type)
    assert accepted.status_code == 201
    over = prefix + b" " * (limit - len(prefix)) + b"}"
    assert len(over) == limit + 1
    rejected = await api.post("/tasks", content=over, headers=json_type)
    assert rejected.status_code == 413


def test_the_server_never_runs_without_a_token(dsn):
    from fronta import ConfigurationError  # noqa: PLC0415
    from fronta.server import create_app  # noqa: PLC0415
    from fronta.server.api import bearer_ok  # noqa: PLC0415

    with pytest.raises(ConfigurationError, match="FRONTA_SERVER_TOKEN"):
        create_app(Settings(dsn=dsn, **FAST))
    assert not bearer_ok(None, None)
    assert not bearer_ok("Bearer anything", None)
