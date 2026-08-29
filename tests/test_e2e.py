"""End to end through the real entrypoints: db init, worker, server; SDK, REST, MCP, dashboard."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from typing import TYPE_CHECKING

import httpx
import psycopg
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from tests.conftest import REPO, free_port, fronta_cli, wait_until, worker_env
from tests.workers import In, sleep_task

if TYPE_CHECKING:
    from collections.abc import Iterator

TOKEN = "e2e-token"  # noqa: S105  # test fixture value
LINUX = sys.platform == "linux"


@pytest.fixture
def stack(dsn, settings) -> Iterator[str]:
    """`fronta db init`, a worker and a server as real subprocesses; yields the server URL."""
    env = worker_env(settings, FRONTA_SERVER_TOKEN=TOKEN, FRONTA_DSN=dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA fronta CASCADE")
    init = subprocess.run(  # noqa: S603  # our own CLI, fixed argv
        [fronta_cli(), "db", "init"], env=env, capture_output=True, text=True, check=False
    )
    assert init.returncode == 0, init.stderr
    assert "ready" in init.stdout
    port = free_port()
    targets = ["tests.subworkers:e2e_worker"]
    if LINUX:
        targets.append("tests.subworkers:e2e_process_worker")
    workers = [
        subprocess.Popen(  # noqa: S603  # our own CLI, fixed argv
            [fronta_cli(), "worker", target], cwd=REPO, env=env, stderr=subprocess.PIPE
        )
        for target in targets
    ]
    server = subprocess.Popen(  # noqa: S603  # our own CLI, fixed argv
        [fronta_cli(), "server", "--port", str(port)], cwd=REPO, env=env, stderr=subprocess.PIPE
    )
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                httpx.get(base + "/", timeout=1)
                break
            except httpx.HTTPError:
                assert server.poll() is None, server.stderr.read().decode()
                assert time.monotonic() < deadline, "server did not start"
                time.sleep(0.1)
        yield base
    finally:
        for proc in (server, *workers):
            if proc.poll() is None:
                proc.terminate()
        for proc in (server, *workers):
            proc.wait(timeout=30)
        for worker in workers:
            assert worker.returncode == 0, worker.stderr.read().decode()


@pytest.mark.usefixtures("sdk")
async def test_end_to_end(stack):
    headers = {"Authorization": f"Bearer {TOKEN}"}
    async with httpx.AsyncClient(base_url=stack, headers=headers, timeout=10) as api:
        # The worker published its definitions.
        expected_types = {"sleep", "progress", "state"}
        if LINUX:
            expected_types.add("echo_proc")
        await wait_until(lambda: _has_types(api, expected_types), timeout=30)

        # Enqueue via the SDK (process-global pool from FRONTA_DSN), REST and MCP.
        sdk_id = await sleep_task.enqueue(In(n=1, sleep_s=0.2))
        rest = await api.post("/api/v1/tasks", json={"type": "state", "input": {"n": 2}})
        assert rest.status_code == 201
        rest_id = rest.json()["id"]
        process_id = await _enqueue_process(api)
        async with (
            httpx.AsyncClient(headers=headers) as http,
            streamable_http_client(stack + "/mcp", http_client=http) as streams,
            ClientSession(streams[0], streams[1]) as session,
        ):
            await session.initialize()
            created = await session.call_tool("enqueue", {"type": "progress", "input": {"n": 3}})
            assert not created.is_error
            mcp_id = created.structured_content["id"]

        # State and results via REST.
        task_ids = [sdk_id, rest_id, mcp_id]
        if process_id is not None:
            task_ids.append(process_id)
        for task_id in task_ids:
            await wait_until(lambda tid=task_id: _succeeded(api, tid), timeout=30)
        sdk_row = (await api.get(f"/api/v1/tasks/{sdk_id}")).json()
        assert sdk_row["result"]["n"] == 1
        state_row = (await api.get(f"/api/v1/tasks/{rest_id}")).json()
        assert state_row["result"] == {"resource": "ready", "cancelled": False}
        await _assert_process_result(api, process_id)
        progress_row = (await api.get(f"/api/v1/tasks/{mcp_id}")).json()
        assert progress_row["progress"] == {"step": 2, "n": 3}
        assert progress_row["result"] == {"done": True}

        # ... and via MCP.
        async with (
            httpx.AsyncClient(headers=headers) as http,
            streamable_http_client(stack + "/mcp", http_client=http) as streams,
            ClientSession(streams[0], streams[1]) as session,
        ):
            await session.initialize()
            got = await session.call_tool("get_task", {"id": sdk_id})
            assert got.structured_content["state"] == "succeeded"
            listed = await session.call_tool("list_tasks", {"state": "succeeded"})
            assert {t["id"] for t in listed.structured_content["items"]} >= set(task_ids)

        # Cancel a running task.
        running = await api.post("/api/v1/tasks", json={"type": "sleep", "input": {"sleep_s": 60}})
        running_id = running.json()["id"]
        await wait_until(lambda: _state_is(api, running_id, "running"), timeout=30)
        cancelled = await api.post(f"/api/v1/tasks/{running_id}/cancel")
        assert cancelled.json()["state"] == "running"
        await wait_until(lambda: _state_is(api, running_id, "cancelled"), timeout=30)

        # The dashboard is served and reads the same API.
        page = await api.get("/")
        assert page.status_code == 200
        assert "Fronta" in page.text
        listing = (await api.get("/api/v1/tasks", params={"state": "cancelled"})).json()
        assert [t["id"] for t in listing["items"]] == [running_id]

    async with httpx.AsyncClient(base_url=stack, timeout=10) as anonymous:
        assert (await anonymous.get("/api/v1/tasks")).status_code == 401
        assert (
            await anonymous.post("/api/v1/tasks", json={"type": "sleep", "input": {}})
        ).status_code == 401
        assert (await anonymous.get("/")).status_code == 200


async def _has_types(api, names):
    response = await api.get("/api/v1/task-types")
    return response.status_code == 200 and {t["name"] for t in response.json()} >= names


async def _enqueue_process(api):
    if not LINUX:
        return None
    response = await api.post("/api/v1/tasks", json={"type": "echo_proc", "input": {"n": 2}})
    assert response.status_code == 201
    return response.json()["id"]


async def _assert_process_result(api, task_id):
    if task_id is None:
        return
    row = (await api.get(f"/api/v1/tasks/{task_id}")).json()
    assert row["result"]["exit_code"] == 0
    assert '{"n":2' in row["result"]["stdout"]
    assert "FRONTA_DSN" not in row["result"]["stdout"]


async def _succeeded(api, task_id):
    return await _state_is(api, task_id, "succeeded")


async def _state_is(api, task_id, state):
    response = await api.get(f"/api/v1/tasks/{task_id}")
    return response.status_code == 200 and response.json()["state"] == state


def test_cli_help_lists_the_commands():
    result = subprocess.run(  # noqa: S603  # our own CLI, fixed argv
        [fronta_cli(), "--help"], capture_output=True, text=True, check=False, env={**os.environ}
    )
    assert result.returncode == 0
    for command in ("db", "worker", "server"):
        assert command in result.stdout


def test_worker_rejects_bad_targets(dsn):
    env = {**os.environ, "FRONTA_DSN": dsn}
    for target in (
        "nomodule",
        "nomodule:worker",
        "tests.subworkers:nothing",
        "tests.workers:sleep_task",
    ):
        result = subprocess.run(  # noqa: S603  # our own CLI, fixed argv
            [fronta_cli(), "worker", target],
            cwd=REPO,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode != 0, target
        assert "Error" in result.stderr or "Usage" in result.stderr
