"""The dashboard in a real browser: token prompt, list, detail, cancel, enqueue, task types.

Needs Playwright with a Chromium build (`uv run playwright install chromium`); skipped otherwise.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
import uvicorn

from fronta import Settings, Worker
from fronta.server import create_app
from tests.conftest import FAST, free_port, wait_until
from tests.workers import progress_task, sleep_task

playwright = pytest.importorskip("playwright.async_api")

TOKEN = "dash-token"  # noqa: S105  # test fixture value
TASKS = "[data-testid=tasks] tbody tr"
DETAIL = "[data-testid=task-detail]"


async def test_dashboard_workflow_in_a_browser(dsn, settings, run_worker):
    port = free_port()
    app = create_app(Settings(dsn=dsn, **{**FAST, "server_token": TOKEN, "server_port": port}))
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    serving = asyncio.create_task(server.serve())
    base = f"http://127.0.0.1:{port}"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    try:
        async with httpx.AsyncClient(base_url=base, headers=headers) as api:
            await wait_until(lambda: _up(api), timeout=10)
            async with run_worker(Worker([sleep_task, progress_task], settings=settings)):
                await wait_until(lambda: _types(api, 2), timeout=10)
                done = await _enqueue(api, "progress", {"n": 9})
                running = await _enqueue(api, "sleep", {"sleep_s": 60})
                await wait_until(lambda: _state(api, done, "succeeded"), timeout=15)
                await wait_until(lambda: _state(api, running, "running"), timeout=15)
                async with playwright.async_playwright() as p:
                    try:
                        browser = await p.chromium.launch(headless=True)
                    except Exception as exc:  # any launch failure: no browser here
                        pytest.skip(f"no Chromium for Playwright: {exc}")
                    page = await browser.new_page(viewport={"width": 1280, "height": 900})
                    await _sign_in(page, base)
                    await _inspect(page, done)
                    await _cancel(page, running)
                    await _enqueue_from_the_form(page)
                    await page.click("nav >> text=Task types")
                    await page.wait_for_function(
                        "document.querySelectorAll('[data-testid=task-types] tbody tr')"
                        ".length === 2"
                    )
                    await browser.close()
                listing = (await api.get("/api/v1/tasks", params={"state": "cancelled"})).json()
                assert [t["id"] for t in listing["items"]] == [running]
                newest = (await api.get("/api/v1/tasks", params={"type": "sleep"})).json()
                form_task = newest["items"][0]["id"]
                assert form_task > running  # the task enqueued from the form
                await wait_until(lambda: _state(api, form_task, "succeeded"), timeout=15)
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, 20)


async def _sign_in(page, base):
    await page.goto(base + "/")
    await page.wait_for_selector("[data-testid=token-input]")
    await page.wait_for_function("document.querySelector('p.error').textContent.includes('401')")
    await page.fill("[data-testid=token-input]", TOKEN)
    await page.click("text=Use token")
    await page.wait_for_function(f"document.querySelectorAll('{TASKS}').length >= 2")


async def _open(page, task_id):
    await page.locator(f"{TASKS} td:first-child").get_by_text(str(task_id), exact=True).click()
    await page.wait_for_function(
        f"document.querySelector('{DETAIL} strong').textContent.includes('{task_id}')"
    )
    return page.locator(DETAIL)


async def _inspect(page, done):
    detail = await _open(page, done)
    assert "succeeded" in await detail.locator("span.state").first.inner_text()
    assert '"done": true' in await detail.locator("pre").nth(1).inner_text()
    assert '"step": 2' in await detail.locator("pre").nth(3).inner_text()


async def _cancel(page, running):
    await _open(page, running)
    await page.click(f"{DETAIL} button.danger")
    await page.wait_for_function(
        f"document.querySelector('{DETAIL} span.state').textContent.trim() === 'cancelled'",
        timeout=15000,
    )


async def _enqueue_from_the_form(page):
    await page.click("nav >> text=Enqueue")
    await page.select_option("[data-testid=enqueue-type]", "sleep")
    await page.wait_for_function(
        "document.querySelector('[data-testid=enqueue-input]').value.includes('\"n\"')"
    )
    await page.fill("[data-testid=enqueue-input]", '{"n": 42}')
    await page.click("button.primary")
    await page.wait_for_function("document.body.innerText.includes('enqueued task')")
    await page.fill("[data-testid=enqueue-input]", "{not json")
    await page.click("button.primary")
    await page.wait_for_function("document.body.innerText.includes('not valid JSON')")


async def _enqueue(api, task_type, payload):
    response = await api.post("/api/v1/tasks", json={"type": task_type, "input": payload})
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def _up(api):
    try:
        return (await api.get("/")).status_code == 200
    except httpx.HTTPError:
        return False


async def _types(api, count):
    return len((await api.get("/api/v1/task-types")).json()) >= count


async def _state(api, task_id, state):
    return (await api.get(f"/api/v1/tasks/{task_id}")).json()["state"] == state
