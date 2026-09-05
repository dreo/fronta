"""The dashboard in a real browser: token prompt, list, detail, cancel, enqueue, task types;
request hygiene (one initialization, stale responses discarded, one request per submit, polling
scoped to the selection); keyboard operability and a 375px viewport.

Needs Playwright with a Chromium build (`uv run playwright install chromium`); skipped otherwise.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import pytest_asyncio
import uvicorn

from fronta import Settings, Worker
from fronta.server import create_app
from tests.conftest import FAST, free_port, wait_until
from tests.workers import progress_task, sleep_task

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

playwright = pytest.importorskip("playwright.async_api")

TOKEN = "dash-token"  # noqa: S105  # test fixture value
TASKS = "[data-testid=tasks] tbody tr"
DETAIL = "[data-testid=task-detail]"


@dataclass
class Dash:
    api: httpx.AsyncClient
    base: str
    browser: Any

    async def page(self, width: int = 1280, height: int = 900) -> Any:
        return await self.browser.new_page(viewport={"width": width, "height": height})

    async def enqueue(self, task_type: str, payload: dict[str, Any], **extra: Any) -> int:
        body = {"type": task_type, "input": payload, **extra}
        response = await self.api.post("/api/v1/tasks", json=body)
        assert response.status_code == 201, response.text
        return int(response.json()["id"])

    async def state(self, task_id: int) -> str:
        return str((await self.api.get(f"/api/v1/tasks/{task_id}")).json()["state"])


@pytest_asyncio.fixture
async def dash(dsn, settings, run_worker) -> AsyncIterator[Dash]:
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
                async with playwright.async_playwright() as p:
                    try:
                        browser = await p.chromium.launch(headless=True)
                    except Exception as exc:  # any launch failure: no browser here
                        pytest.skip(f"no Chromium for Playwright: {exc}")
                    try:
                        yield Dash(api, base, browser)
                    finally:
                        await browser.close()
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, 20)


async def test_dashboard_workflow_in_a_browser(dash):
    done = await dash.enqueue("progress", {"n": 9})
    running = await dash.enqueue("sleep", {"sleep_s": 60})
    await wait_until(lambda: _state(dash, done, "succeeded"), timeout=15)
    await wait_until(lambda: _state(dash, running, "running"), timeout=15)
    page = await dash.page()
    await _sign_in(page, dash.base)
    await _inspect(page, done)
    await _cancel(page, running)
    await _enqueue_from_the_form(page)
    await page.click("nav >> text=Task types")
    await page.wait_for_function(
        "document.querySelectorAll('[data-testid=task-types] tbody tr').length === 2"
    )
    listing = (await dash.api.get("/api/v1/tasks", params={"state": "cancelled"})).json()
    assert [t["id"] for t in listing["items"]] == [running]
    newest = (await dash.api.get("/api/v1/tasks", params={"type": "sleep"})).json()
    form_task = newest["items"][0]["id"]
    assert form_task > running  # the task enqueued from the form
    await wait_until(lambda: _state(dash, form_task, "succeeded"), timeout=15)


async def test_one_initialization_and_no_stale_list_responses(dash):
    first = await dash.enqueue("progress", {"n": 1})
    second = await dash.enqueue("progress", {"n": 2})
    await wait_until(lambda: _state(dash, second, "succeeded"), timeout=15)
    page = await dash.page()
    requests: list[str] = []
    page.on("request", lambda request: requests.append(request.url))
    await page.goto(dash.base + "/")
    await page.wait_for_function("document.querySelector('p.error').textContent.includes('401')")
    await asyncio.sleep(0.5)  # a duplicated init would have fired its second round by now
    assert sum("/api/v1/task-types" in url for url in requests) == 1
    assert sum("/api/v1/tasks?" in url for url in requests) == 1

    await _sign_in(page, dash.base, expected_rows=2)

    async def delay_failed_filter(route: Any) -> None:
        if "state=failed" in route.request.url:
            await asyncio.sleep(0.8)  # this response arrives after the one requested later
        await route.continue_()

    await page.route("**/api/v1/tasks?**", delay_failed_filter)
    await page.select_option("#filter-state", "failed")
    await page.select_option("#filter-state", "")
    await asyncio.sleep(1.5)
    rows = await page.locator(TASKS).count()
    assert rows == 2, "the stale 'failed' response (no rows) must not overwrite the current list"
    assert await page.input_value("#filter-state") == ""
    assert first in [
        int(text) for text in await page.locator(f"{TASKS} td:first-child").all_inner_texts()
    ]


async def test_one_request_per_submit_and_polling_scoped_to_the_selection(dash):
    done = await dash.enqueue("progress", {"n": 3})
    running = await dash.enqueue("sleep", {"sleep_s": 60})
    await wait_until(lambda: _state(dash, done, "succeeded"), timeout=15)
    await wait_until(lambda: _state(dash, running, "running"), timeout=15)
    page = await dash.page()
    await _sign_in(page, dash.base, expected_rows=2)
    posts: list[str] = []
    page.on(
        "request",
        lambda request: (
            posts.append(request.url)
            if request.method == "POST" and request.url.endswith("/api/v1/tasks")
            else None
        ),
    )

    async def slow_enqueue(route: Any) -> None:
        if route.request.method == "POST":
            await asyncio.sleep(0.6)
        await route.continue_()

    await page.route("**/api/v1/tasks", slow_enqueue)
    await page.click("nav >> text=Enqueue")
    await page.select_option("[data-testid=enqueue-type]", "sleep")
    await page.fill("[data-testid=enqueue-input]", '{"n": 1}')
    await page.dblclick("button.primary")
    await page.wait_for_function("document.body.innerText.includes('enqueued task')")
    await asyncio.sleep(0.8)
    assert len(posts) == 1
    await page.unroute("**/api/v1/tasks")

    # Cancel the running task, then select another task while the cancel is still followed.
    await page.click("nav >> text=Tasks")
    await page.wait_for_function(f"document.querySelectorAll('{TASKS}').length >= 3")
    await _open(page, running)
    await page.click(f"{DETAIL} button.danger")
    await _open(page, done)
    await wait_until(lambda: _state(dash, running, "cancelled"), timeout=15)
    await asyncio.sleep(1.5)  # any polling of the cancelled task would have reopened it by now
    header = await page.locator(f"{DETAIL} strong").inner_text()
    assert str(done) in header
    assert str(running) not in header
    # Recovery after an error: an invalid submit, then a valid one.
    await page.click("nav >> text=Enqueue")
    await page.fill("[data-testid=enqueue-input]", "{not json")
    await page.click("button.primary")
    await page.wait_for_function("document.body.innerText.includes('not valid JSON')")
    await page.fill("[data-testid=enqueue-input]", '{"n": 2}')
    await page.click("button.primary")
    await page.wait_for_function("document.body.innerText.includes('enqueued task')")


async def test_keyboard_only_workflow_and_a_small_viewport(dash):
    done = await dash.enqueue("progress", {"n": 4}, key="k-" + "x" * 200)
    running = await dash.enqueue("sleep", {"sleep_s": 60})
    await wait_until(lambda: _state(dash, done, "succeeded"), timeout=15)
    await wait_until(lambda: _state(dash, running, "running"), timeout=15)
    page = await dash.page(width=375, height=667)
    await page.goto(dash.base + "/")
    await page.wait_for_selector("#token")
    assert await _page_width(page) <= 375
    # Sign in with the keyboard alone.
    await page.focus("#token")
    await page.keyboard.type(TOKEN)
    await page.keyboard.press("Enter")
    await page.wait_for_function(f"document.querySelectorAll('{TASKS}').length >= 2")
    assert await _page_width(page) <= 375  # a long key and a table do not widen the page
    # Every control has an accessible name.
    for label in ("API token", "Filter by type", "Filter by state", "Filter by key"):
        assert await page.get_by_label(label, exact=True).count() == 1, label
    # Open a task from the row's button, cancel it from the detail, all by keyboard.
    await page.get_by_role("button", name=f"open task {running}").focus()
    await page.keyboard.press("Enter")
    await page.wait_for_function(
        f"document.querySelector('{DETAIL} strong').textContent.includes('{running}')"
    )
    await page.get_by_role("button", name="Cancel").focus()
    await page.keyboard.press("Enter")
    await page.wait_for_function(
        f"document.querySelector('{DETAIL} span.state').textContent.trim() === 'cancelled'",
        timeout=15000,
    )
    # Enqueue from the form by keyboard: nav, type, input, submit.
    await page.locator("nav >> text=Enqueue").focus()
    await page.keyboard.press("Enter")
    for label in ("Task type", "Priority", "Dedupe key", "Concurrency key", "Input (JSON object)"):
        assert await page.get_by_label(label, exact=True).count() == 1, label
    await page.get_by_label("Task type", exact=True).select_option("sleep")
    await page.get_by_label("Input (JSON object)", exact=True).fill('{"n": 42}')
    await page.locator("button.primary").focus()
    await page.keyboard.press("Enter")
    await page.wait_for_function("document.body.innerText.includes('enqueued task')")
    assert await _page_width(page) <= 375


async def _page_width(page) -> int:
    return int(await page.evaluate("document.documentElement.scrollWidth"))


async def _sign_in(page, base, expected_rows=2):
    await page.goto(base + "/")
    await page.wait_for_selector("[data-testid=token-input]")
    await page.wait_for_function("document.querySelector('p.error').textContent.includes('401')")
    await page.fill("[data-testid=token-input]", TOKEN)
    await page.click("text=Use token")
    await page.wait_for_function(f"document.querySelectorAll('{TASKS}').length >= {expected_rows}")


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


async def _up(api):
    try:
        return (await api.get("/")).status_code == 200
    except httpx.HTTPError:
        return False


async def _types(api, count):
    return len((await api.get("/api/v1/task-types")).json()) >= count


async def _state(dash: Dash, task_id: int, state: str) -> bool:
    return await dash.state(task_id) == state
