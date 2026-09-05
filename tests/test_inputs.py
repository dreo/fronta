"""Valid inputs survive the queue round trip: aliases, strict types, `Json[T]`, nested models.

Stored inputs are alias-shaped JSON (what the published validation schema describes); workers
validate them in JSON mode accepting aliases and field names; inputs that cannot round-trip are
refused at enqueue instead of failing the task at claim.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    Json,
    ValidationError,
    field_serializer,
)

from fronta import InvalidInput, Settings, State, Worker, store, task
from fronta.definitions import dump_input
from fronta.model import NewTask
from fronta.server import create_app
from tests.conftest import FAST, free_port, serve, wait_until

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

TOKEN = "inputs-token"  # noqa: S105  # test fixture value
RECEIVED: list[BaseModel] = []


class Strict(BaseModel):
    model_config = ConfigDict(strict=True)

    when: datetime
    ident: UUID
    count: int


class Aliased(BaseModel):
    user_id: int = Field(alias="userId")
    note: str = "n/a"


class Choices(BaseModel):
    name: str = Field(validation_alias=AliasChoices("name", "fullName"))
    level: int = 1


class Wrapped(BaseModel):
    raw: Json[dict[str, int]]
    inner: Aliased
    tags: list[str] = []


class Twisted(BaseModel):
    """Serializes `value` into a shape its own validation rejects: cannot round-trip."""

    value: int

    @field_serializer("value")
    def _twist(self, value: int) -> str:
        return f"v={value}"


@task("strict_in", input=Strict, attempt_timeout=30)
async def strict_in(ctx: Any, inp: Strict) -> dict[str, Any]:
    del ctx
    RECEIVED.append(inp)
    return {"when": inp.when.isoformat(), "ident": str(inp.ident), "count": inp.count}


@task("aliased_in", input=Aliased, attempt_timeout=30)
async def aliased_in(ctx: Any, inp: Aliased) -> dict[str, Any]:
    del ctx
    RECEIVED.append(inp)
    return {"user_id": inp.user_id, "note": inp.note}


@task("choices_in", input=Choices, attempt_timeout=30)
async def choices_in(ctx: Any, inp: Choices) -> str:
    del ctx
    RECEIVED.append(inp)
    return inp.name


@task("wrapped_in", input=Wrapped, attempt_timeout=30)
async def wrapped_in(ctx: Any, inp: Wrapped) -> dict[str, Any]:
    del ctx
    RECEIVED.append(inp)
    return {"raw": inp.raw, "user_id": inp.inner.user_id, "tags": inp.tags}


@task("twisted_in", input=Twisted, attempt_timeout=30)
async def twisted_in(ctx: Any, inp: Twisted) -> int:
    del ctx
    return inp.value


ALL = [strict_in, aliased_in, choices_in, wrapped_in, twisted_in]


async def _settled(conn, task_id, timeout=20):
    async def done():
        row = await store.get_task(conn, task_id)
        return row is not None and row.state in (State.SUCCEEDED, State.FAILED)

    await wait_until(done, timeout=timeout)
    return await store.get_task(conn, task_id)


@pytest.mark.usefixtures("sdk")
async def test_strict_aliased_and_wrapped_inputs_reach_the_handler_intact(
    conn, settings, run_worker
):
    RECEIVED.clear()
    when = datetime(2030, 1, 2, 3, 4, 5, tzinfo=UTC)
    ident = UUID("12345678-1234-5678-1234-567812345678")
    originals = [
        Strict(when=when, ident=ident, count=3),
        Aliased(userId=7),
        Choices(fullName="Ada"),
        Wrapped(raw='{"a": 1}', inner=Aliased(userId=9, note="x"), tags=["t"]),
    ]
    async with run_worker(Worker(ALL, settings=settings)):
        ids = [
            await strict_in.enqueue(originals[0]),
            await aliased_in.enqueue(originals[1]),
            await choices_in.enqueue(originals[2]),
            await wrapped_in.enqueue(originals[3]),
        ]
        rows = [await _settled(conn, task_id) for task_id in ids]
    assert [row.state for row in rows] == [State.SUCCEEDED] * 4, [row.error for row in rows]
    by_model = {type(model).__name__: model for model in RECEIVED}  # handlers ran concurrently
    assert by_model == {type(model).__name__: model for model in originals}
    assert rows[0].result == {"when": when.isoformat(), "ident": str(ident), "count": 3}
    assert rows[1].input == {"userId": 7, "note": "n/a"}  # stored by alias, like the schema
    assert json.loads(rows[3].input["raw"]) == {"a": 1}  # Json[T] stays JSON text when stored
    assert rows[3].result == {"raw": {"a": 1}, "user_id": 9, "tags": ["t"]}


@pytest.mark.usefixtures("sdk")
async def test_a_legacy_row_stored_by_field_name_still_validates(conn, settings, run_worker):
    RECEIVED.clear()
    async with run_worker(Worker([aliased_in], settings=settings)):
        legacy = await store.enqueue(
            conn, NewTask("aliased_in", '{"user_id": 5}', aliased_in.policy)
        )
        row = await _settled(conn, legacy)
    assert row.state is State.SUCCEEDED, row.error
    assert [Aliased(userId=5)] == RECEIVED


@pytest.mark.usefixtures("sdk")
async def test_dict_inputs_accept_aliases_and_field_names(conn, settings, run_worker):
    RECEIVED.clear()
    async with run_worker(Worker([aliased_in], settings=settings)):
        by_alias = await aliased_in.enqueue({"userId": 1})
        rows = [await _settled(conn, by_alias)]
        with pytest.raises(ValidationError):  # pydantic's own rule: dict input by alias only
            await aliased_in.enqueue({"user_id": 2})
    assert rows[0].state is State.SUCCEEDED


@pytest.mark.usefixtures("sdk")
async def test_inputs_that_cannot_round_trip_are_rejected_at_enqueue(conn):
    await store.publish_task_type(conn, twisted_in.spec)
    with pytest.raises(InvalidInput, match="round trip"):
        await twisted_in.enqueue(Twisted(value=1))
    cur = await conn.execute("SELECT count(*) FROM fronta.tasks")
    assert (await cur.fetchone())[0] == 0


@pytest.mark.usefixtures("sdk")
async def test_invalid_values_are_still_rejected(conn, settings, run_worker):
    with pytest.raises(ValidationError):
        await strict_in.enqueue({"when": "2030-01-02T03:04:05Z", "ident": "nope", "count": "1"})
    async with run_worker(Worker([strict_in], settings=settings)):
        # A row that bypassed enqueue validation fails at claim, without retry, as before.
        bad = await store.enqueue(conn, NewTask("strict_in", '{"when": 1}', strict_in.policy))
        row = await _settled(conn, bad)
    assert row.state is State.FAILED
    assert row.error["type"] == "InputValidationError"
    assert row.attempt == 1


def test_the_process_stdin_representation_is_the_stored_one():
    """Process tasks receive `dump_input` on stdin: the same alias-shaped, round-trippable JSON."""
    model = Wrapped(raw='{"a": 1}', inner=Aliased(userId=9), tags=[])
    assert dump_input(model) == {
        "raw": '{"a":1}',  # round-trip mode re-serializes the parsed JSON text
        "inner": {"userId": 9, "note": "n/a"},
        "tags": [],
    }
    assert dump_input(
        Strict(when=datetime(2030, 1, 1, tzinfo=UTC), ident=UUID(int=1), count=1)
    ) == {
        "when": "2030-01-01T00:00:00Z",
        "ident": "00000000-0000-0000-0000-000000000001",
        "count": 1,
    }


@pytest_asyncio.fixture
async def api(dsn) -> AsyncIterator[httpx.AsyncClient]:
    port = free_port()
    settings = Settings(dsn=dsn, **{**FAST, "server_token": TOKEN, "server_port": port})
    async for base in serve(create_app(settings), port):
        async with httpx.AsyncClient(
            base_url=base + "/api/v1", headers={"Authorization": f"Bearer {TOKEN}"}
        ) as client:
            yield client


@pytest.mark.usefixtures("sdk")
async def test_rest_inputs_in_the_published_shape_reach_the_handler(
    conn, settings, run_worker, api
):
    RECEIVED.clear()
    async with run_worker(Worker([strict_in, aliased_in], settings=settings)):
        strict = await api.post(
            "/tasks",
            json={
                "type": "strict_in",
                "input": {
                    "when": "2030-01-02T03:04:05Z",
                    "ident": "12345678-1234-5678-1234-567812345678",
                    "count": 3,
                },
            },
        )
        assert strict.status_code == 201, strict.text
        aliased = await api.post("/tasks", json={"type": "aliased_in", "input": {"userId": 7}})
        assert aliased.status_code == 201, aliased.text
        rows = [
            await _settled(conn, strict.json()["id"]),
            await _settled(conn, aliased.json()["id"]),
        ]
        wrong = await api.post("/tasks", json={"type": "aliased_in", "input": {"user_id": 7}})
        assert wrong.status_code == 422  # the published schema is by alias
    assert [row.state for row in rows] == [State.SUCCEEDED] * 2, [row.error for row in rows]
    assert {type(model).__name__: model for model in RECEIVED} == {
        "Strict": Strict(
            when=datetime(2030, 1, 2, 3, 4, 5, tzinfo=UTC),
            ident=UUID("12345678-1234-5678-1234-567812345678"),
            count=3,
        ),
        "Aliased": Aliased(userId=7),
    }
