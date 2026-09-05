"""Rolling out an incompatible task contract safely: a new task name, overlapping fleets.

Claims route by name, so an old worker never sees the new type's inputs, old work drains on the
old fleet, and a restart of the old worker cannot alter the new type's schema or limits.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from fronta import State, Worker, store, task
from tests.conftest import wait_until


class ThingV1(BaseModel):
    a: int


class ThingV2(BaseModel):
    a: int
    b: str  # a new required field: incompatible with v1 inputs


@task("thing", input=ThingV1, attempt_timeout=30)
async def thing_v1(ctx: Any, inp: ThingV1) -> dict[str, Any]:
    del ctx
    return {"v": 1, "a": inp.a}


@task("thing_v2", input=ThingV2, attempt_timeout=30, max_concurrency=3)
async def thing_v2(ctx: Any, inp: ThingV2) -> dict[str, Any]:
    del ctx
    return {"v": 2, "a": inp.a, "b": inp.b}


async def _all_succeeded(conn, ids):
    cur = await conn.execute(
        "SELECT count(*) FROM fronta.tasks WHERE id = ANY(%s) AND state = 'succeeded'", (ids,)
    )
    return (await cur.fetchone())[0] == len(ids)


@pytest.mark.usefixtures("sdk")
async def test_versioned_names_route_each_input_to_a_capable_worker(conn, settings, run_worker):
    old = Worker([thing_v1], settings=settings)
    new = Worker([thing_v2], settings=settings)
    async with run_worker(old):
        before = [await thing_v1.enqueue(ThingV1(a=i)) for i in range(3)]  # old producers
        async with run_worker(new):  # the new fleet joins; producers switch over
            during_old = [await thing_v1.enqueue(ThingV1(a=10 + i)) for i in range(2)]
            during_new = [await thing_v2.enqueue(ThingV2(a=20 + i, b="x")) for i in range(3)]
            await wait_until(
                lambda: _all_succeeded(conn, before + during_old + during_new), timeout=30
            )
        # The old fleet stays until its work has drained; nothing was misrouted.
        for task_id in before + during_old:
            row = await store.get_task(conn, task_id)
            assert row.result["v"] == 1
            assert row.worker == old.worker_id
        for task_id in during_new:
            row = await store.get_task(conn, task_id)
            assert row.result["v"] == 2
            assert row.worker == new.worker_id
    # Restarting the old worker later republishes only its own name.
    async with run_worker(Worker([thing_v1], settings=settings)):
        pass
    published = {row.name: row for row in await store.get_task_types(conn)}
    assert published["thing_v2"].fingerprint == thing_v2.spec.fingerprint
    assert published["thing_v2"].policy.max_concurrency == 3
    assert published["thing_v2"].input_schema["required"] == ["a", "b"]


@pytest.mark.usefixtures("sdk")
async def test_dedupe_and_concurrency_keys_are_scoped_by_name(conn):
    """Versioning splits the key domains: the same key in both names is two tasks."""
    for definition in (thing_v1, thing_v2):
        await store.publish_task_type(conn, definition.spec)
    first = await thing_v1.enqueue(ThingV1(a=1), key="k", concurrency_key="c")
    second = await thing_v2.enqueue(ThingV2(a=1, b="x"), key="k", concurrency_key="c")
    assert first != second
    assert await thing_v1.enqueue(ThingV1(a=2), key="k") == first
    assert await thing_v2.enqueue(ThingV2(a=2, b="y"), key="k") == second
    cur = await conn.execute("SELECT count(*) FROM fronta.tasks WHERE state = %s", (State.QUEUED,))
    assert (await cur.fetchone())[0] == 2
