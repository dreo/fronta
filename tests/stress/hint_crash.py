"""Kill a real worker after its completion commits but before any of its hints can flush."""

from __future__ import annotations

import asyncio
import os
import signal
import sys

from fronta import Settings, State, Worker, runtime, store, subscribe, task
from fronta.hints import Hints
from tests.conftest import running
from tests.stress.__main__ import rows
from tests.stress.worker import Input


@task("hint_child", input=Input)
async def child(_ctx, inp):
    return inp.n


@task("hint_parent", input=Input)
async def parent(ctx, inp):
    return await ctx.enqueue(child, inp)


async def victim():
    original = store.complete

    async def complete(conn, outcomes):
        applied = await original(conn, outcomes)
        if applied:
            os.kill(os.getpid(), signal.SIGKILL)
        return applied

    async def hold_hints(*_args):
        await asyncio.Event().wait()

    store.complete, Hints._send = complete, hold_hints
    worker = Worker([parent])

    async def announce():
        await worker.started.wait()
        print("ready", flush=True)  # noqa: T201  # parent-process handshake

    announcing = asyncio.create_task(announce())
    try:
        return await worker.run()
    finally:
        announcing.cancel()
        await asyncio.gather(announcing, return_exceptions=True)


async def measure(conn, dsn, _unused):
    settings = Settings(dsn=dsn)
    await runtime.open_pool(settings)
    process = None
    try:
        async with (
            running(Worker([child], settings=settings)),
            subscribe("crash", settings=settings) as feed,
        ):
            # Let the child worker reach its idle poll ceiling before losing the enqueue hint.
            await asyncio.sleep(2)
            env = {k: v for k, v in os.environ.items() if not k.startswith("FRONTA_")}
            env["FRONTA_DSN"] = dsn
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "tests.stress.hint_crash",
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            async with asyncio.timeout(15):
                assert await process.stdout.readline() == b"ready\n"
                task_id = await parent.enqueue(Input(n=7))
                out, err = await process.communicate()
            assert process.returncode == -signal.SIGKILL, (process.returncode, out, err)
            events = []
            async with asyncio.timeout(5):
                while len(events) < 2:
                    batch = await anext(feed)
                    events.extend(batch.events)
                    await batch.ack()
            tasks = await rows(
                conn,
                "SELECT id,type,state,attempt,failures,result,"
                "extract(epoch FROM started_at-created_at) delay_s FROM fronta.tasks ORDER BY id",
            )
            assert len(tasks) == 2
            assert all(
                r["state"] == "succeeded" and r["attempt"] == 1 and r["failures"] == 0
                for r in tasks
            )
            assert tasks[0]["id"] == task_id
            assert tasks[0]["result"] == tasks[1]["id"]
            assert tasks[1]["result"] == 7
            assert sorted(e.id for e in events) == [r["id"] for r in tasks]
            assert all(e.state is State.SUCCEEDED and e.attempt == 1 for e in events)
            delay = float(tasks[1]["delay_s"])
            return {
                "passed": delay < settings.poll_interval_s,
                "worker_exit_code": process.returncode,
                "child_enqueue_to_start_s": delay,
                "poll_ceiling_s": settings.poll_interval_s,
                "tasks": tasks,
                "events": [
                    {"seq": e.seq, "id": e.id, "state": e.state, "attempt": e.attempt}
                    for e in events
                ],
            }
    finally:
        if process is not None and process.returncode is None:
            process.kill()
            await process.wait()
        await runtime.close_pool()


if __name__ == "__main__":
    sys.exit(asyncio.run(victim()))
