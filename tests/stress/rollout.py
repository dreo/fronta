"""Drain legacy workers for the column change, then rehearse old/new worker overlap."""

from __future__ import annotations

import asyncio
import sys

from fronta import store
from tests.stress.__main__ import rows, scalar, seed, wait_done
from tests.stress.endurance import Fleet


async def rehearse(conn, dsn, legacy, directory):  # noqa: PLR0915  # ordered rollout
    directory.mkdir()
    initial_logs, old_logs, new_logs = directory / "initial", directory / "old", directory / "new"
    initial_logs.mkdir()
    old_logs.mkdir()
    new_logs.mkdir()
    config = {"concurrency": 32, "sleep_s": 0.01}
    initial = Fleet(dsn, config, initial_logs, repo=legacy)
    initial.env["PYTHONPATH"] = str(legacy / "src")
    old = Fleet(dsn, config, old_logs, repo=legacy)
    old.env["PYTHONPATH"] = str(legacy / "src")
    new = Fleet(dsn, config, new_logs)
    initialize = (
        "import asyncio,os,psycopg; from fronta import store\n"
        "async def main():\n"
        " async with await psycopg.AsyncConnection.connect(os.environ['FRONTA_DSN'],"
        "autocommit=True) as conn:\n"
        "  await store.init_schema(conn)\n"
        "asyncio.run(main())\n"
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        initialize,
        cwd=legacy,
        env=old.env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await process.communicate()
    assert process.returncode == 0, (out.decode(), err.decode())
    legacy_claims = await rows(
        conn,
        "SELECT proname FROM pg_proc JOIN pg_namespace n ON n.oid=pronamespace "
        "WHERE n.nspname='fronta' AND (proname='claim_tasks' OR proname ~ '^claim_v[0-9]+$')",
    )
    stop = asyncio.Event()
    submitted = 0

    async def produce():
        nonlocal submitted
        while not stop.is_set():
            await seed(conn, config, 500)
            submitted += 500
            await conn.execute("SELECT pg_notify('fronta_wake','')")
            await asyncio.sleep(0.1)

    producing = None
    try:
        for _ in range(4):
            await initial.add()
        producing = asyncio.create_task(produce())
        await asyncio.sleep(3)
        before = await scalar(conn, "SELECT count(*) FROM fronta.tasks WHERE state='succeeded'")
        assert before > 0
        # A task-column change cannot preserve an already-entering legacy call's row descriptor.
        stop.set()
        await producing
        await wait_done(conn, submitted, initial.active, 120)
        await initial.close()
        assert all(p.returncode == 0 for p in initial.all)
        assert await scalar(conn, "SELECT count(*) FROM fronta.tasks WHERE state='running'") == 0
        await store.init_schema(conn)
        for function in legacy_claims:
            assert await scalar(
                conn, "SELECT EXISTS(SELECT FROM pg_proc WHERE proname=%s)", (function["proname"],)
            )
        cutover = await scalar(conn, "SELECT clock_timestamp()")
        for _ in range(4):
            await old.add()
        for _ in range(4):
            await new.add()
        stop.clear()
        producing = asyncio.create_task(produce())
        await asyncio.sleep(5)
        old.check()
        new.check()
        mixed = await rows(
            conn,
            "SELECT worker,count(*) n FROM fronta.tasks "
            "WHERE state='succeeded' AND started_at >= %s GROUP BY worker",
            (cutover,),
        )
        assert len(mixed) >= 8, mixed
        stop.set()
        await producing
        await wait_done(conn, submitted, old.active + new.active, 120)
        await old.close()
        stop.clear()
        producing = asyncio.create_task(produce())
        await asyncio.sleep(3)
        stop.set()
        await producing
        await wait_done(conn, submitted, new.active, 120)
        await store.init_schema(conn, prune=True)
        assert not await scalar(
            conn, "SELECT EXISTS(SELECT FROM pg_proc WHERE proname='claim_tasks')"
        )
        check = (
            await rows(
                conn,
                "SELECT count(*) total,count(*) FILTER(WHERE state<>'succeeded' OR attempt<>1"
                " OR failures<>0) bad FROM fronta.tasks",
            )
        )[0]
        assert check["total"] == submitted
        assert check["bad"] == 0, check
    finally:
        if producing:
            producing.cancel()
            await asyncio.gather(producing, return_exceptions=True)
        await initial.close()
        await old.close()
        await new.close()
    assert all(p.returncode == 0 for p in initial.all + old.all + new.all)
    logs = "\n".join(p.read_text() for p in directory.rglob("*.log"))
    unexpected = [
        line
        for line in logs.splitlines()
        if ("WARNING" in line or "ERROR" in line) and "fingerprint" not in line
    ]
    assert not unexpected, unexpected
    return {
        "passed": True,
        "rehearsal_version": "drained-column-upgrade-v2",
        "initial_legacy_workers_drained_before_ddl": True,
        "producers_paused_for_drain": True,
        "legacy": str(legacy),
        "submitted": submitted,
        "completed_before_init": before,
        "mixed_workers": mixed,
        "final": check,
        "worker_exit_codes": [p.returncode for p in initial.all + old.all + new.all],
    }
