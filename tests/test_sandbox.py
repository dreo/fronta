"""Sandbox: the process contract, the boundary, limits, the kill protocol, orphans, the probe."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time

import psycopg
import pytest

from fronta import Sandbox, SandboxError, Settings, State, Worker, process_task, sandbox, store
from fronta import worker as worker_module
from fronta.model import NewTask
from tests.conftest import FAST, leftover_sandboxes, spawn_worker, wait_until, worker_env
from tests.workers import In, echo_proc, hostile_proc, long_proc, sleep_proc

SH = "/bin/sh"
pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="bubblewrap process sandboxes require Linux"
)


def script(name, body, **kwargs):
    return process_task(name, [SH, "-c", body], input=In, **kwargs)


async def run_one(conn, settings, run_worker, definition, inp=None, **enqueue):
    async with run_worker(Worker([definition], settings=settings)):
        task_id = await store.enqueue(
            conn,
            _new_task(definition, inp or In(), **enqueue),
        )
        await wait_until(lambda: _terminal(conn, task_id), timeout=30)
    return await store.get_task(conn, task_id)


def _new_task(definition, inp, **kwargs):
    return NewTask(definition.name, inp.model_dump_json(), definition.policy, **kwargs)


async def _terminal(conn, task_id):
    row = await store.get_task(conn, task_id)
    return row is not None and row.state in (State.SUCCEEDED, State.FAILED, State.CANCELLED)


async def _state(conn, task_id, state):
    row = await store.get_task(conn, task_id)
    return row is not None and row.state is state


def marked(task_id):
    return sandbox.find_marked("FRONTA_TASK_ID", str(task_id))


async def test_contract_stdin_cwd_env_and_only_stdio_descriptors(conn, settings, run_worker):
    probe = script(
        "contract",
        "input=$(cat); printf '%s\\n' \"$input\"; pwd; env | sort;"
        " ls /proc/self/fd | tr '\\n' ' '; echo",
        sandbox=Sandbox(env={"EXTRA": "yes"}),
    )
    row = await run_one(conn, settings, run_worker, probe, In(n=42))
    assert row.state is State.SUCCEEDED
    out = row.result["stdout"].splitlines()
    assert json.loads(out[0]) == {"n": 42, "sleep_s": 0.0, "key": None}
    assert out[1] == "/work"
    env = dict(line.split("=", 1) for line in out[2:-1])
    assert env["FRONTA_TASK_ID"] == str(row.id)
    assert env["FRONTA_ATTEMPT"] == "1"
    assert env["FRONTA_WORKER_ID"] == row.worker
    assert len(env["FRONTA_SANDBOX_ID"]) == 32
    assert env["HOME"] == "/work"
    assert env["EXTRA"] == "yes"
    assert env["PATH"]
    assert env["LANG"] == "C.UTF-8"
    assert "FRONTA_DSN" not in env
    assert not any(
        k
        for k in env
        if k
        not in {
            "FRONTA_TASK_ID",
            "FRONTA_ATTEMPT",
            "FRONTA_WORKER_ID",
            "FRONTA_SANDBOX_ID",
            "HOME",
            "PATH",
            "LANG",
            "PWD",
            "EXTRA",
            "OLDPWD",
            "SHLVL",
            "_",
        }
    )
    assert out[-1].split() == ["0", "1", "2", "3"]  # 3 is ls's own directory descriptor
    assert row.result["exit_code"] == 0
    assert row.result["stderr"] == ""
    assert row.result["truncated"] is False


async def test_boundary_read_only_root_bounded_tmpfs_no_network_no_nested_namespaces(
    conn, settings, run_worker
):
    probe = script(
        "boundary",
        "touch /usr/x 2>/dev/null; echo usr=$?;"
        " touch /work/ok /tmp/ok; echo work=$?;"
        " head -c 3000000 /dev/zero > /work/big 2>/dev/null;"
        " echo big=$? size=$(stat -c %s /work/big);"
        " python3 -c 'import socket; s=socket.socket(); s.settimeout(2);"
        ' s.connect(("1.1.1.1", 53))\' 2>&1 | tail -1;'
        " python3 -c 'import socket; socket.gethostbyname(\"example.com\")' 2>&1 | tail -1;"
        " unshare -U true 2>&1 | tail -1;"
        " ls /home /root /etc/passwd 2>&1 | head -1",
        sandbox=Sandbox(tmpfs_bytes=1 << 20),
    )
    row = await run_one(conn, settings, run_worker, probe)
    assert row.state is State.SUCCEEDED, row.error
    out = row.result["stdout"]
    assert "usr=1" in out
    assert "work=0" in out
    assert "big=1 size=1048576" in out
    assert "Network is unreachable" in out
    assert "Temporary failure in name resolution" in out or "Name or service not known" in out
    assert "unshare" in out
    assert "failed" in out or "denied" in out or "No space" in out
    assert "No such file" in out


async def test_exit_zero_is_a_result_and_non_zero_a_retried_failure(conn, settings, run_worker):
    failing = script("failing", "echo partial; echo bad >&2; exit 7", max_attempts=2)
    row = await run_one(conn, settings, run_worker, failing)
    assert row.state is State.FAILED
    assert row.attempt == 2  # retried once
    assert row.failures == 2
    assert row.error["type"] == "ProcessFailed"
    assert row.error["exit_code"] == 7
    assert row.error["stdout"] == "partial\n"
    assert row.error["stderr"] == "bad\n"
    assert row.result is None
    fine = script("fine", "echo ok")
    row = await run_one(conn, settings, run_worker, fine)
    assert row.state is State.SUCCEEDED
    assert row.result == {"exit_code": 0, "stdout": "ok\n", "stderr": "", "truncated": False}


async def test_missing_executable_is_a_failed_attempt(conn, settings, run_worker):
    missing = process_task("missing", ["/nonexistent/binary"], input=In, max_attempts=1)
    row = await run_one(conn, settings, run_worker, missing)
    assert row.state is State.FAILED
    assert row.error["type"] == "ProcessFailed"
    assert "No such file" in row.error["stderr"]


async def test_output_is_truncated_at_the_cap_and_flagged(conn, settings, run_worker):
    chatty = script(
        "chatty",
        "head -c 300000 /dev/zero | tr '\\0' 'a'; echo; head -c 5000 /dev/zero | tr '\\0' 'b' >&2",
        sandbox=Sandbox(max_output_bytes=1000),
    )
    row = await run_one(conn, settings, run_worker, chatty)
    assert row.state is State.SUCCEEDED
    assert len(row.result["stdout"]) == 1000
    assert len(row.result["stderr"]) == 1000
    assert row.result["truncated"] is True


async def test_large_stdin_with_a_flooding_process_does_not_deadlock(conn, settings, run_worker):
    flood = script(
        "flood", "head -c 2000000 /dev/zero; cat | wc -c", sandbox=Sandbox(max_output_bytes=100)
    )
    big = In(key="x" * 900_000)
    started = time.monotonic()
    row = await run_one(conn, settings, run_worker, flood, big)
    assert row.state is State.SUCCEEDED
    assert time.monotonic() - started < 20
    assert row.result["truncated"] is True


async def test_memory_cpu_and_pid_limits_are_enforced(conn, settings, run_worker):
    hog = script(
        "hog",
        "python3 -c 'x = bytearray(300 << 20); print(\"allocated\")'",
        sandbox=Sandbox(memory_bytes=128 << 20),
        max_attempts=1,
    )
    row = await run_one(conn, settings, run_worker, hog)
    assert row.state is State.FAILED
    assert "MemoryError" in row.error["stderr"]

    spinner = script(
        "spinner", "while :; do :; done", sandbox=Sandbox(cpu_time_s=1), max_attempts=1
    )
    started = time.monotonic()
    row = await run_one(conn, settings, run_worker, spinner)
    assert row.state is State.FAILED
    assert row.error["exit_code"] in (137, 152)  # SIGKILL or SIGXCPU
    assert time.monotonic() - started < 10

    forker = script(
        "forker",
        "for i in $(seq 40); do sleep 2 & done; wait; echo forks-done",
        sandbox=Sandbox(max_pids=10),
        max_attempts=1,
    )
    row = await run_one(conn, settings, run_worker, forker)
    assert "Cannot fork" in (row.error or {}).get("stderr", "") + (row.result or {}).get(
        "stderr", ""
    )


async def test_term_ignoring_setsid_tree_is_killed_after_the_grace_period_on_timeout(
    conn, settings, run_worker
):
    async with run_worker(Worker([hostile_proc], settings=settings)):
        task_id = await store.enqueue(conn, _new_task(hostile_proc, In()))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        await wait_until(lambda: _marked(task_id), timeout=10)
        started = time.monotonic()
        await wait_until(lambda: _state(conn, task_id, State.FAILED), timeout=30)
        # The slot was freed only after the sandbox was verified dead.
        assert not marked(task_id)
        assert time.monotonic() - started <= settings.grace_s + settings.kill_timeout_s + 5
    row = await store.get_task(conn, task_id)
    assert row.error["type"] == "AttemptTimeout"
    assert not marked(task_id)


async def test_term_ignoring_tree_is_killed_on_cancel_and_the_task_is_cancelled(
    conn, settings, run_worker
):
    stubborn = process_task(
        "stubborn_proc",
        [SH, "-c", "trap '' TERM; setsid sleep 600 & sleep 600"],
        input=In,
        attempt_timeout=600,
        sandbox=Sandbox(max_pids=20),
    )
    async with run_worker(Worker([stubborn], settings=settings)):
        task_id = await store.enqueue(conn, _new_task(stubborn, In()))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        await wait_until(lambda: _marked(task_id), timeout=10)
        assert await store.request_cancel(conn, task_id) is State.RUNNING
        await wait_until(lambda: _state(conn, task_id, State.CANCELLED), timeout=30)
    assert not marked(task_id)


async def test_cooperative_process_gets_sigterm_and_exits_in_time(conn, settings, run_worker):
    polite = script("polite", "trap 'echo bye; exit 0' TERM; sleep 600 & wait", attempt_timeout=600)
    async with run_worker(Worker([polite], settings=settings)):
        task_id = await store.enqueue(conn, _new_task(polite, In()))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        await wait_until(lambda: _marked(task_id), timeout=10)
        started = time.monotonic()
        await store.request_cancel(conn, task_id)
        await wait_until(lambda: _state(conn, task_id, State.CANCELLED), timeout=10)
        assert time.monotonic() - started < settings.grace_s  # cooperative: no kill needed
    assert not marked(task_id)


async def test_sigkill_of_the_worker_leaves_no_sandboxed_process(conn, settings):
    await store.publish_task_type(conn, long_proc.spec)
    task_id = await store.enqueue(conn, _new_task(long_proc, In()))
    proc = spawn_worker("tests.subworkers:sandbox_crash_worker", worker_env(settings))
    try:
        await wait_until(lambda: _state(conn, task_id, State.RUNNING), timeout=20)
        await wait_until(lambda: _marked(task_id), timeout=10)
        assert marked(task_id)
        proc.kill()
        proc.wait(timeout=10)
        await asyncio.sleep(0.5)
        assert marked(task_id) == []
        assert leftover_sandboxes() == []
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


@pytest.mark.usefixtures("conn")
async def test_orphan_from_the_arming_window_is_killed_by_the_next_workers_scavenger(
    settings, run_worker
):
    """Kill the outer bwrap right after spawn until an init survives it, then let a worker start."""
    dead_worker = f"{sandbox.worker_id().split(':')[0]}:999999:1"
    env = sandbox.command_env(
        Sandbox(), worker=dead_worker, sandbox_id="orphan-" + os.urandom(4).hex()
    )
    cmd = sandbox.build_argv(
        bwrap_path=settings.bwrap_path,
        sandbox=Sandbox(),
        argv=(SH, "-c", "sleep 600"),
        status_fd=1,
        env=env,
    )
    outer_env = {
        "PATH": os.environ["PATH"],
        sandbox.WORKER_ENV: dead_worker,
        sandbox.SANDBOX_ENV: env[sandbox.SANDBOX_ENV],
    }
    quiet = {"start_new_session": True, "stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    reproduced = False
    for attempt in range(40):
        proc = subprocess.Popen(cmd, env=outer_env, **quiet)  # noqa: S603  # our own command
        time.sleep(0.001 + (attempt % 6) * 0.001)
        proc.kill()
        proc.wait(timeout=5)
        time.sleep(0.05)
        if sandbox.find_marked(sandbox.WORKER_ENV, dead_worker):
            reproduced = True
            break
    if not reproduced:  # timing did not cooperate on this host: use a plain orphan-like sandbox
        subprocess.Popen(cmd, env=outer_env, **quiet)  # noqa: S603  # our own command
        await wait_until(lambda: _has_marked(sandbox.WORKER_ENV, dead_worker), timeout=5)
    assert sandbox.find_marked(sandbox.WORKER_ENV, dead_worker)
    async with run_worker(Worker([echo_proc], settings=settings)):
        await wait_until(lambda: _none_marked(sandbox.WORKER_ENV, dead_worker), timeout=10)
    assert sandbox.find_marked(sandbox.WORKER_ENV, dead_worker) == []


async def test_a_stop_during_the_sandbox_spawn_leaves_nothing_behind(
    conn, dsn, run_worker, tmp_path
):
    """The attempt times out while bwrap is still starting: the spawn must clean up after itself."""
    wrapper = tmp_path / "slow-bwrap"
    wrapper.write_text('#!/bin/sh\nsleep 6\nexec bwrap "$@"\n')
    wrapper.chmod(0o755)
    slow = Settings(dsn=dsn, **{**FAST, "bwrap_path": str(wrapper)})
    lazy = script("lazy", "sleep 600", attempt_timeout=1, max_attempts=1)
    async with run_worker(Worker([lazy], settings=slow)):
        task_id = await store.enqueue(conn, _new_task(lazy, In()))
        await wait_until(lambda: _state(conn, task_id, State.FAILED), timeout=30)
    row = await store.get_task(conn, task_id)
    assert row.error["type"] == "AttemptTimeout"
    assert marked(task_id) == []
    assert leftover_sandboxes() == []


async def test_a_failing_graceful_stop_still_ends_in_a_verified_kill(
    conn, settings, run_worker, monkeypatch
):
    def broken(*_args, **_kwargs):
        msg = "simulated /proc failure"
        raise OSError(msg)

    monkeypatch.setattr(sandbox, "signal_marked", broken)
    stubborn = script("stubborn2", "trap '' TERM; sleep 600", attempt_timeout=600, max_attempts=1)
    async with run_worker(Worker([stubborn], settings=settings)):
        task_id = await store.enqueue(conn, _new_task(stubborn, In()))
        await wait_until(lambda: _state(conn, task_id, State.RUNNING))
        await wait_until(lambda: _marked(task_id), timeout=10)
        assert await store.request_cancel(conn, task_id) is State.RUNNING
        await wait_until(lambda: _state(conn, task_id, State.CANCELLED), timeout=30)
        assert marked(task_id) == []
    assert leftover_sandboxes() == []


async def test_invalid_utf8_and_nul_in_output_become_replacement_characters(
    conn, settings, run_worker
):
    binary = script("binary", "printf 'a\\377\\000b'; printf 'e\\000' >&2")
    row = await run_one(conn, settings, run_worker, binary)
    assert row.state is State.SUCCEEDED
    assert row.result["stdout"] == "a\ufffd\ufffdb"
    assert row.result["stderr"] == "e\ufffd"


async def test_a_full_tmp_is_an_ordinary_process_error(conn, settings, run_worker):
    filler = script(
        "filler",
        "head -c 3000000 /dev/zero > /tmp/big; echo rc=$?; stat -c %s /tmp/big",
        sandbox=Sandbox(tmpfs_bytes=1 << 20),
    )
    row = await run_one(conn, settings, run_worker, filler)
    assert row.state is State.SUCCEEDED
    assert "rc=1" in row.result["stdout"]
    assert "1048576" in row.result["stdout"]
    assert "No space left" in row.result["stderr"]


async def _none_marked(name, value):
    return not sandbox.find_marked(name, value)


async def test_probe_fails_closed_when_the_sandbox_cannot_run(dsn):
    broken = Settings(dsn=dsn, **{**FAST, "bwrap_path": "/nonexistent/bwrap"})
    with pytest.raises(SandboxError, match="probe failed"):
        await Worker([echo_proc], settings=broken).run()
    unbindable = process_task(
        "unbindable",
        [SH, "-c", "true"],
        input=In,
        sandbox=Sandbox(ro_binds=("/usr", "/nonexistent/path")),
    )
    with pytest.raises(SandboxError, match="probe failed"):
        await Worker([unbindable], settings=Settings(dsn=dsn, **FAST)).run()


async def test_probe_runs_once_per_distinct_sandbox_configuration(dsn, run_worker, monkeypatch):
    calls = []
    original = sandbox.probe

    async def counting(bwrap_path, sandbox_, worker, **kwargs):
        calls.append(sandbox_.identity())
        await original(bwrap_path, sandbox_, worker, **kwargs)

    monkeypatch.setattr(sandbox, "probe", counting)
    a = script("a", "true")
    b = script("b", "true")
    c = script("c", "true", sandbox=Sandbox(max_pids=5))
    async with run_worker(Worker([a, b, c, sleep_proc], settings=Settings(dsn=dsn, **FAST))):
        pass
    assert len(calls) == 2


async def _has_marked(name, value):
    return bool(sandbox.find_marked(name, value))


async def _marked(task_id):
    return bool(marked(task_id))


def test_no_sandbox_process_survives_this_module():
    assert leftover_sandboxes() == []


async def test_cancelling_a_spawn_kills_what_it_started(settings, tmp_path):
    wrapper = tmp_path / "slow-bwrap"
    wrapper.write_text('#!/bin/sh\nsleep 6\nexec bwrap "$@"\n')
    wrapper.chmod(0o755)
    env = sandbox.command_env(
        Sandbox(), worker=sandbox.worker_id(), sandbox_id="spawn-" + os.urandom(4).hex()
    )
    spawning = asyncio.create_task(
        sandbox.SandboxProcess.spawn(
            bwrap_path=str(wrapper), sandbox=Sandbox(), argv=(SH, "-c", "sleep 600"), env=env
        )
    )
    await wait_until(lambda: _has_marked(sandbox.SANDBOX_ENV, env[sandbox.SANDBOX_ENV]), timeout=5)
    spawning.cancel()
    with pytest.raises(asyncio.CancelledError):
        await spawning
    assert sandbox.find_marked(sandbox.SANDBOX_ENV, env[sandbox.SANDBOX_ENV]) == []
    assert settings.bwrap_path  # the settings fixture also guarantees the test database


async def test_a_runner_that_crashes_after_the_spawn_does_not_leak_the_sandbox(
    conn, settings, run_worker, monkeypatch
):
    original = sandbox.SandboxProcess.run

    async def crash(self, stdin, max_output):
        if self.sandbox_id.startswith("probe-"):  # the startup probe must keep working
            return await original(self, stdin, max_output)
        await asyncio.sleep(0.3)  # the sandbox is alive at this point
        msg = "simulated runner crash"
        raise RuntimeError(msg)

    monkeypatch.setattr(sandbox.SandboxProcess, "run", crash)
    sleeper = script("sleeper2", "sleep 600", attempt_timeout=600, max_attempts=1)
    async with run_worker(Worker([sleeper], settings=settings)):
        task_id = await store.enqueue(conn, _new_task(sleeper, In()))
        await wait_until(lambda: _state(conn, task_id, State.FAILED), timeout=30)
        assert marked(task_id) == []
    row = await store.get_task(conn, task_id)
    assert row.error["type"] == "RuntimeError"
    assert leftover_sandboxes() == []
    monkeypatch.setattr(sandbox.SandboxProcess, "run", original)


async def test_a_crashed_runner_has_its_sandbox_killed_before_the_outcome_is_written(
    conn, settings, run_worker, monkeypatch
):
    """Even with the database down, a crashed runner's sandbox dies at once, not after the retry."""
    original_run = sandbox.SandboxProcess.run
    original_fail = store.fail
    gate = {"open": False, "calls": 0}

    async def crash(self, stdin, max_output):
        if self.sandbox_id.startswith("probe-"):
            return await original_run(self, stdin, max_output)
        await asyncio.sleep(0.3)
        msg = "simulated runner crash"
        raise RuntimeError(msg)

    async def gated_fail(*args, **kwargs):
        gate["calls"] += 1
        if not gate["open"]:
            msg = "simulated outage"
            raise psycopg.OperationalError(msg)
        return await original_fail(*args, **kwargs)

    monkeypatch.setattr(sandbox.SandboxProcess, "run", crash)
    monkeypatch.setattr(store, "fail", gated_fail)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_S", 0.1)
    monkeypatch.setattr(worker_module, "_TRANSITION_RETRY_MAX_S", 0.1)
    sleeper = script("sleeper3", "sleep 600", attempt_timeout=600, max_attempts=1)
    async with run_worker(Worker([sleeper], settings=settings)):
        task_id = await store.enqueue(conn, _new_task(sleeper, In()))
        await wait_until(lambda: _calls_at_least(gate, 3), timeout=15)  # the transition is retrying
        assert marked(task_id) == []  # ... and the sandbox is already dead
        gate["open"] = True
        await wait_until(lambda: _state(conn, task_id, State.FAILED), timeout=15)
    assert leftover_sandboxes() == []


async def _calls_at_least(gate, n):
    return gate["calls"] >= n


async def test_an_unverified_kill_blocks_the_outcome_until_the_sandbox_is_verified_dead(
    conn, settings, run_worker, monkeypatch
):
    """While `kill()` reports False the sandbox is really alive: no outcome, no free slot."""
    original_kill = sandbox.SandboxProcess.kill
    original_fail = store.fail
    state = {"refusals": 3, "fail_calls": 0, "alive_at_refusal": []}

    async def refusing_kill(self, timeout_s):
        if self.sandbox_id.startswith("probe-"):
            return await original_kill(self, timeout_s)
        if state["refusals"] > 0:
            state["refusals"] -= 1
            state["alive_at_refusal"].append(
                (
                    bool(sandbox.find_marked(sandbox.SANDBOX_ENV, self.sandbox_id)),
                    state["fail_calls"],
                )
            )
            await asyncio.sleep(0.2)  # a real kill waits on the pidfd; this one only pretends
            return False
        return await original_kill(self, timeout_s)

    async def counting_fail(*args, **kwargs):
        state["fail_calls"] += 1
        return await original_fail(*args, **kwargs)

    monkeypatch.setattr(sandbox.SandboxProcess, "kill", refusing_kill)
    monkeypatch.setattr(store, "fail", counting_fail)
    lingering = script("lingering", "trap '' TERM; sleep 600", attempt_timeout=1, max_attempts=1)
    async with run_worker(Worker([lingering], settings=settings)):
        task_id = await store.enqueue(conn, _new_task(lingering, In()))
        await wait_until(lambda: _state(conn, task_id, State.FAILED), timeout=40)
    assert state["refusals"] == 0
    assert state["alive_at_refusal"] == [(True, 0)] * 3  # alive each time, outcome not yet written
    assert (await store.get_task(conn, task_id)).error["type"] == "AttemptTimeout"
    assert leftover_sandboxes() == []


async def test_kill_errors_never_spin_the_event_loop(conn, settings, run_worker, monkeypatch):
    """A kill that raises must yield: the watchdog would otherwise abort the worker (exit 70)."""
    original_kill = sandbox.SandboxProcess.kill
    state = {"errors": 3}

    async def failing_kill(self, timeout_s):
        if self.sandbox_id.startswith("probe-"):
            return await original_kill(self, timeout_s)
        if state["errors"] > 0:
            state["errors"] -= 1
            msg = "simulated pidfd failure"
            raise OSError(msg)
        return await original_kill(self, timeout_s)

    monkeypatch.setattr(sandbox.SandboxProcess, "kill", failing_kill)
    stubborn = script("stubborn3", "trap '' TERM; sleep 600", attempt_timeout=1, max_attempts=1)
    async with run_worker(Worker([stubborn], settings=settings)):
        task_id = await store.enqueue(conn, _new_task(stubborn, In()))
        await wait_until(lambda: _state(conn, task_id, State.FAILED), timeout=40)
    assert state["errors"] == 0
    assert leftover_sandboxes() == []


async def test_the_configured_kill_timeout_reaches_every_spawn(dsn, run_worker, monkeypatch):
    """Probes and attempts alike spawn with the worker's `kill_timeout_s`, never a default."""
    original_spawn = sandbox.SandboxProcess.spawn
    seen: list[float] = []

    async def recording_spawn(cls, **kwargs):
        seen.append(kwargs["kill_timeout_s"])
        return await original_spawn.__func__(cls, **kwargs)

    monkeypatch.setattr(sandbox.SandboxProcess, "spawn", classmethod(recording_spawn))
    tuned = Settings(dsn=dsn, **{**FAST, "kill_timeout_s": 1.25})
    fine = script("fine2", "true")
    async with run_worker(Worker([fine], settings=tuned)):
        pass
    assert seen  # the startup probe spawned
    assert set(seen) == {1.25}


async def test_a_failing_startup_probe_cleans_up_with_the_configured_timeout(
    dsn, monkeypatch, tmp_path
):
    """A bwrap that dies before its status line: the probe's cleanup runs with `kill_timeout_s`."""
    original_abort = sandbox._abort
    seen: list[float] = []

    async def recording_abort(proc, outer_pidfd, timeout_s):
        seen.append(timeout_s)
        return await original_abort(proc, outer_pidfd, timeout_s)

    monkeypatch.setattr(sandbox, "_abort", recording_abort)
    broken = tmp_path / "broken-bwrap"
    broken.write_text("#!/bin/sh\necho 'bwrap: simulated failure' >&2\nexit 3\n")
    broken.chmod(0o755)
    tuned = Settings(dsn=dsn, **{**FAST, "kill_timeout_s": 1.25, "bwrap_path": str(broken)})
    fine = script("fine3", "true")
    with pytest.raises(SandboxError, match="simulated failure"):
        await Worker([fine], settings=tuned).run()
    assert seen == [1.25]  # the failed probe's cleanup ran once, with the configured timeout
    assert leftover_sandboxes() == []
