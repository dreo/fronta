"""Host-platform behavior shared by the worker and Linux process sandbox."""

from __future__ import annotations

import os
import socket
import sys

import pytest

from fronta import SandboxError, Worker, sandbox
from tests.workers import echo_proc


def test_worker_ids_are_unique_without_proc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sandbox, "_starttime", lambda _pid: None)

    first = sandbox.worker_id()
    second = sandbox.worker_id()

    assert first != second
    assert first.startswith(f"{socket.gethostname()}:{os.getpid()}:nonce-")
    monkeypatch.setattr(sandbox, "_starttime", lambda _pid: "123")
    assert sandbox.is_worker_alive(first) is None


def test_process_tasks_report_the_linux_requirement(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sandbox.sys, "platform", "darwin")

    with pytest.raises(SandboxError, match=r"process tasks need Linux.*darwin"):
        sandbox.require_linux()


def test_a_missing_worker_pid_is_dead(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sandbox, "_starttime", lambda _pid: None)

    def missing(_pid: int, _signal: int) -> None:
        raise ProcessLookupError

    monkeypatch.setattr(sandbox.os, "kill", missing)

    worker = f"{socket.gethostname()}:999999:1"
    assert sandbox.is_worker_alive(worker) is False


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS platform contract")
async def test_worker_with_process_tasks_refuses_to_start_on_macos(settings) -> None:
    with pytest.raises(SandboxError, match=r"process tasks need Linux.*darwin"):
        await Worker([echo_proc], settings=settings).run()
