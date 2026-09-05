"""bubblewrap sandbox: command builder, spawn, pidfd signalling, kill protocol, probe, scavenger.

Process identity is never a bare pid: every signal goes through a pidfd opened before the target's
marker is re-verified, so a recycled pid can never receive a Fronta signal. Every sandbox process
(outer bwrap, inner init, the command and its descendants) carries `FRONTA_SANDBOX_ID` and
`FRONTA_WORKER_ID` in its initial environment, which is how a sandbox is found in `/proc`.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import errno
import json
import os
import signal
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Self
from uuid import uuid4

from fronta.errors import SandboxError
from fronta.model import DEFAULT_RO_BINDS

if TYPE_CHECKING:
    from fronta.model import Sandbox

SANDBOX_ENV = "FRONTA_SANDBOX_ID"
WORKER_ENV = "FRONTA_WORKER_ID"
PROBE_OUTPUT = b"FRONTA_SANDBOX_OK\n"
_MERGED_USR_DIRS = ("/bin", "/sbin", "/lib", "/lib64", "/lib32", "/libx32")
_SYS_PIDFD_OPEN = 434
_SYS_PIDFD_SEND_SIGNAL = 424
_STATUS_LINE_TIMEOUT_S = 30.0
_STDERR_CAP = 64 * 1024
_WORKER_ID_PARTS = 3  # host:pid:starttime

_libc: ctypes.CDLL | None = None
"""Loaded on first use: importing Fronta must work on any platform; only running sandboxed
processes needs Linux (`require_linux()` guards the entry points)."""


def _syscall(number: int, *args: object) -> int:
    global _libc  # noqa: PLW0603  # lazy, process-wide handle
    libc = _libc
    if libc is None:
        libc = ctypes.CDLL(None, use_errno=True)
        libc.syscall.restype = ctypes.c_long
        _libc = libc
    return int(libc.syscall(number, *args))


def require_linux() -> None:
    """Process tasks need Linux (user namespaces, pidfds, /proc); everything else is portable."""
    if sys.platform != "linux":
        msg = f"process tasks need Linux (bubblewrap sandboxes); this is {sys.platform}"
        raise SandboxError(msg)


def worker_id() -> str:
    """`host:pid:starttime-or-nonce` — unique even on hosts without Linux `/proc`."""
    pid = os.getpid()
    identity = _starttime(pid) or f"nonce-{uuid4().hex}"
    return f"{socket.gethostname()}:{pid}:{identity}"


def _starttime(pid: int) -> str | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii", errors="replace")
    except OSError:
        return None
    # field 22, counted after the ")" that closes the command name
    return stat.rsplit(")", 1)[1].split()[19]


def is_worker_alive(worker: str) -> bool | None:
    """True/False for a worker id of this host; None when it cannot be judged here."""
    parts = worker.rsplit(":", 2)
    if len(parts) != _WORKER_ID_PARTS or parts[0] != socket.gethostname():
        return None
    try:
        pid = int(parts[1])
    except ValueError:
        return None
    if pid <= 0 or parts[2].startswith("nonce-"):
        return None
    starttime = _starttime(pid)
    if starttime is not None:
        return starttime == parts[2]
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        pass
    return None


class Pidfd:
    """A pidfd (raw syscalls: this CPython build may lack `os.pidfd_open`).

    `close()` while `wait_exit()` is in flight is deferred until the wait ends, so the descriptor
    number can never be recycled under a registered reader.
    """

    def __init__(self, fd: int) -> None:
        self.fd = fd
        self._waiting = False
        self._close_requested = False

    @classmethod
    def open(cls, pid: int) -> Pidfd | None:
        """None when the process is already gone (ESRCH)."""
        if hasattr(os, "pidfd_open"):
            try:
                return cls(os.pidfd_open(pid))
            except ProcessLookupError:
                return None
        fd = _syscall(_SYS_PIDFD_OPEN, ctypes.c_int(pid), ctypes.c_uint(0))
        if fd < 0:
            err = ctypes.get_errno()
            if err == errno.ESRCH:
                return None
            raise OSError(err, os.strerror(err))
        return cls(int(fd))

    def send_signal(self, sig: signal.Signals) -> bool:
        """False when the process already exited (or the pidfd is closed)."""
        if self.fd < 0:
            return False
        if hasattr(signal, "pidfd_send_signal"):
            try:
                signal.pidfd_send_signal(self.fd, sig)
            except ProcessLookupError:
                return False
            return True
        rc = _syscall(_SYS_PIDFD_SEND_SIGNAL, ctypes.c_int(self.fd), ctypes.c_int(sig), None, 0)
        if rc == 0:
            return True
        err = ctypes.get_errno()
        if err == errno.ESRCH:
            return False
        raise OSError(err, os.strerror(err))

    async def wait_exit(self, timeout_s: float) -> bool:
        """True once the process has exited (pidfd readable); False on timeout."""
        if self.fd < 0:
            return True
        loop = asyncio.get_running_loop()
        exited = loop.create_future()

        def on_exit() -> None:
            if not exited.done():
                exited.set_result(True)

        self._waiting = True
        loop.add_reader(self.fd, on_exit)
        try:
            await asyncio.wait_for(exited, timeout_s)
        except TimeoutError:
            return False
        finally:
            loop.remove_reader(self.fd)
            self._waiting = False
            if self._close_requested:
                self.close()
        return True

    def close(self) -> None:
        if self._waiting:
            self._close_requested = True
            return
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1


def _environ_has(pid: int, marker: bytes) -> bool:
    try:
        return marker in Path(f"/proc/{pid}/environ").read_bytes()
    except OSError:
        return False


def find_marked(name: str, value: str) -> list[int]:
    """Pids whose initial environment carries `name=value`."""
    marker = f"{name}={value}".encode() + b"\0"
    return [pid for pid, environ in _proc_environs() if marker in environ]


def _proc_environs() -> list[tuple[int, bytes]]:
    """(pid, initial environment) of every process whose environment is readable."""
    try:
        entries = list(Path("/proc").iterdir())
    except OSError:
        return []
    found = []
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            found.append((int(entry.name), (entry / "environ").read_bytes()))
        except OSError:
            continue
    return found


def _kill_marked(pid: int, marker: bytes, sig: signal.Signals) -> bool:
    """Signal `pid` through a pidfd after re-checking the marker under that identity."""
    try:
        pidfd = Pidfd.open(pid)
    except OSError:
        return False
    if pidfd is None:
        return False
    try:
        # The pidfd pins the identity: a pid recycled after this point cannot match anymore.
        return _environ_has(pid, marker) and pidfd.send_signal(sig)
    except OSError:
        return False
    finally:
        pidfd.close()


def signal_marked(
    name: str, value: str, sig: signal.Signals, *, exclude: frozenset[int] = frozenset()
) -> int:
    """Signal every process carrying the marker, each through a pidfd. Returns the count.

    Best effort by construction: a process that cannot be opened or signalled (gone, EPERM,
    descriptor exhaustion) is skipped; the hard kill and the scavenger are the backstops.
    """
    marker = f"{name}={value}".encode() + b"\0"
    return sum(
        _kill_marked(pid, marker, sig) for pid in find_marked(name, value) if pid not in exclude
    )


def _owner_of(environ: bytes) -> str | None:
    """The `FRONTA_WORKER_ID` value in an initial environment, if any."""
    prefix = WORKER_ENV.encode() + b"="
    for entry in environ.split(b"\0"):
        if entry.startswith(prefix):
            return entry[len(prefix) :].decode(errors="replace")
    return None


def scavenge_orphans() -> int:
    """SIGKILL sandbox processes whose worker (same host) is dead. Returns the count."""
    dead: set[str] = set()
    alive: set[str] = {worker_id()}
    killed = 0
    for pid, environ in _proc_environs():
        owner = _owner_of(environ)
        if owner is None or owner in alive:
            continue
        if owner not in dead:
            if is_worker_alive(owner) is not False:
                alive.add(owner)
                continue
            dead.add(owner)
        killed += _kill_marked(pid, f"{WORKER_ENV}={owner}".encode() + b"\0", signal.SIGKILL)
    return killed


def build_argv(
    *,
    bwrap_path: str,
    sandbox: Sandbox,
    argv: tuple[str, ...],
    status_fd: int,
    env: dict[str, str],
) -> list[str]:
    """The full bwrap command line for one attempt."""
    cmd = [
        bwrap_path,
        "--unshare-user",
        "--unshare-all",
        "--disable-userns",
        "--die-with-parent",
        "--new-session",
        "--json-status-fd",
        str(status_fd),
    ]
    for path in sandbox.ro_binds:
        cmd += ["--ro-bind-try" if path in DEFAULT_RO_BINDS else "--ro-bind", path, path]
    for path in _MERGED_USR_DIRS:
        if Path(path).is_symlink():
            cmd += ["--symlink", os.fspath(Path(path).readlink()), path]
    cmd += ["--proc", "/proc", "--dev", "/dev"]
    size = str(sandbox.tmpfs_bytes)
    cmd += ["--size", size, "--tmpfs", "/tmp"]  # noqa: S108  # inside the sandbox
    cmd += ["--size", size, "--tmpfs", "/work", "--chdir", "/work", "--clearenv"]
    for key, value in env.items():
        cmd += ["--setenv", key, value]
    cmd.append("--")
    limits = []
    if sandbox.cpu_time_s is not None:
        limits.append(f"--cpu={max(1, int(sandbox.cpu_time_s))}")
    if sandbox.memory_bytes is not None:
        limits.append(f"--as={sandbox.memory_bytes}")
    if sandbox.max_pids is not None:
        limits.append(f"--nproc={sandbox.max_pids}")
    if limits:
        cmd += ["prlimit", *limits, "--"]
    cmd += list(argv)
    return cmd


def command_env(
    sandbox: Sandbox, *, worker: str, sandbox_id: str, task_env: dict[str, str] | None = None
) -> dict[str, str]:
    """The command's environment: cleared except PATH/HOME/LANG, `FRONTA_*`, and `Sandbox.env`."""
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": "/work",
        "LANG": "C.UTF-8",
        **sandbox.env,
        WORKER_ENV: worker,
        SANDBOX_ENV: sandbox_id,
        **(task_env or {}),
    }


def _text(data: bytes) -> str:
    """Decode a stream for storage: invalid UTF-8 and NUL (unstorable in jsonb) become U+FFFD."""
    return data.decode("utf-8", errors="replace").replace("\x00", "\ufffd")


@dataclass(frozen=True, slots=True)
class ProcessResult:
    exit_code: int
    stdout: str
    stderr: str
    truncated: bool

    def to_json(self) -> dict[str, object]:
        return {
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "truncated": self.truncated,
        }


@dataclass(slots=True)
class SandboxProcess:
    """One spawned sandbox: `spawn()`, then `run()`; stop with `terminate()` and `kill()`."""

    proc: asyncio.subprocess.Process
    status: asyncio.StreamReader
    sandbox_id: str
    init_pid: int
    init_pidfd: Pidfd | None
    outer_pidfd: Pidfd | None
    kill_timeout_s: float = 5.0
    dead: bool = False
    """True once the sandbox init is verified gone (nothing inside can run anymore)."""

    @classmethod
    async def spawn(
        cls,
        *,
        bwrap_path: str,
        sandbox: Sandbox,
        argv: tuple[str, ...],
        env: dict[str, str],
        kill_timeout_s: float = 5.0,
    ) -> Self:
        """Start bwrap and wait for its `child-pid` status line (the launch barrier).

        `env` is the command's whole environment; it must carry `FRONTA_SANDBOX_ID` and
        `FRONTA_WORKER_ID`, which are copied onto the outer bwrap process as well.
        """
        require_linux()
        sandbox_id = env[SANDBOX_ENV]
        read_fd, write_fd = os.pipe()
        try:
            cmd = build_argv(
                bwrap_path=bwrap_path, sandbox=sandbox, argv=argv, status_fd=write_fd, env=env
            )
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                pass_fds=(write_fd,),
                env={
                    "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
                    WORKER_ENV: env[WORKER_ENV],
                    SANDBOX_ENV: sandbox_id,
                },
                start_new_session=True,
            )
        except OSError as exc:
            os.close(read_fd)
            msg = f"cannot start {bwrap_path}: {exc}"
            raise SandboxError(msg) from exc
        finally:
            os.close(write_fd)
        outer_pidfd: Pidfd | None = None
        init_pidfd: Pidfd | None = None
        try:
            outer_pidfd = Pidfd.open(proc.pid)
            loop = asyncio.get_running_loop()
            status = asyncio.StreamReader()
            await loop.connect_read_pipe(
                lambda: asyncio.StreamReaderProtocol(status), os.fdopen(read_fd, "rb", 0)
            )
            try:
                line = await asyncio.wait_for(status.readline(), _STATUS_LINE_TIMEOUT_S)
            except TimeoutError:
                line = b""
            if line:
                init_pid = int(json.loads(line)["child-pid"])
                init_pidfd = Pidfd.open(init_pid)
        except BaseException:
            # Cancelled or failed while the sandbox was starting: nothing may outlive this call.
            await _abort(proc, outer_pidfd, kill_timeout_s, sandbox_id)
            if outer_pidfd is not None:
                outer_pidfd.close()
            raise
        if not line:
            # bwrap gave up before creating the sandbox (bad option, missing bind, no userns).
            stderr = await _abort(proc, outer_pidfd, kill_timeout_s, sandbox_id)
            if outer_pidfd is not None:
                outer_pidfd.close()
            msg = f"sandbox setup failed (bwrap exit {proc.returncode}): {stderr[:2000]}"
            raise SandboxError(msg)
        return cls(proc, status, sandbox_id, init_pid, init_pidfd, outer_pidfd, kill_timeout_s)

    async def run(self, stdin: bytes, max_output: int) -> ProcessResult:
        """Feed stdin, drain both outputs (capped), wait for exit."""
        writer, stdout, stderr = self.proc.stdin, self.proc.stdout, self.proc.stderr
        if writer is None or stdout is None or stderr is None:  # pragma: no cover
            msg = "sandbox process was not spawned with pipes"
            raise RuntimeError(msg)
        truncated = False

        async def feed() -> None:
            try:
                writer.write(stdin)
                await writer.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                writer.close()

        async def drain(stream: asyncio.StreamReader) -> bytes:
            nonlocal truncated
            kept = bytearray()
            while chunk := await stream.read(65536):
                room = max_output - len(kept)
                if room > 0:
                    kept += chunk[:room]
                if len(chunk) > room:
                    truncated = True
            return bytes(kept)

        try:
            _, out, err = await asyncio.gather(feed(), drain(stdout), drain(stderr))
            await self.proc.wait()
            self.dead = True  # the outer bwrap only exits after its init did
        except BaseException:
            # Cancelled or broken while the process ran: it does not outlive this call.
            await self.kill(self.kill_timeout_s)
            raise
        finally:
            self._close_fds()
        exit_code = self.proc.returncode if self.proc.returncode is not None else -1
        status_rest = await self.status.read()
        for line in status_rest.splitlines():
            try:
                exit_code = int(json.loads(line).get("exit-code", exit_code))
            except (ValueError, AttributeError):
                continue
        return ProcessResult(
            exit_code=exit_code,
            stdout=_text(out),
            stderr=_text(err),
            truncated=truncated,
        )

    def terminate(self) -> int:
        """SIGTERM the command and its descendants (the graceful signal). Returns the count.

        The outer bwrap and the inner init are excluded: a SIGTERM to either tears the whole
        namespace down at once, which is the hard kill, not the graceful one.
        """
        if self.proc.returncode is not None:
            return 0
        return signal_marked(
            SANDBOX_ENV,
            self.sandbox_id,
            signal.SIGTERM,
            exclude=frozenset({self.proc.pid, self.init_pid}),
        )

    async def kill(self, timeout_s: float) -> bool:
        """SIGKILL the outer bwrap and the inner init; True once the init is verified dead.

        False means the init did not exit within `timeout_s` although SIGKILL is pending, which
        only happens to a process stuck in uninterruptible I/O: it cannot run user code again and
        dies when the kernel releases it. Callers keep asking until it is verified.
        """
        if self.dead:
            return True
        if self.outer_pidfd is not None:
            with contextlib.suppress(OSError):
                self.outer_pidfd.send_signal(signal.SIGKILL)
        if self.init_pidfd is None:
            self.dead = True  # the init was already gone when the sandbox was recorded
            return True
        with contextlib.suppress(OSError):
            self.init_pidfd.send_signal(signal.SIGKILL)
        self.dead = await self.init_pidfd.wait_exit(timeout_s)
        if self.dead:
            self._close_fds()
        return self.dead

    def _close_fds(self) -> None:
        """Release the pidfds; the init's stays open until its death is verified."""
        if self.outer_pidfd is not None:
            self.outer_pidfd.close()
            self.outer_pidfd = None
        if self.dead and self.init_pidfd is not None:
            self.init_pidfd.close()
            self.init_pidfd = None


async def _abort(
    proc: asyncio.subprocess.Process,
    outer_pidfd: Pidfd | None,
    timeout_s: float,
    sandbox_id: str,
) -> str:
    """SIGKILL a starting sandbox (outer bwrap; the init dies with it) and reap it; stderr text.

    Bounded by `timeout_s` for the reap and again for the stderr read.
    """
    if outer_pidfd is not None:
        with contextlib.suppress(OSError):
            outer_pidfd.send_signal(signal.SIGKILL)
    if proc.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
    # A configured bwrap wrapper or bwrap's init can outlive the outer process. This is especially
    # important before --die-with-parent is armed: kill every descendant under its UUID-scoped
    # marker, using the same pidfd/recheck protocol as established sandboxes. The /proc walk
    # runs in a thread: its cost grows with the host's process count.
    await asyncio.to_thread(signal_marked, SANDBOX_ENV, sandbox_id, signal.SIGKILL)
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(proc.wait(), timeout_s)
    if proc.stderr is None:
        return ""
    with contextlib.suppress(OSError, ValueError, TimeoutError):
        data = await asyncio.wait_for(proc.stderr.read(_STDERR_CAP), timeout_s)
        return data.decode(errors="replace").strip()
    return ""


async def probe(
    bwrap_path: str, sandbox: Sandbox, worker: str, *, kill_timeout_s: float = 5.0
) -> None:
    """Fail closed: the sandbox must run `/bin/sh` (and `prlimit` when limits are set)."""
    try:
        sp = await SandboxProcess.spawn(
            bwrap_path=bwrap_path,
            sandbox=sandbox,
            argv=("/bin/sh", "-c", "echo FRONTA_SANDBOX_OK"),
            env=command_env(sandbox, worker=worker, sandbox_id="probe-" + uuid4().hex),
            kill_timeout_s=kill_timeout_s,
        )
        result = await asyncio.wait_for(sp.run(b"", 4096), _STATUS_LINE_TIMEOUT_S)
    except (SandboxError, TimeoutError) as exc:
        msg = f"sandbox probe failed: {exc}"
        raise SandboxError(msg) from exc
    if result.exit_code != 0 or result.stdout.encode() != PROBE_OUTPUT:
        msg = (
            f"sandbox probe failed: exit {result.exit_code},"
            f" stdout {result.stdout!r}, stderr {result.stderr.strip()[:500]!r}"
        )
        raise SandboxError(msg)
