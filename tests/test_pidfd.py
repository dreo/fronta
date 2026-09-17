"""Native and syscall process handles have the same lifecycle and error semantics."""

import asyncio
import ctypes
import errno
import os
import signal
import sys

import pytest

from fronta import sandbox


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("error", [None, errno.ESRCH, errno.EPERM, errno.ENOSYS])
def test_open_preserves_process_lookup_and_system_errors(monkeypatch, native, error):
    def open_native(_pid):
        if error is not None:
            raise OSError(error, os.strerror(error))
        return 123

    def syscall(_number, *_args):
        ctypes.set_errno(error or 0)
        return -1 if error else 123

    monkeypatch.setattr(sandbox, "_syscall", syscall)
    if native:
        monkeypatch.setattr(os, "pidfd_open", open_native, raising=False)
    else:
        monkeypatch.delattr(os, "pidfd_open", raising=False)
    if error in (None, errno.ESRCH):
        handle = sandbox.Pidfd.open(42)
        assert (handle.fd if handle else None) == (123 if error is None else None)
    else:
        with pytest.raises(OSError, match=os.strerror(error)) as raised:
            sandbox.Pidfd.open(42)
        assert raised.value.errno == error


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("error", [None, errno.ESRCH, errno.EPERM, errno.ENOSYS])
def test_signal_preserves_process_lookup_and_system_errors(monkeypatch, native, error):
    def send_native(_fd, _sig):
        if error is not None:
            raise OSError(error, os.strerror(error))

    def syscall(_number, *_args):
        ctypes.set_errno(error or 0)
        return -1 if error else 0

    monkeypatch.setattr(sandbox, "_syscall", syscall)
    if native:
        monkeypatch.setattr(signal, "pidfd_send_signal", send_native, raising=False)
    else:
        monkeypatch.delattr(signal, "pidfd_send_signal", raising=False)
    handle = sandbox.Pidfd(123)
    if error in (None, errno.ESRCH):
        assert handle.send_signal(signal.SIGTERM) is (error is None)
    else:
        with pytest.raises(OSError, match=os.strerror(error)) as raised:
            handle.send_signal(signal.SIGTERM)
        assert raised.value.errno == error
    handle.fd = -1
    assert handle.send_signal(signal.SIGTERM) is False


@pytest.mark.skipif(sys.platform != "linux", reason="real Linux process handles")
@pytest.mark.parametrize("missing", [(), ("open",), ("signal",), ("open", "signal")])
@pytest.mark.parametrize("close_while_waiting", [False, True])
async def test_real_process_signal_wait_and_close(monkeypatch, missing, close_while_waiting):
    proc = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(60)")
    handle = None
    try:
        # Let asyncio register its own child watcher before hiding APIs from Fronta.
        if "open" in missing:
            monkeypatch.delattr(os, "pidfd_open", raising=False)
        if "signal" in missing:
            monkeypatch.delattr(signal, "pidfd_send_signal", raising=False)
        handle = sandbox.Pidfd.open(proc.pid)
        assert handle is not None
        assert not os.get_inheritable(handle.fd)
        assert await handle.wait_exit(0.01) is False
        waiting = asyncio.create_task(handle.wait_exit(5))
        await asyncio.sleep(0)  # register the reader before testing a concurrent close
        if close_while_waiting:
            handle.close()
            assert handle.fd >= 0
        assert handle.send_signal(signal.SIGTERM)
        assert await waiting
        assert await proc.wait() == -signal.SIGTERM
        assert handle.send_signal(signal.SIGTERM) is False
        handle.close()
        assert handle.fd == -1
        assert await handle.wait_exit(0) is True
    finally:
        if proc.returncode is None:
            proc.kill()
        await proc.wait()
        if handle is not None:
            handle.close()
