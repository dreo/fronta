"""The worker: claim loop, attempt controller, heartbeats, reaper, purger, listener, shutdown.

One `Attempt` controls one claimed task. The handler (or sandbox) runs as its own task; the
controller races it against the attempt timeout and stop requests, so no handler behavior can
trap the controller. The stop cause is recorded before stopping and is the single source of the
outcome. Heartbeats run until the attempt's final transition has committed.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import random
import signal
import sys
import threading
import time
from contextlib import AbstractAsyncContextManager
from enum import StrEnum
from typing import TYPE_CHECKING, Any, cast

import psycopg
from pydantic import BaseModel

from fronta import codec, runtime, sandbox, store
from fronta.definitions import ProcessTaskDefinition, TaskDefinition
from fronta.errors import (
    ConfigurationError,
    InputValidationError,
    NonRetryableError,
    ProgressTooLarge,
    ResultSerializationError,
)
from fronta.executors import AsyncioExecution, Execution, ProcessExecution, ProcessFailed

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from datetime import datetime
    from uuid import UUID

    from psycopg_pool import AsyncConnectionPool

    from fronta.config import Settings
    from fronta.model import JSON, TaskRow

log = logging.getLogger("fronta.worker")

EXIT_FATAL = 70
"""Exit status when an asyncio handler ignored cancellation past the grace period."""

_TRANSITION_RETRY_S = 1.0
_TRANSITION_RETRY_MAX_S = 10.0
_WATCHDOG_TICK_S = 0.5
_FINAL_ERRORS = (NonRetryableError, InputValidationError, ResultSerializationError)


class Cause(StrEnum):
    TIMEOUT = "timeout"
    CANCEL = "cancel"
    SHUTDOWN = "shutdown"
    LOST = "lost"


class TaskLogAdapter(logging.LoggerAdapter[logging.Logger]):
    """Prefixes every line with the correlation fields and keeps them in `record.extra`."""

    def process(self, msg: str, kwargs: Any) -> tuple[str, Any]:  # noqa: ANN401  # stdlib signature
        extra = self.extra or {}
        kwargs["extra"] = {**extra, **kwargs.get("extra", {})}
        prefix = (
            f"[task={extra.get('task_id')} attempt={extra.get('attempt')}"
            f" type={extra.get('task_type')}]"
        )
        return f"{prefix} {msg}", kwargs


class TaskContext[StateT]:
    """`Context` implementation handed to handlers."""

    def __init__(self, attempt: Attempt, state: StateT) -> None:
        self._attempt = attempt
        self.task_id = attempt.row.id
        self.attempt = attempt.row.attempt
        self.state = state
        self.cancelled = asyncio.Event()
        self.log: logging.LoggerAdapter[logging.Logger] = TaskLogAdapter(
            log,
            {"task_id": self.task_id, "attempt": self.attempt, "task_type": attempt.row.type},
        )

    async def progress(self, value: JSON) -> None:
        worker = self._attempt.worker
        try:
            encoded = codec.encode_capped(value, worker.settings.progress_cap, "progress")
        except codec.OverCap as exc:
            raise ProgressTooLarge(str(exc)) from exc
        async with worker.pool.connection() as conn:
            stored = await store.set_progress(conn, self.task_id, self._attempt.token, encoded)
        if not stored:
            self._attempt.stop(Cause.LOST)

    async def enqueue[I: BaseModel](  # noqa: PLR0913  # public signature fixed by SPEC.md
        self,
        task: TaskDefinition[I, Any],
        input: I,
        *,
        priority: int = 0,
        run_at: datetime | None = None,
        key: str | None = None,
        concurrency_key: str | None = None,
    ) -> int:
        async with self._attempt.worker.pool.connection() as conn, conn.transaction():
            return await task.enqueue(
                input,
                conn=conn,
                priority=priority,
                run_at=run_at,
                key=key,
                concurrency_key=concurrency_key,
            )


class Attempt:
    """Controller of one claimed task from claim to its final transition."""

    def __init__(self, worker: Worker[Any], row: TaskRow) -> None:
        self.worker = worker
        self.row = row
        assert row.token is not None  # noqa: S101  # a claimed row always carries a token
        self.token = row.token
        self.cause: Cause | None = None
        self._stop_event = asyncio.Event()
        self._settled = False  # the final transition is done: heartbeats must end
        self.ctx = TaskContext(self, worker.state)
        self.execution = self._make_execution()
        self.fatal = False
        self.task: asyncio.Task[None] | None = None

    def _make_execution(self) -> Execution:
        definition = self.worker.definitions[self.row.type]
        settings = self.worker.settings
        if isinstance(definition, ProcessTaskDefinition):
            return ProcessExecution(
                definition,
                self.row,
                bwrap_path=settings.bwrap_path,
                worker=self.worker.worker_id,
                result_cap=settings.result_cap,
                error_cap=settings.error_cap,
                kill_timeout_s=settings.kill_timeout_s,
            )
        return AsyncioExecution(definition, self.row, self.ctx, settings.result_cap)

    def stop(self, cause: Cause) -> None:
        """Record the cause (first one wins) and ask the attempt to end."""
        if self.cause is None:
            self.cause = cause
            if cause is Cause.CANCEL:
                self.ctx.cancelled.set()
            self._stop_event.set()

    async def run(self) -> None:
        settings = self.worker.settings
        heartbeat = asyncio.create_task(self._heartbeats(), name=f"heartbeat-{self.row.id}")
        runner = asyncio.create_task(self.execution.run(), name=f"run-{self.row.id}")
        stop = asyncio.create_task(self._stop_event.wait(), name=f"stop-{self.row.id}")
        try:
            await asyncio.wait(
                {runner, stop},
                timeout=self.row.attempt_timeout_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
            stopped = not runner.done()
            if stopped:
                if self.cause is None:
                    self.cause = Cause.TIMEOUT
                self.ctx.log.info("stopping attempt: %s", self.cause)
                self._stop_execution()
                await asyncio.wait({runner}, timeout=settings.grace_s)
            if not runner.done():
                verified = await self._kill_execution()
                await asyncio.wait({runner}, timeout=settings.kill_timeout_s)
                if not runner.done():
                    self.fatal = isinstance(self.execution, AsyncioExecution)
                    self.ctx.log.error(
                        "attempt did not end after the grace period and the kill"
                        " (verified_dead=%s); %s",
                        verified,
                        "the worker will exit" if self.fatal else "cancelling the runner",
                    )
                    if not self.fatal:
                        # A cancelled process runner kills whatever its spawn started.
                        runner.cancel()
                        await asyncio.wait({runner}, timeout=settings.kill_timeout_s)
            if not isinstance(self.execution, AsyncioExecution):
                # The sandbox is verified dead before any outcome is written: a transition may
                # wait for the database, and nothing may run meanwhile. An init stuck in
                # uninterruptible I/O with SIGKILL pending keeps the attempt (and its slot) open
                # until the kernel releases it: "running" stays the honest state.
                while not await self._kill_execution():
                    self.ctx.log.error("sandbox not verified dead yet; waiting for it")
                    await asyncio.sleep(min(1.0, settings.kill_timeout_s))
            if stopped:
                await self._finish_stopped(runner)
            else:
                await self._finish_completed(runner)
        finally:
            if not isinstance(self.execution, AsyncioExecution):
                await self._end_process_runner(runner)
            stop.cancel()
            self._settled = True
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

    async def _end_process_runner(self, runner: asyncio.Task[str]) -> None:
        """Backstop for a controller that failed early: kill the sandbox, then await the runner."""
        await self._kill_execution()
        runner.cancel()
        while not runner.done():  # its cleanup is bounded by the configured timeouts
            await asyncio.wait({runner}, timeout=self.worker.settings.kill_timeout_s)
            if not runner.done():
                self.ctx.log.error("process runner still cleaning up; waiting for it")

    def _stop_execution(self) -> None:
        try:
            self.execution.stop()
        except OSError as exc:  # /proc or pidfd trouble: the hard kill is the backstop
            self.ctx.log.error("graceful stop failed: %s", exc)

    async def _kill_execution(self) -> bool:
        try:
            return await self.execution.kill(self.worker.settings.kill_timeout_s)
        except OSError as exc:
            self.ctx.log.error("kill failed: %s", exc)
            await asyncio.sleep(min(1.0, self.worker.settings.kill_timeout_s))  # never spin
            return False

    async def _finish_completed(self, runner: asyncio.Task[str]) -> None:
        """The execution ended on its own: its result or exception is the outcome."""
        if self.cause is Cause.LOST:
            self.ctx.log.warning("token lost; outcome discarded")
            return
        exc = None if runner.cancelled() else runner.exception()
        if exc is None and not runner.cancelled():
            result = runner.result()
            await self._transition(
                "succeed", lambda c: store.succeed(c, self.row.id, self.token, result)
            )
        elif isinstance(exc, _FINAL_ERRORS):
            error = codec.encode(codec.error_metadata(exc, self.worker.settings.error_cap))
            await self._transition(
                "fail (final)",
                lambda c: store.fail(c, self.row.id, self.token, error, retry=False),
            )
        else:
            if isinstance(exc, ProcessFailed):
                metadata = exc.metadata
            elif exc is None:
                metadata = {"type": "CancelledError", "message": "handler cancelled itself"}
            else:
                metadata = codec.error_metadata(exc, self.worker.settings.error_cap)
            failure = codec.encode(metadata)
            await self._transition(
                "fail", lambda c: store.fail(c, self.row.id, self.token, failure, retry=True)
            )

    async def _finish_stopped(self, runner: asyncio.Task[str]) -> None:
        """A stop was issued: the recorded cause is the outcome; the runner is only logged."""
        if runner.done() and not runner.cancelled() and runner.exception() is not None:
            self.ctx.log.info("runner ended after the stop: %r", runner.exception())
        if self.cause is Cause.TIMEOUT:
            metadata = {
                "type": "AttemptTimeout",
                "message": f"attempt exceeded {self.row.attempt_timeout_s} s",
            }
            timeout_error = codec.encode(metadata)
            await self._transition(
                "fail (timeout)",
                lambda c: store.fail(c, self.row.id, self.token, timeout_error, retry=True),
            )
        elif self.cause is Cause.CANCEL:
            await self._transition(
                "cancel ack", lambda c: store.ack_cancel(c, self.row.id, self.token)
            )
        elif self.cause is Cause.SHUTDOWN:
            await self._transition("release", lambda c: store.release(c, self.row.id, self.token))
        else:
            self.ctx.log.warning("token lost; outcome discarded")

    async def _transition(self, what: str, op: Callable[[store.Conn], Awaitable[object]]) -> None:
        """Apply the fenced transition, retrying connection errors until a definitive answer."""
        await fenced_write(self.worker.pool, self.ctx.log, what, op)

    async def _heartbeats(self) -> None:
        """Renew the lease until the attempt is settled or the lease is lost.

        The controller cancels this task; `_settled` is the backstop for a cancellation that a
        database library swallows inside `connection()` (psycopg-pool < 3.2.8 did), so a finished
        attempt can never keep heartbeating until the pool closes.
        """
        settings = self.worker.settings
        while not self._settled:
            await asyncio.sleep(settings.heartbeat_s)
            if self._settled:
                return
            try:
                async with self.worker.pool.connection() as conn:
                    beat = await store.heartbeat(conn, self.row.id, self.token, settings.lease_s)
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                self.ctx.log.warning("heartbeat failed: %s", exc)
                continue
            if beat is store.Heartbeat.LOST:
                self.ctx.log.warning("heartbeat rejected: lease lost")
                self.stop(Cause.LOST)
                return  # the lease is gone; further beats would only be rejected
            if beat is store.Heartbeat.CANCEL_REQUESTED:
                self.stop(Cause.CANCEL)


class Worker[StateT]:
    """Runs the listed task types until SIGTERM/SIGINT; `run()` returns the exit code."""

    def __init__(
        self,
        tasks: Sequence[TaskDefinition[Any, Any]],
        *,
        lifespan: Callable[[Worker[StateT]], AbstractAsyncContextManager[StateT]] | None = None,
        settings: Settings | None = None,
    ) -> None:
        if not tasks:
            msg = "a worker needs at least one task definition"
            raise ConfigurationError(msg)
        self.definitions: dict[str, TaskDefinition[Any, Any]] = {}
        for definition in tasks:
            if definition.name in self.definitions:
                msg = f"duplicate task type {definition.name!r}"
                raise ConfigurationError(msg)
            self.definitions[definition.name] = definition
        self.lifespan = lifespan
        self._settings = settings
        self.state: StateT = cast("StateT", None)
        self.worker_id = sandbox.worker_id()
        self.attempts: dict[int, Attempt] = {}
        self._pool: AsyncConnectionPool[Any] | None = None
        self._wake = asyncio.Event()
        self._stopping = asyncio.Event()
        self.started = asyncio.Event()
        self._immediate = asyncio.Event()
        self._exit_code = 0
        self._last_tick = time.monotonic()
        self._watchdog_stop = threading.Event()
        self._signals = 0

    @property
    def settings(self) -> Settings:
        """Resolved on first use: importing a module that builds a Worker needs no environment."""
        if self._settings is None:
            self._settings = runtime.get_settings()
        return self._settings

    @property
    def pool(self) -> AsyncConnectionPool[Any]:
        if self._pool is None:
            msg = "worker is not running"
            raise RuntimeError(msg)
        return self._pool

    # ------------------------------------------------------------------ lifecycle

    def request_shutdown(self) -> None:
        """Begin a graceful shutdown; a second call skips the grace wait."""
        self._signals += 1
        if self._signals > 1:
            self._immediate.set()
        log.info("shutdown requested (%s)", "immediate" if self._immediate.is_set() else "graceful")
        self._stopping.set()
        self._wake.set()

    async def run(self) -> int:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.request_shutdown)
        try:
            return await self._run()
        finally:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(sig)

    async def _run(self) -> int:
        settings = self.settings
        runtime.configure(settings)
        log.info("worker %s starting: types=%s", self.worker_id, sorted(self.definitions))
        self._pool = runtime.make_pool(settings, application_name="fronta-worker")
        try:
            await runtime.open_ready(self._pool, settings.connect_timeout_s)
            await self._start_checks()
            async with self._lifespan_cm() as state:
                self.state = state
                await self._serve()
        finally:
            await self._pool.close()
            self._pool = None
        log.info("worker %s stopped (exit %s)", self.worker_id, self._exit_code)
        if self._exit_code == EXIT_FATAL:
            # A coroutine that ignores cancellation would also block the event loop's own
            # teardown: the only deterministic way out is a hard exit after our cleanup.
            logging.shutdown()
            os._exit(EXIT_FATAL)
        return self._exit_code

    def _lifespan_cm(self) -> AbstractAsyncContextManager[Any]:
        if self.lifespan is None:
            return contextlib.nullcontext(None)
        return self.lifespan(self)

    async def _start_checks(self) -> None:
        async with self.pool.connection() as conn:
            for definition in self.definitions.values():
                spec = definition.spec
                previous = await store.publish_task_type(conn, spec)
                if previous is not None and previous != spec.fingerprint:
                    log.warning(
                        "definition of %r changed (fingerprint %s -> %s): last writer wins",
                        definition.name,
                        previous[:12],
                        spec.fingerprint[:12],
                    )
        probed: set[tuple[Any, ...]] = set()
        for definition in self.definitions.values():
            if isinstance(definition, ProcessTaskDefinition):
                identity = definition.sandbox.identity()
                if identity not in probed:
                    await sandbox.probe(
                        self.settings.bwrap_path,
                        definition.sandbox,
                        self.worker_id,
                        kill_timeout_s=self.settings.kill_timeout_s,
                    )
                    probed.add(identity)
        if probed:
            await self._scavenge()

    async def _scavenge(self) -> None:
        try:
            killed = await asyncio.to_thread(sandbox.scavenge_orphans)
        except OSError as exc:
            log.warning("scavenger failed: %s", exc)
            return
        if killed:
            log.warning("scavenged %d orphaned sandbox processes", killed)

    async def _serve(self) -> None:
        # A Worker may be constructed long before it starts. The watchdog measures event-loop
        # progress while serving, so its baseline must begin here rather than in __init__.
        self._last_tick = time.monotonic()
        watchdog = threading.Thread(target=self._watchdog, name="fronta-watchdog", daemon=True)
        watchdog.start()
        loops = [
            asyncio.create_task(self._tick(), name="tick"),
            asyncio.create_task(self._listen(), name="listener"),
            asyncio.create_task(self._reaper(), name="reaper"),
            asyncio.create_task(self._purger(), name="purger"),
        ]
        for loop_task in loops:
            loop_task.add_done_callback(_report_loop_end)
        self.started.set()
        try:
            await self._claim_loop()
            await self._drain()
        finally:
            for loop_task in loops:
                loop_task.cancel()
            await asyncio.gather(*loops, return_exceptions=True)
            self._watchdog_stop.set()

    async def _drain(self) -> None:
        """Let running attempts finish within the grace period, then stop and settle the rest.

        Settling means each attempt's stop protocol (grace, kill) plus its fenced transition. A
        graceful shutdown waits for the transition without a deadline: only the database can hold
        it, and a completed attempt is never left unrecorded. A second signal skips the handler
        grace, waits out the stop protocol, then cancels whatever is still unsettled (a
        cancelled controller still kills its sandbox on the way out) before the worker exits.
        """
        settings = self.settings
        pending = {a.task for a in self.attempts.values() if a.task is not None}
        if pending and not self._immediate.is_set():
            log.info(
                "waiting up to %.0fs for %d running attempt(s)", settings.grace_s, len(pending)
            )
            await self._wait_all(pending, settings.grace_s, stop_on_immediate=True)
        for attempt in list(self.attempts.values()):
            attempt.stop(Cause.SHUTDOWN)
        pending = {a.task for a in self.attempts.values() if a.task is not None}
        if not pending:
            return
        if not self._immediate.is_set():
            await self._wait_all(pending, None, stop_on_immediate=True)
        pending = {a.task for a in self.attempts.values() if a.task is not None}
        if pending:  # a second signal: wait out the stop protocol, then cut the rest loose
            await self._wait_all(pending, self._stop_budget_s(), stop_on_immediate=False)
        pending = {a.task for a in self.attempts.values() if a.task is not None}
        if pending:
            left = sorted(self.attempts)
            log.error("immediate shutdown abandoned unsettled attempt(s): %s", left)
            for task in pending:
                task.cancel()
            await self._wait_all(pending, self._stop_budget_s(), stop_on_immediate=False)

    def _stop_budget_s(self) -> float:
        """Upper bound of one attempt's stop protocol.

        Grace, then kill + runner wait + runner cancellation (with its own kill), then the
        pre-transition verification, then the final kill and runner wait: six kill timeouts.
        """
        return self.settings.grace_s + 6 * self.settings.kill_timeout_s + 1.0

    async def _wait_all(
        self, tasks: set[asyncio.Task[None]], timeout: float | None, *, stop_on_immediate: bool
    ) -> None:
        """Wait for all `tasks`, the timeout (if any), or — optionally — a second signal."""
        deadline = None if timeout is None else time.monotonic() + timeout
        immediate = asyncio.create_task(self._immediate.wait())
        watched = {*tasks, immediate} if stop_on_immediate else set(tasks)
        try:
            while tasks and not (stop_on_immediate and immediate.done()):
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return
                done, _ = await asyncio.wait(
                    watched, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
                )
                tasks -= done
                watched -= done
        finally:
            immediate.cancel()

    # ------------------------------------------------------------------ loops

    async def _claim_loop(self) -> None:
        """Fill free slots; claims run concurrently up to the pool size, one transaction each.

        Rows that a claim returns after the shutdown signal landed are released, never started.
        """
        settings = self.settings
        types = sorted(self.definitions)
        parallel = max(1, settings.pool_size - 1)
        while not self._stopping.is_set():
            free = settings.concurrency - len(self.attempts)
            if free > 0:
                rows = await asyncio.gather(
                    *(self._claim(types) for _ in range(min(free, parallel)))
                )
                claimed = [row for row in rows if row is not None]
                if self._stopping.is_set():
                    await self._release_unstarted(claimed)
                    return
                for row in claimed:
                    self._start_attempt(row)
                if claimed:
                    continue
            self._wake.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), settings.poll_interval_s)

    async def _release_unstarted(self, rows: list[TaskRow]) -> None:
        """Rows claimed while the shutdown signal landed go straight back to the queue."""
        for row in rows:
            if row.token is None:  # pragma: no cover  # a claimed row always carries a token
                continue
            await fenced_write(
                self.pool,
                log,
                f"release of unstarted task {row.id}",
                _release_op(row.id, row.token),
                abort=self._immediate,
            )

    async def _claim(self, types: list[str]) -> TaskRow | None:
        try:
            async with self.pool.connection() as conn:
                return await store.claim(
                    conn,
                    types=types,
                    worker=self.worker_id,
                    lease_s=self.settings.lease_s,
                    deadline_s=self.settings.poll_interval_s,
                )
        except psycopg.Error as exc:
            log.warning("claim failed: %s", exc)
            await asyncio.sleep(_TRANSITION_RETRY_S)
            return None

    def _start_attempt(self, row: TaskRow) -> None:
        attempt = Attempt(self, row)
        self.attempts[row.id] = attempt
        attempt.ctx.log.info("claimed")
        attempt.task = asyncio.create_task(attempt.run(), name=f"attempt-{row.id}")
        attempt.task.add_done_callback(lambda _: self._attempt_done(attempt))

    def _attempt_done(self, attempt: Attempt) -> None:
        self.attempts.pop(attempt.row.id, None)
        task = attempt.task
        if task is not None and not task.cancelled() and task.exception() is not None:
            attempt.ctx.log.error("attempt controller crashed", exc_info=task.exception())
        if attempt.fatal:
            log.critical("handler of task %s ignored cancellation; exiting", attempt.row.id)
            self._exit_code = EXIT_FATAL
            if not self._stopping.is_set():
                self.request_shutdown()
        self._wake.set()

    async def _listen(self) -> None:
        settings = self.settings
        while True:
            try:
                async with await psycopg.AsyncConnection.connect(
                    settings.dsn,
                    autocommit=True,
                    connect_timeout=max(1, math.ceil(settings.connect_timeout_s)),
                    application_name="fronta-listener",
                ) as conn:
                    await conn.execute(
                        f"LISTEN {store.WAKE_CHANNEL}; LISTEN {store.CANCEL_CHANNEL}"
                    )
                    log.info("listening for notifications")
                    while True:
                        async for notice in conn.notifies(timeout=settings.poll_interval_s):
                            self._on_notify(notice.channel, notice.payload)
            except (psycopg.Error, OSError) as exc:
                log.warning("listener lost the connection (%s); reconnecting", exc)
                await asyncio.sleep(_TRANSITION_RETRY_S)

    def _on_notify(self, channel: str, payload: str) -> None:
        if channel == store.WAKE_CHANNEL:
            if not payload or payload in self.definitions:
                self._wake.set()
        elif channel == store.CANCEL_CHANNEL:
            with contextlib.suppress(ValueError):
                attempt = self.attempts.get(int(payload))
                if attempt is not None:
                    attempt.stop(Cause.CANCEL)

    async def _reaper(self) -> None:
        settings = self.settings
        has_processes = any(isinstance(d, ProcessTaskDefinition) for d in self.definitions.values())
        while True:
            await asyncio.sleep(settings.reaper_interval_s * random.uniform(0.8, 1.2))  # noqa: S311
            try:
                async with self.pool.connection() as conn:
                    reaped = await store.reap(conn)
                for task_id, task_type, state in reaped:
                    log.warning(
                        "reaped task %s (%s): lease expired -> %s", task_id, task_type, state
                    )
                if has_processes:
                    await self._scavenge()
            except (psycopg.Error, OSError) as exc:
                log.warning("reaper failed: %s", exc)

    async def _purger(self) -> None:
        settings = self.settings
        while True:
            await asyncio.sleep(settings.purge_interval_s * random.uniform(0.8, 1.2))  # noqa: S311
            try:
                deleted = 0
                while True:
                    async with self.pool.connection() as conn:
                        batch = await store.purge_tasks(
                            conn, settings.retention_s, settings.purge_batch
                        )
                    deleted += batch
                    if batch < settings.purge_batch:
                        break
                if deleted:
                    log.info("purged %d terminal task(s)", deleted)
            except (psycopg.Error, OSError) as exc:
                log.warning("purge failed: %s", exc)

    async def _tick(self) -> None:
        while True:
            self._last_tick = time.monotonic()
            await asyncio.sleep(_WATCHDOG_TICK_S)

    def _watchdog(self) -> None:
        """Abort the process when the event loop stops running (a handler blocked it).

        The stall must be seen twice in a row: after a SIGSTOP/SIGCONT this thread may wake before
        the loop's tick task, and a resumed loop catches up within milliseconds.
        """
        stalls = 0
        while not self._watchdog_stop.wait(_WATCHDOG_TICK_S):
            stalled = time.monotonic() - self._last_tick
            stalls = stalls + 1 if stalled > self.settings.lease_s else 0
            if stalls >= 2:  # noqa: PLR2004  # two consecutive observations
                sys.stderr.write(
                    f"fronta worker {self.worker_id}: event loop blocked for {stalled:.0f}s;"
                    f" aborting (exit {EXIT_FATAL})\n"
                )
                sys.stderr.flush()
                os._exit(EXIT_FATAL)


def _report_loop_end(task: asyncio.Task[None]) -> None:
    """A background loop must only end by cancellation; anything else is a worker bug."""
    if not task.cancelled() and task.exception() is not None:
        log.critical("background loop %s died", task.get_name(), exc_info=task.exception())


async def fenced_write(
    pool: AsyncConnectionPool[Any],
    log_: logging.Logger | logging.LoggerAdapter[logging.Logger],
    what: str,
    op: Callable[[store.Conn], Awaitable[object]],
    *,
    abort: asyncio.Event | None = None,
) -> None:
    """Run a fenced write, retrying connection errors until the database gives a definitive answer.

    A definitive answer is 1 row (applied) or 0 rows (the token is gone: the reaper acted while
    the database was unreachable). Anything else is a bug and is logged, not retried. `abort`
    (an immediate shutdown) ends the retries at the next check and cuts a retry sleep short;
    cancellation ends them at once.
    """
    delay = _TRANSITION_RETRY_S
    while True:
        if abort is not None and abort.is_set():
            log_.error("%s abandoned by an immediate shutdown", what)
            return
        try:
            async with pool.connection() as conn:
                outcome = await op(conn)
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            log_.warning("%s failed (%s); retrying in %.0fs", what, exc, delay)
            if abort is None:
                await asyncio.sleep(delay)
            else:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(abort.wait(), delay)
            delay = min(delay * 2, _TRANSITION_RETRY_MAX_S)
            continue
        except psycopg.Error:
            log_.exception("%s failed permanently; the reaper will requeue", what)
            return
        if outcome is None or outcome is False:
            log_.warning("%s rejected: token no longer valid", what)
        else:
            log_.info("%s -> %s", what, outcome)
        return


def _release_op(task_id: int, token: UUID) -> Callable[[store.Conn], Awaitable[object]]:
    async def op(conn: store.Conn) -> object:
        return await store.release(conn, task_id, token)

    return op
