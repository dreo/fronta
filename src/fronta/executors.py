"""The two ways to run an attempt behind one protocol: an asyncio handler or a sandboxed process."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, Protocol
from uuid import uuid4

from pydantic import BaseModel, ValidationError

from fronta import codec
from fronta.errors import InputValidationError, ResultSerializationError, SandboxError
from fronta.sandbox import SandboxProcess, command_env

if TYPE_CHECKING:
    from fronta.definitions import Context, ProcessTaskDefinition, TaskDefinition
    from fronta.model import TaskRow


class Execution(Protocol):
    """One attempt. `run()` returns the encoded JSON result or raises."""

    async def run(self) -> str: ...

    def stop(self) -> None:
        """Ask the attempt to end (cancel the handler / SIGTERM the sandbox). Idempotent."""

    async def kill(self, timeout_s: float) -> bool:
        """Force the attempt to end; True when it is verified over (always True for asyncio)."""


class ProcessFailed(Exception):
    """A process attempt ended with a non-zero exit code; `metadata` is the error object."""

    def __init__(self, metadata: dict[str, Any]) -> None:
        super().__init__(f"exit code {metadata.get('exit_code')}")
        self.metadata = metadata


def validate_input(definition: TaskDefinition[Any, Any], row: TaskRow) -> BaseModel:
    try:
        model: BaseModel = definition.input_model.model_validate(row.input)
    except ValidationError as exc:
        msg = f"input of task {row.id} does not match {definition.input_model.__name__}: {exc}"
        raise InputValidationError(msg) from exc
    return model


def encode_result(definition: TaskDefinition[Any, Any], value: object, cap: int) -> str:
    """Validate against the output model (if any) and encode; anything else is a final failure."""
    try:
        if definition.output_model is not None:
            value = definition.output_model.model_validate(value).model_dump(mode="json")
        elif isinstance(value, BaseModel):
            value = value.model_dump(mode="json")
        return codec.encode_capped(value, cap, "result")  # type: ignore[arg-type]  # checked by dumps
    except (ValidationError, TypeError, ValueError) as exc:
        msg = f"result of task {definition.name!r} cannot be stored: {exc}"
        raise ResultSerializationError(msg) from exc


class AsyncioExecution:
    def __init__(
        self, definition: TaskDefinition[Any, Any], row: TaskRow, ctx: Context[Any], result_cap: int
    ) -> None:
        self.definition = definition
        self.row = row
        self.ctx = ctx
        self.result_cap = result_cap
        self._task: asyncio.Task[Any] | None = None

    async def run(self) -> str:
        self._task = asyncio.current_task()
        handler = self.definition.handler
        if handler is None:  # pragma: no cover  # only process definitions lack a handler
            msg = f"task type {self.definition.name!r} has no handler"
            raise RuntimeError(msg)
        model = validate_input(self.definition, self.row)
        value = await handler(self.ctx, model)
        return encode_result(self.definition, value, self.result_cap)

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()

    async def kill(self, timeout_s: float) -> bool:  # noqa: ARG002  # protocol signature
        return True  # a coroutine cannot be killed; the worker decides what that means


class ProcessExecution:
    def __init__(  # noqa: PLR0913  # every argument is a distinct input of the attempt
        self,
        definition: ProcessTaskDefinition[Any],
        row: TaskRow,
        *,
        bwrap_path: str,
        worker: str,
        result_cap: int,
        error_cap: int,
        kill_timeout_s: float,
    ) -> None:
        self.definition = definition
        self.row = row
        self.bwrap_path = bwrap_path
        self.worker = worker
        self.result_cap = result_cap
        self.error_cap = error_cap
        self.kill_timeout_s = kill_timeout_s
        self.sandbox_id = uuid4().hex
        self._proc: SandboxProcess | None = None
        self._stopped = False
        self._spawning = False
        self._spawned = asyncio.Event()

    async def run(self) -> str:
        model = validate_input(self.definition, self.row)
        stdin = codec.encode(model.model_dump(mode="json")).encode("utf-8")
        self._spawning = True
        try:
            self._proc = await SandboxProcess.spawn(
                bwrap_path=self.bwrap_path,
                sandbox=self.definition.sandbox,
                argv=self.definition.argv,
                env=command_env(
                    self.definition.sandbox,
                    worker=self.worker,
                    sandbox_id=self.sandbox_id,
                    task_env={
                        "FRONTA_TASK_ID": str(self.row.id),
                        "FRONTA_ATTEMPT": str(self.row.attempt),
                    },
                ),
                kill_timeout_s=self.kill_timeout_s,
            )
        except SandboxError as exc:
            raise ProcessFailed({"type": "SandboxError", "message": str(exc)}) from exc
        finally:
            self._spawning = False
            self._spawned.set()
        if self._stopped:  # stop() arrived while spawning
            self.stop()
        result = await self._proc.run(stdin, self.definition.sandbox.max_output_bytes)
        if result.exit_code != 0:
            metadata: dict[str, Any] = {
                "type": "ProcessFailed",
                "message": f"exit code {result.exit_code}",
                **result.to_json(),
            }
            raise ProcessFailed(
                codec.truncate(metadata, self.error_cap, keep_tail=("stdout", "stderr"))
            )
        return encode_result(self.definition, result.to_json(), self.result_cap)

    def stop(self) -> None:
        self._stopped = True
        if self._proc is not None:
            with contextlib.suppress(OSError):  # best effort: the hard kill is the backstop
                self._proc.terminate()

    async def kill(self, timeout_s: float) -> bool:
        """Hard-kill the sandbox; True once it is verified dead.

        A spawn still in flight is waited for (its cancellation cleanup kills what it started);
        this never returns without awaiting, so a caller may loop on it.
        """
        if self._spawning:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._spawned.wait(), timeout_s)
            if self._spawning:
                return False
        if self._proc is None:
            return True
        return await self._proc.kill(timeout_s)
