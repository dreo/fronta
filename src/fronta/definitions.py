"""Task definitions: the `task` / `process_task` decorators, `Sandbox`, and `enqueue`.

Stored inputs are the model's JSON-mode dump by alias (the shape the published validation schema
describes and the server stores), made round-trippable (`round_trip=True`, so `Json[T]` fields
stay JSON text). Workers validate stored inputs in JSON mode accepting both aliases and field
names, so strict types (datetime, UUID, ...) and aliased fields survive the queue. An input that
would not validate again is rejected at enqueue with `InvalidInput` rather than failing the task
permanently at claim time.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol

from psycopg.pq import TransactionStatus
from pydantic import BaseModel, ValidationError

from fronta import codec, runtime, store
from fronta.errors import InvalidInput, PayloadTooLarge, TaskNotFound
from fronta.model import Backoff, Executor, NewTask, Policy, Sandbox, TaskRow, TaskTypeSpec

if TYPE_CHECKING:
    import asyncio
    import logging

    from fronta.config import Settings
    from fronta.model import JSON


class Context[StateT](Protocol):
    """What a handler receives. Implemented by the worker."""

    task_id: int
    attempt: int
    state: StateT
    log: logging.LoggerAdapter[logging.Logger]
    metadata: JSON
    cancelled: asyncio.Event

    async def progress(self, value: JSON) -> None:
        """Store progress (any JSON value up to the progress cap; over-cap raises)."""

    async def enqueue[I: BaseModel](  # noqa: PLR0913  # public signature fixed by SPEC.md
        self,
        task: TaskDefinition[I, Any],
        input: I,
        *,
        priority: int = 0,
        run_at: datetime | None = None,
        key: str | None = None,
        concurrency_key: str | None = None,
        metadata: JSON = None,
    ) -> int:
        """Enqueue another task, immediately and independently of this task's outcome."""


type Handler[InputT: BaseModel, OutputT] = Callable[[Context[Any], InputT], Awaitable[OutputT]]


def _seconds(value: float | timedelta) -> float:
    return value.total_seconds() if isinstance(value, timedelta) else float(value)


def dump_input(model: BaseModel) -> dict[str, Any]:
    """The stored representation of a validated input (see the module docstring)."""
    value = model.model_dump(mode="json", by_alias=True, round_trip=True)
    if not isinstance(value, dict):
        msg = f"input of {type(model).__name__} must serialize to a JSON object"
        raise InvalidInput(msg)
    return value


def load_input[M: BaseModel](model: type[M], text: str) -> M:
    """Validate a stored input (JSON text) in JSON mode, accepting aliases and field names."""
    return model.model_validate_json(text, by_alias=True, by_name=True)


class TaskDefinition[InputT: BaseModel, OutputT]:
    """An asyncio task type: name, models, policy and the handler."""

    executor = Executor.ASYNCIO

    def __init__(
        self,
        name: str,
        *,
        input_model: type[InputT],
        output_model: type[BaseModel] | None,
        policy: Policy,
        handler: Handler[InputT, OutputT] | None,
    ) -> None:
        store.check_name(name)
        self.name = name
        self.input_model = input_model
        self.output_model = output_model
        self.policy = policy
        self.handler = handler

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.name!r}>"

    @property
    def spec(self) -> TaskTypeSpec:
        return TaskTypeSpec(
            name=self.name,
            executor=self.executor,
            input_schema=self.input_model.model_json_schema(mode="validation"),
            output_schema=(
                None
                if self.output_model is None
                else self.output_model.model_json_schema(mode="serialization")
            ),
            policy=self.policy,
        )

    def encode_input(self, input: InputT | Mapping[str, Any], cap: int) -> str:
        """Validate against the input model and encode; enforce the payload cap.

        The encoded text is validated once more the way a worker will validate it, so an input
        that cannot round-trip (a serializer that changes shape, aliases that differ between
        validation and serialization) is refused here instead of failing the task at claim.
        """
        model = (
            input if isinstance(input, self.input_model) else self.input_model.model_validate(input)
        )
        try:
            text = codec.encode_capped(dump_input(model), cap, "payload")
        except codec.OverCap as exc:
            raise PayloadTooLarge(str(exc)) from exc
        except (codec.Unstorable, TypeError) as exc:
            msg = f"input of {self.name!r} cannot be stored: {exc}"
            raise InvalidInput(msg) from exc
        try:
            load_input(self.input_model, text)
        except ValidationError as exc:
            msg = (
                f"input of {self.name!r} does not survive the queue round trip through"
                f" {self.input_model.__name__}: {exc}"
            )
            raise InvalidInput(msg) from exc
        return text

    async def enqueue(  # noqa: PLR0913  # public signature fixed by SPEC.md section 3
        self,
        input: InputT | Mapping[str, Any],
        *,
        conn: store.Conn | None = None,
        priority: int = 0,
        run_at: datetime | None = None,
        key: str | None = None,
        concurrency_key: str | None = None,
        metadata: JSON = None,
    ) -> int:
        """Enqueue with SDK settings; a supplied transaction keeps its commit/rollback boundary."""
        return await self.enqueue_with(
            runtime.get_settings(),
            input,
            conn=conn,
            priority=priority,
            run_at=run_at,
            key=key,
            concurrency_key=concurrency_key,
            metadata=metadata,
        )

    async def enqueue_with(  # noqa: PLR0913  # the public signature plus the settings
        self,
        settings: Settings,
        input: InputT | Mapping[str, Any],
        *,
        conn: store.Conn | None = None,
        priority: int = 0,
        run_at: datetime | None = None,
        key: str | None = None,
        concurrency_key: str | None = None,
        metadata: JSON = None,
    ) -> int:
        """`enqueue()` with explicit settings for the caps and the dedupe deadline.

        A worker context uses its own worker's settings here, so its caps never depend on the
        process-global SDK configuration.
        """
        new_task = NewTask(
            type=self.name,
            input_json=self.encode_input(input, settings.payload_cap),
            policy=self.policy,
            priority=priority,
            run_at=run_at,
            key=key,
            concurrency_key=concurrency_key,
            metadata_json=encode_metadata(metadata, settings.progress_cap),
        )
        deadline = settings.statement_timeout_s
        if conn is not None:
            task_id = await store.enqueue(conn, new_task, deadline_s=deadline)
            if conn.autocommit and conn.info.transaction_status == TransactionStatus.IDLE:
                hints = runtime.hints(settings, conn=conn)
                hints.wake([self.name])
                hints.feed(["queued"])
            return task_id
        pool = await runtime.open_pool()
        async with pool.connection() as own_conn:
            task_id = await store.enqueue(own_conn, new_task, deadline_s=deadline)
        hints = runtime.hints(settings)
        hints.wake([self.name])
        hints.feed(["queued"])
        return task_id


class ProcessTaskDefinition[InputT: BaseModel](TaskDefinition[InputT, dict[str, Any]]):
    """A sandboxed executable: input on stdin, result `{exit_code, stdout, stderr, truncated}`."""

    executor = Executor.PROCESS

    def __init__(
        self,
        name: str,
        *,
        argv: tuple[str, ...],
        input_model: type[InputT],
        policy: Policy,
        sandbox: Sandbox,
    ) -> None:
        if not argv:
            msg = "argv must not be empty"
            raise ValueError(msg)
        super().__init__(
            name, input_model=input_model, output_model=None, policy=policy, handler=None
        )
        self.argv = argv
        self.sandbox = sandbox


def _policy(
    max_attempts: int,
    attempt_timeout: float | timedelta,
    backoff: Backoff,
    max_concurrency: int | None,
    max_concurrency_per_key: int | None,
) -> Policy:
    return Policy(
        max_attempts=max_attempts,
        attempt_timeout_s=_seconds(attempt_timeout),
        backoff=backoff,
        max_concurrency=max_concurrency,
        max_concurrency_per_key=max_concurrency_per_key,
    )


def task[InputT: BaseModel, OutputT](  # noqa: PLR0913  # public signature fixed by SPEC.md
    name: str,
    *,
    input: type[InputT],
    output: type[BaseModel] | None = None,
    max_attempts: int = 3,
    attempt_timeout: float | timedelta = 3600.0,
    backoff: Backoff | None = None,
    max_concurrency: int | None = None,
    max_concurrency_per_key: int | None = None,
) -> Callable[[Handler[InputT, OutputT]], TaskDefinition[InputT, OutputT]]:
    """Declare an asyncio task type. Decorates `async def handler(ctx, input) -> output`."""
    policy = _policy(
        max_attempts,
        attempt_timeout,
        backoff or Backoff(),
        max_concurrency,
        max_concurrency_per_key,
    )

    def decorate(handler: Handler[InputT, OutputT]) -> TaskDefinition[InputT, OutputT]:
        return TaskDefinition(
            name, input_model=input, output_model=output, policy=policy, handler=handler
        )

    return decorate


def process_task[InputT: BaseModel](  # noqa: PLR0913  # public signature fixed by SPEC.md
    name: str,
    argv: tuple[str, ...] | list[str],
    *,
    input: type[InputT],
    sandbox: Sandbox | None = None,
    max_attempts: int = 3,
    attempt_timeout: float | timedelta = 3600.0,
    backoff: Backoff | None = None,
    max_concurrency: int | None = None,
    max_concurrency_per_key: int | None = None,
) -> ProcessTaskDefinition[InputT]:
    """Declare a sandboxed process task type. `argv[0]` is resolved inside the sandbox."""
    policy = _policy(
        max_attempts,
        attempt_timeout,
        backoff or Backoff(),
        max_concurrency,
        max_concurrency_per_key,
    )
    return ProcessTaskDefinition(
        name, argv=tuple(argv), input_model=input, policy=policy, sandbox=sandbox or Sandbox()
    )


async def get_task(task_id: int, *, conn: store.Conn | None = None) -> TaskRow:
    """Return a task's latest durable row; raise :class:`TaskNotFound` when it is absent."""
    if conn is None:
        pool = await runtime.open_pool()
        async with pool.connection() as own_conn:
            row = await store.get_task(own_conn, task_id)
    else:
        row = await store.get_task(conn, task_id)
    if row is None:
        msg = f"no task {task_id}"
        raise TaskNotFound(msg)
    return row


def encode_metadata(value: JSON, cap: int) -> str | None:
    if value is None:
        return None
    try:
        return codec.encode_capped(value, cap, "metadata")
    except codec.OverCap as exc:
        raise PayloadTooLarge(str(exc)) from exc
    except (codec.Unstorable, TypeError) as exc:
        raise InvalidInput(str(exc)) from exc


async def _pause(task_type: str, paused: bool) -> None:
    pool = await runtime.open_pool()
    async with pool.connection() as conn:
        await store.set_paused(conn, task_type, paused)
    if not paused:
        runtime.hints().wake([task_type])


async def pause(task_type: str) -> None:
    """Stop new admissions of a type without interrupting its running tasks."""
    await _pause(task_type, True)


async def resume(task_type: str) -> None:
    await _pause(task_type, False)


async def requeue(task_id: int) -> None:
    pool = await runtime.open_pool()
    async with pool.connection() as conn:
        task_type = await store.requeue(conn, task_id)
    runtime.hints().wake([task_type])
    runtime.hints().feed(["queued"])


async def stats() -> dict[str, Any]:
    pool = await runtime.open_pool()
    async with pool.connection() as conn:
        return await store.stats(conn)
