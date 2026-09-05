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

from pydantic import BaseModel, ValidationError

from fronta import codec, runtime, store
from fronta.errors import InvalidInput, PayloadTooLarge
from fronta.model import Backoff, Executor, NewTask, Policy, Sandbox, TaskTypeSpec

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
    ) -> int:
        """Enqueue and return the task id (or the id of the active task with the same key).

        With a non-autocommit `conn` the insert joins the caller's transaction (never committed by
        Fronta). An autocommit connection gets one transaction for the insert and notifications.
        Without `conn` the process-global pool is used and the insert is committed here; only that
        path needs `FRONTA_DSN`.
        """
        return await self.enqueue_with(
            runtime.get_settings(),
            input,
            conn=conn,
            priority=priority,
            run_at=run_at,
            key=key,
            concurrency_key=concurrency_key,
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
        )
        deadline = settings.statement_timeout_s
        if conn is not None:
            if conn.autocommit:
                async with conn.transaction():
                    return await store.enqueue(conn, new_task, deadline_s=deadline)
            return await store.enqueue(conn, new_task, deadline_s=deadline)
        pool = await runtime.open_pool()
        async with pool.connection() as own_conn, own_conn.transaction():
            return await store.enqueue(own_conn, new_task, deadline_s=deadline)


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
