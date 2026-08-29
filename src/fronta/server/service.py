"""Operations shared by REST and MCP over the store; raises domain errors the transports map."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from jsonschema import Draft202012Validator

from fronta import codec, runtime, store
from fronta.errors import (
    InvalidInput,
    NotCancellable,
    PayloadTooLarge,
    TaskNotFound,
    UnknownTaskType,
)
from fronta.model import NewTask, State, TaskFilter

_VALIDATOR_CACHE_SIZE = 128
"""Compiled schema validators kept per fingerprint (a republish changes the fingerprint)."""

if TYPE_CHECKING:
    from datetime import datetime

    from psycopg_pool import AsyncConnectionPool

    from fronta.config import Settings
    from fronta.model import TaskRow, TaskSummary, TaskTypeRow


def task_to_dict(row: TaskRow) -> dict[str, Any]:
    data = asdict(row)
    data["state"] = row.state.value
    data["token"] = None  # never expose the execution token
    return data


def summary_to_dict(row: TaskSummary) -> dict[str, Any]:
    data = asdict(row)
    data["state"] = row.state.value
    return data


def task_type_to_dict(row: TaskTypeRow) -> dict[str, Any]:
    return {
        "name": row.name,
        "executor": row.executor.value,
        "input_schema": row.input_schema,
        "output_schema": row.output_schema,
        "policy": row.policy.to_json(),
        "fingerprint": row.fingerprint,
        "updated_at": row.updated_at,
    }


class Service:
    """The five operations of the control plane. `start()` opens the pool, `stop()` closes it."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._pool: AsyncConnectionPool[Any] | None = None
        self._validators: dict[str, Draft202012Validator] = {}

    async def start(self) -> None:
        pool = runtime.make_pool(self.settings, application_name="fronta-server")
        await runtime.open_ready(pool, self.settings.connect_timeout_s)
        self._pool = pool

    async def stop(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self) -> AsyncConnectionPool[Any]:
        if self._pool is None:
            msg = "service is not started"
            raise RuntimeError(msg)
        return self._pool

    async def list_task_types(self) -> list[TaskTypeRow]:
        async with self.pool.connection() as conn:
            return await store.get_task_types(conn)

    async def enqueue(  # noqa: PLR0913  # the operation's parameters, as specified
        self,
        task_type: str,
        input: dict[str, Any],
        *,
        priority: int = 0,
        run_at: datetime | None = None,
        key: str | None = None,
        concurrency_key: str | None = None,
    ) -> int:
        if run_at is not None and run_at.tzinfo is None:
            msg = "run_at must carry a timezone"
            raise InvalidInput(msg)
        async with self.pool.connection() as conn:
            row = await store.get_task_type(conn, task_type)
            if row is None:
                msg = f"unknown task type {task_type!r}"
                raise UnknownTaskType(msg)
            self._validate(row, input)
            try:
                encoded = codec.encode_capped(input, self.settings.payload_cap, "payload")
            except codec.OverCap as exc:
                raise PayloadTooLarge(str(exc)) from exc
            except codec.Unstorable as exc:
                raise InvalidInput(str(exc)) from exc
            new_task = NewTask(
                type=task_type,
                input_json=encoded,
                policy=row.policy,
                priority=priority,
                run_at=run_at,
                key=key,
                concurrency_key=concurrency_key,
            )
            try:
                async with conn.transaction():
                    return await store.enqueue(
                        conn, new_task, deadline_s=self.settings.statement_timeout_s
                    )
            except ValueError as exc:  # oversized key / name
                raise InvalidInput(str(exc)) from exc

    def _validate(self, row: TaskTypeRow, input: dict[str, Any]) -> None:
        validator = self._validators.get(row.fingerprint)
        if validator is None:
            validator = Draft202012Validator(
                row.input_schema, format_checker=Draft202012Validator.FORMAT_CHECKER
            )
            if len(self._validators) >= _VALIDATOR_CACHE_SIZE:
                self._validators.pop(next(iter(self._validators)))
            self._validators[row.fingerprint] = validator
        errors = sorted(validator.iter_errors(input), key=lambda e: list(e.path))
        if errors:
            details = "; ".join(
                f"{'/'.join(str(p) for p in e.path) or '<root>'}: {e.message}" for e in errors[:5]
            )
            msg = f"input does not match the schema of {row.name!r}: {details}"
            raise InvalidInput(msg)

    async def get_task(self, task_id: int) -> TaskRow:
        async with self.pool.connection() as conn:
            row = await store.get_task(conn, task_id)
        if row is None:
            msg = f"no task {task_id}"
            raise TaskNotFound(msg)
        return row

    async def list_tasks(self, flt: TaskFilter) -> list[TaskSummary]:
        limit = min(max(flt.limit, 1), self.settings.list_page_max)
        async with self.pool.connection() as conn:
            return await store.list_tasks(
                conn, TaskFilter(flt.type, flt.state, flt.key, flt.before, limit)
            )

    async def cancel(self, task_id: int) -> State:
        async with self.pool.connection() as conn:
            state = await store.request_cancel(conn, task_id)
            if state is not None:
                return state
            row = await store.get_task(conn, task_id)
        if row is None:
            msg = f"no task {task_id}"
            raise TaskNotFound(msg)
        msg = f"task {task_id} is {row.state.value}"
        raise NotCancellable(msg)
