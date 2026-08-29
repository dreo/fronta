"""Fronta: distributed task processing on PostgreSQL with sandboxed process execution."""

from importlib.metadata import version

from fronta.config import Settings
from fronta.definitions import Context, ProcessTaskDefinition, TaskDefinition, process_task, task
from fronta.errors import (
    ConfigurationError,
    FrontaError,
    InputValidationError,
    InvalidInput,
    NonRetryableError,
    NotCancellable,
    PayloadTooLarge,
    ProgressTooLarge,
    ResultSerializationError,
    SandboxError,
    TaskNotFound,
    UnknownTaskType,
)
from fronta.model import Backoff, Policy, Sandbox, State, TaskRow, TaskSummary, TaskTypeRow
from fronta.runtime import close_pool, configure, open_pool
from fronta.worker import Worker

__version__: str = version("fronta")

__all__ = [
    "Backoff",
    "ConfigurationError",
    "Context",
    "FrontaError",
    "InputValidationError",
    "InvalidInput",
    "NonRetryableError",
    "NotCancellable",
    "PayloadTooLarge",
    "Policy",
    "ProcessTaskDefinition",
    "ProgressTooLarge",
    "ResultSerializationError",
    "Sandbox",
    "SandboxError",
    "Settings",
    "State",
    "TaskDefinition",
    "TaskNotFound",
    "TaskRow",
    "TaskSummary",
    "TaskTypeRow",
    "UnknownTaskType",
    "Worker",
    "__version__",
    "close_pool",
    "configure",
    "open_pool",
    "process_task",
    "task",
]
