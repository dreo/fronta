"""Fronta: distributed task processing on PostgreSQL with sandboxed process execution."""

from importlib.metadata import version

from fronta.config import Settings
from fronta.definitions import (
    Context,
    ProcessTaskDefinition,
    TaskDefinition,
    get_task,
    pause,
    process_task,
    requeue,
    resume,
    stats,
    task,
)
from fronta.errors import (
    ConfigurationError,
    FrontaError,
    InputValidationError,
    InvalidInput,
    NonRetryableError,
    NotCancellable,
    NotRequeueable,
    PayloadTooLarge,
    ProgressTooLarge,
    ResultSerializationError,
    SandboxError,
    TaskNotFound,
    UnknownTaskType,
)
from fronta.feed import subscribe, unsubscribe
from fronta.model import (
    Backoff,
    Executor,
    Policy,
    Sandbox,
    State,
    TaskEvent,
    TaskRow,
    TaskSummary,
    TaskTypeRow,
)
from fronta.runtime import close_pool, configure, open_pool
from fronta.worker import Worker

__version__: str = version("fronta")

__all__ = [
    "Backoff",
    "ConfigurationError",
    "Context",
    "Executor",
    "FrontaError",
    "InputValidationError",
    "InvalidInput",
    "NonRetryableError",
    "NotCancellable",
    "NotRequeueable",
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
    "TaskEvent",
    "TaskNotFound",
    "TaskRow",
    "TaskSummary",
    "TaskTypeRow",
    "UnknownTaskType",
    "Worker",
    "__version__",
    "close_pool",
    "configure",
    "get_task",
    "open_pool",
    "pause",
    "process_task",
    "requeue",
    "resume",
    "stats",
    "subscribe",
    "task",
    "unsubscribe",
]
