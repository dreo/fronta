"""Domain exceptions.

They live in one bottom-layer module because they form the public API surface (`fronta.X`) and are
raised and handled across layers (codec -> executors -> worker/server).
"""


class FrontaError(Exception):
    """Base class of every Fronta error."""


class ConfigurationError(FrontaError):
    """Invalid settings or definition parameters."""


class PayloadTooLarge(FrontaError):
    """Enqueue input exceeds the payload cap (UTF-8 bytes of the JSON encoding)."""


class ProgressTooLarge(FrontaError):
    """`ctx.progress()` value exceeds the progress cap."""


class InputValidationError(FrontaError):
    """Stored input does not match the input model at claim time. Fails the task without retry."""


class ResultSerializationError(FrontaError):
    """Result is not JSON, not finite, over the cap, or violates the output model. No retry."""


class NonRetryableError(FrontaError):
    """Raised by a handler to fail the task without retry."""


class UnknownTaskType(FrontaError):
    """The task type has not been published by any worker."""


class TaskNotFound(FrontaError):
    """No task with this id."""


class NotCancellable(FrontaError):
    """The task is already terminal."""


class InvalidInput(FrontaError):
    """Server-side input does not match the published JSON schema."""


class SandboxError(FrontaError):
    """The sandbox cannot be set up on this host (probe failed) or a spawn failed."""
