"""A worker built without explicit settings: the CLI resolves `FRONTA_*` when it starts."""

from fronta import Worker
from tests.workers import sleep_task

worker = Worker([sleep_task])
