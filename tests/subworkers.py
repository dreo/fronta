"""Worker instances for `fronta worker tests.subworkers:<name>`; settings come from `FRONTA_*`."""

from __future__ import annotations

from fronta import Settings, Worker
from tests.workers import (
    blocker_task,
    echo_proc,
    lifespan,
    long_proc,
    progress_task,
    sleep_task,
    state_task,
    stubborn_task,
)

settings = Settings()  # type: ignore[call-arg]  # FRONTA_DSN and friends from the environment

crash_worker = Worker([sleep_task], settings=settings)
sandbox_crash_worker = Worker([sleep_task, long_proc], settings=settings)
fatal_worker = Worker([stubborn_task, sleep_task], settings=settings)
blocker_worker = Worker([blocker_task], settings=settings)
e2e_worker = Worker([sleep_task, progress_task, state_task], lifespan=lifespan, settings=settings)
e2e_process_worker = Worker([echo_proc], settings=settings)
