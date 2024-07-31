from __future__ import annotations

import dataclasses as dc
import logging
from datetime import timedelta
from typing import Any, AsyncGenerator, AsyncIterator, Awaitable, Callable, Generic, TypeVar, final

from anyio import CancelScope, EndOfStream

from justscheduleit._utils import DC_CONFIG, random_jitter, sleep
from justscheduleit.scheduler import RecurringTask, _RecurringTaskExecution, _SchedulerExecution  # noqa

__all__ = ["every", "recurrent", "after"]

T = TypeVar("T")

logger = logging.getLogger("justscheduleit.cond")


# noinspection PyPep8Naming
@final
@dc.dataclass(frozen=True, **DC_CONFIG)  # TODO Check repr()
class every:
    """
    Triggers every `period`, with a random `jitter`.
    """

    period: timedelta
    jitter: tuple[int, int] | None = dc.field(
        default=(1, 10),
        # kw_only=True,  # Enable when Python 3.10 is the minimum version
    )

    async def __call__(self, _, __: _SchedulerExecution) -> AsyncIterator[None]:
        iter_n = 0
        iter_delay = timedelta(0)  # Execute the first iteration immediately
        while True:
            iter_jitter = random_jitter(self.jitter)
            iter_total_delay = iter_delay + iter_jitter
            logger.debug(
                "(every %s, iter %s) Sleeping for %s (delay: %s, jitter: %s)",
                self.period,
                iter_n,
                iter_total_delay,
                iter_delay,
                iter_jitter,
            )
            await sleep(iter_total_delay)
            yield
            iter_delay = self.period
            iter_n += 1


# noinspection PyPep8Naming
@final
@dc.dataclass(frozen=True, **DC_CONFIG)  # TODO Check repr()
class recurrent:
    """
    Triggers every `default_period` (unless overwritten), with a random `jitter`.
    """

    default_period: timedelta = timedelta(minutes=1)
    jitter: tuple[int, int] | None = dc.field(
        default=(1, 10),
        # kw_only=True,  # Enable when Python 3.10 is the minimum version
    )

    async def __call__(self, _, __: _SchedulerExecution) -> AsyncGenerator[None, timedelta]:
        iter_n = 0
        iter_delay = timedelta(0)  # Execute the first iteration immediately
        while True:
            iter_jitter = random_jitter(self.jitter)
            iter_total_delay = iter_delay + iter_jitter
            logger.debug(
                "(recurrent, iter %s) Sleeping for %s (delay: %s, jitter: %s)",
                iter_n,
                iter_total_delay,
                iter_delay,
                iter_jitter,
            )
            await sleep(iter_total_delay)
            delta_override = yield
            if isinstance(delta_override, timedelta):
                iter_delay = delta_override
            else:
                logger.warning("Invalid delta override (should be a timedelta instance): %s", delta_override)
                iter_delay = self.default_period
            iter_n += 1


# noinspection PyPep8Naming
@final
@dc.dataclass(frozen=True, **DC_CONFIG)  # TODO Check repr()
class after(Generic[T]):
    """
    Triggers every time `task` is completed, with a random `jitter`.
    """

    task: Callable[..., Awaitable[T]] | Callable[..., T] | RecurringTask[T, Any]
    jitter: tuple[int, int] | None = dc.field(
        default=(1, 10),
        # kw_only=True,  # Enable when Python 3.10 is the minimum version
    )

    def __call__(self, _: CancelScope, scheduler_execution: _SchedulerExecution) -> AsyncGenerator[T, Any]:
        task_exec = scheduler_execution.find_task_exec(self.task)
        if task_exec is None:
            raise ValueError(f"Task {self.task} not found in the scheduler")

        return self._run(task_exec)

    async def _run(self, task_exec: _RecurringTaskExecution[T]):
        task_executions = task_exec.subscribe()
        task = task_exec.task

        async with task_executions:
            try:
                while True:
                    logger.debug("(after %s) Waiting for another iteration to complete...", task.name)
                    result = await task_executions.receive()
                    if self.jitter is not None:
                        iter_jitter = random_jitter(self.jitter)
                        logger.debug("(after %s) Sleeping for %s (iteration jitter)", task.name, iter_jitter)
                        await sleep(iter_jitter)
                    # Do not nest `yield` inside a cancel scope, see
                    # https://anyio.readthedocs.io/en/stable/cancellation.html#avoiding-cancel-scope-stack-corruption
                    yield result
            except EndOfStream:
                pass  # Just finish normally
