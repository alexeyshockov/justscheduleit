from __future__ import annotations

import dataclasses as dc
import inspect
import logging
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, Awaitable, Callable, Generic, TypeVar, final

from anyio import EndOfStream, ClosedResourceError

from justscheduleit._utils import DC_CONFIG, random_jitter, sleep
from justscheduleit.scheduler import (SchedulerLifetime, ScheduledTask, TaskExecutionFlow, TriggerFactory)

__all__ = ["every", "recurrent", "after"]

TriggerEventT = TypeVar("TriggerEventT")
T = TypeVar("T")  # Task result type

logger = logging.getLogger(__name__)


def skip_first(n: int, trigger: TriggerFactory[TriggerEventT, T]) -> TriggerFactory[TriggerEventT, T]:
    async def _skip(scheduler_lifetime: SchedulerLifetime):
        events = trigger(scheduler_lifetime)
        iter_n = 0
        if inspect.isasyncgen(events):
            try:
                move_next = events.asend(None)  # Start the async generator
                while iter_n < n:  # Skip the first N events
                    iter_n += 1
                    await move_next
                    logger.debug("Skipping iteration %s", iter_n)
                    move_next = events.asend(None)
                while True:  # And continue as usual
                    iter_n += 1
                    event = await move_next
                    try:
                        result = yield event
                        move_next = events.asend(result)
                    except Exception as exc:  # noqa
                        move_next = events.athrow(exc)
            except StopAsyncIteration:
                pass
        else:
            async for event in events:
                iter_n += 1
                if iter_n > n:
                    yield event
                else:
                    logger.debug("Skipping iteration %s", iter_n)

    return _skip if n > 0 else trigger


def take_first(n: int, trigger: TriggerFactory[TriggerEventT, T]) -> TriggerFactory[TriggerEventT, T]:
    async def _take(scheduler_lifetime: SchedulerLifetime):
        events = trigger(scheduler_lifetime)
        iter_n = 0
        if inspect.isasyncgen(events):
            try:
                move_next = events.asend(None)  # Start the async generator
                while iter_n < n:
                    iter_n += 1
                    event = await move_next
                    try:
                        result = yield event
                        move_next = events.asend(result)
                    except Exception as exc:  # noqa
                        move_next = events.athrow(exc)
            except StopAsyncIteration:
                pass
        else:
            async for event in events:
                if iter_n >= n:
                    break
                iter_n += 1
                yield event

    return _take


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
    stop_on_error: bool = dc.field(
        default=False,
        # kw_only=True,  # Enable when Python 3.10 is the minimum version
    )

    async def __call__(self, _) -> AsyncGenerator[None, Any]:
        iter_n = 0
        iter_delay = timedelta(0)  # Execute the first iteration immediately
        while True:
            iter_n += 1
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
            try:
                yield
            except Exception:  # noqa
                if self.stop_on_error:
                    raise
                logger.exception("Error during task execution")
            iter_delay = self.period


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
    stop_on_error: bool = dc.field(
        default=False,
        # kw_only=True,  # Enable when Python 3.10 is the minimum version
    )

    async def __call__(self, _) -> AsyncGenerator[None, timedelta]:
        iter_n = 0
        iter_delay = timedelta(0)  # Execute the first iteration immediately
        while True:
            iter_n += 1
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
            try:
                iter_delay = yield
                if iter_delay is None:
                    iter_delay = self.default_period
                elif not isinstance(iter_delay, timedelta):
                    logger.warning("Invalid delta override (expected %s, got %s)", timedelta, type(iter_delay))
                    iter_delay = self.default_period
            except Exception:  # noqa
                logger.warning("Error during task execution, using default period for the next iteration")
                iter_delay = self.default_period


# noinspection PyPep8Naming
@final
@dc.dataclass(frozen=True, **DC_CONFIG)  # TODO Check repr()
class after(Generic[T]):
    """
    Triggers every time `task` is completed, with a random `jitter`.
    """

    task: Callable[..., Awaitable[T]] | Callable[..., T] | ScheduledTask[T, Any]
    jitter: tuple[int, int] | None = dc.field(
        default=(1, 10),
        # kw_only=True,  # Enable when Python 3.10 is the minimum version
    )

    def __call__(self, scheduler_lifetime: SchedulerLifetime) -> AsyncGenerator[T, Any]:
        task_exec_flow = scheduler_lifetime.find_exec_for(self.task)
        if task_exec_flow is None:
            raise ValueError(f"Task {self.task} not found in the scheduler")

        return self._run(task_exec_flow)

    async def _run(self, task_exec_flow: TaskExecutionFlow[T]):
        task_executions = task_exec_flow.subscribe()
        task = task_exec_flow.task

        async with task_executions:
            while True:
                logger.debug("(after %s) Waiting for another source task iteration...", task.name)
                try:
                    result = await task_executions.receive()
                except (EndOfStream, ClosedResourceError):
                    logger.debug("(after %s) Source task has completed, so do we", task.name)
                    break

                if self.jitter is not None:
                    iter_jitter = random_jitter(self.jitter)
                    logger.debug("(after %s) Sleeping for %s (iteration jitter)", task.name, iter_jitter)
                    await sleep(iter_jitter)

                try:
                    # Do not nest `yield` inside a cancel scope, see
                    # https://anyio.readthedocs.io/en/stable/cancellation.html#avoiding-cancel-scope-stack-corruption
                    yield result
                except Exception:  # noqa
                    logger.warning("Error during task execution")
                    # Continue, as we are bound to the source task's lifecycle
