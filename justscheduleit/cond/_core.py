from __future__ import annotations

import dataclasses as dc
import inspect
import logging
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, Callable, Generic, TypeVar

from anyio import ClosedResourceError, EndOfStream

from justscheduleit._utils import (
    DelayFactory,
    RandomDelay,
    ensure_delay_factory,
    ensure_td,
    sleep,
    task_full_name,
    td_str,
)
from justscheduleit.scheduler import ScheduledTask, SchedulerLifetime, TaskExecutionFlow, TaskT, TriggerFactory

TriggerEventT = TypeVar("TriggerEventT")
T = TypeVar("T")  # Task result type

logger = logging.getLogger("justscheduleit.cond")

DEFAULT_JITTER = RandomDelay((1, 10))


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


@dc.dataclass(frozen=True)  # Enable slots when Python 3.10+
class Every:
    """
    Triggers every `period`, with an (optional) additional `delay` (jitter).
    """

    period: timedelta
    delay: Callable[[], timedelta] = DEFAULT_JITTER
    """
    Additional delay for each iteration, in seconds.

    Can be a fixed value, a random interval, or a custom delay factory.
    """
    stop_on_error: bool = False

    def __repr__(self):
        return f"every('{td_str(self.period)}', delay={self.delay!r})"

    async def __call__(self, _) -> AsyncGenerator[None, Any]:
        iter_n = 0
        iter_interval = timedelta(0)  # Execute the first iteration immediately
        while True:
            iter_n += 1
            iter_jitter = self.delay()
            iter_delay = iter_interval + iter_jitter
            logger.debug(
                "(every %s, iter %s) Sleeping for %s",
                td_str(self.period),
                iter_n,
                td_str(iter_delay),
            )
            await sleep(iter_delay)
            try:
                yield
            except Exception:  # noqa
                if self.stop_on_error:
                    raise
                logger.exception("Error during task execution")
            iter_interval = self.period


def every(period: timedelta | str, *, delay: DelayFactory = DEFAULT_JITTER, stop_on_error: bool = False) -> Every:
    return Every(ensure_td(period), ensure_delay_factory(delay), stop_on_error)


@dc.dataclass(frozen=True)  # Enable slots when Python 3.10+
class Recurrent:
    """
    Triggers every `default_period` (unless overwritten), with an (optional) additional `delay` (jitter).
    """

    default_interval: timedelta
    delay: Callable[[], timedelta] = DEFAULT_JITTER
    """
    Additional delay for each iteration, in seconds.

    Can be a fixed value, a random interval, or a custom delay factory.
    """
    stop_on_error: bool = False

    def __repr__(self):
        return f"recurrent('{td_str(self.default_interval)}', delay={self.delay!r})"

    async def __call__(self, _) -> AsyncGenerator[None, timedelta]:
        iter_n = 0
        iter_interval = timedelta(0)  # Execute the first iteration immediately
        while True:
            iter_n += 1
            iter_jitter = self.delay()
            iter_delay = iter_interval + iter_jitter
            logger.debug(
                "(recurrent, iter %s) Sleeping for %s (interval: %s, delay: %s)",
                iter_n,
                td_str(iter_delay),
                td_str(iter_interval),
                td_str(iter_jitter),
            )
            await sleep(iter_delay)
            try:
                iter_interval = yield
                if iter_interval is None:
                    iter_interval = self.default_interval
                elif not isinstance(iter_delay, timedelta):
                    logger.warning("Invalid delta override (expected %s, got %s)", timedelta, type(iter_delay))
                    iter_interval = self.default_interval
            except Exception:  # noqa
                logger.warning("Error during task execution, using default period for the next iteration")
                iter_interval = self.default_interval


def recurrent(
    default_interval: timedelta = timedelta(minutes=1),
    *,
    delay: DelayFactory = DEFAULT_JITTER,
    stop_on_error: bool = False,
) -> Recurrent:
    return Recurrent(ensure_td(default_interval), ensure_delay_factory(delay), stop_on_error)


@dc.dataclass(frozen=True)  # Enable slots when Python 3.10+
class After(Generic[T]):
    """
    Triggers every time `task` is completed, with an (optional) additional `delay` (jitter).
    """

    task: TaskT[T] | ScheduledTask[T, Any]
    delay: Callable[[], timedelta] = DEFAULT_JITTER
    """
    Additional delay for each iteration, in seconds.

    Can be a fixed value, a random interval, or a custom delay factory.
    """

    def __repr__(self):
        return f"after({self.task}, delay={self.delay!r})"

    @property
    def _task_name(self):
        return self.task.name if isinstance(self.task, ScheduledTask) else task_full_name(self.task)

    def __call__(self, scheduler_lifetime: SchedulerLifetime) -> AsyncGenerator[T, Any]:
        task_exec_flow = scheduler_lifetime.find_exec_for(self.task)
        if task_exec_flow is None:
            raise ValueError(f"Task {self.task} not found in the scheduler")

        return self._run(task_exec_flow)

    async def _run(self, task_exec_flow: TaskExecutionFlow[T, Any]):
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

                iter_jitter = self.delay()
                if iter_jitter.total_seconds() > 0:
                    logger.debug("(after %s) Sleeping for %s", task.name, td_str(iter_jitter))
                    await sleep(iter_jitter)

                try:
                    # Do not nest `yield` inside a cancel scope, see
                    # https://anyio.readthedocs.io/en/stable/cancellation.html#avoiding-cancel-scope-stack-corruption
                    yield result
                except Exception:  # noqa
                    logger.warning("Error during task execution")
                    # Continue, as we are bound to the source task's lifecycle


def after(task: TaskT[T] | ScheduledTask[T, Any], *, delay: DelayFactory = DEFAULT_JITTER) -> After[T]:
    return After(task, ensure_delay_factory(delay))
