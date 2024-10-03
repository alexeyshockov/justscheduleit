from __future__ import annotations

import dataclasses as dc
import logging
from collections.abc import AsyncGenerator, Sequence
from datetime import timedelta
from typing import Any, Callable, Generic, TypeVar, cast

from anyio import get_cancelled_exc_class, move_on_after

from justscheduleit._utils import (
    DelayFactory,
    RandomDelay,
    ensure_delay_factory,
    ensure_td,
    sleep,
    task_full_name,
    td_str,
)
from justscheduleit.scheduler import (
    ScheduledTask,
    SchedulerLifetime,
    TaskExecutionFlow,
    TaskT,
    TriggerFactory,
)

T = TypeVar("T")
TriggerEventT = TypeVar("TriggerEventT")

logger = logging.getLogger("justscheduleit.cond")

DEFAULT_JITTER = RandomDelay((1, 10))


def skip_first(n: int, trigger: TriggerFactory[TriggerEventT, T], /) -> TriggerFactory[TriggerEventT, T]:
    """
    Skip the first N events from the trigger, then continue as usual.
    """

    async def _skip(scheduler_lifetime: SchedulerLifetime):
        events = trigger(scheduler_lifetime)
        iter_n = 0
        try:
            move_next = events.asend(None)  # type: ignore
            while iter_n < n:  # Skip the first N events
                iter_n += 1
                await move_next
                logger.debug("Skipping iteration %s", iter_n)
                move_next = events.asend(None)  # type: ignore
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
        finally:
            await events.aclose()

    return _skip if n > 0 else trigger


def take_first(n: int, trigger: TriggerFactory[TriggerEventT, T], /) -> TriggerFactory[TriggerEventT, T]:
    """
    Take the first N events from the trigger, then stop.
    """

    async def _take(scheduler_lifetime: SchedulerLifetime):
        events = trigger(scheduler_lifetime)
        iter_n = 0
        try:
            move_next = events.asend(None)  # type: ignore
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
        finally:
            await events.aclose()

    return _take


@dc.dataclass(frozen=True, slots=True)
class Batch(Generic[TriggerEventT]):
    trigger: TriggerFactory[TriggerEventT, None]
    max_size: int
    window_duration: float
    stop_on_error: bool = False

    def __repr__(self):
        return f"batch({self.max_size}, {self.window_duration}, {self.trigger!r})"

    async def __call__(  # noqa: C901 (ignore complexity)
        self, scheduler_lifetime: SchedulerLifetime
    ) -> AsyncGenerator[Sequence[TriggerEventT], Any]:
        stream = self.trigger(scheduler_lifetime)
        try:
            while True:
                items = []
                try:
                    with move_on_after(self.window_duration):
                        while len(items) < self.max_size:
                            item = await stream.asend(None)
                            items.append(item)
                except StopAsyncIteration:
                    if not items:  # Do not lose the last batch
                        raise
                except get_cancelled_exc_class():
                    try:
                        if items:
                            yield items
                    except Exception:  # noqa
                        if self.stop_on_error:
                            raise
                        logger.exception("Error during task execution")
                    raise

                try:
                    if items:
                        yield items
                except Exception:  # noqa
                    if self.stop_on_error:
                        raise
                    logger.exception("Error during task execution")
        except StopAsyncIteration:
            pass
        finally:
            await stream.aclose()


def batch(
    max_size: int,
    window_duration: float,
    trigger: TriggerFactory[TriggerEventT, None],
    /,
    *,
    stop_on_error: bool | None = None,
) -> Batch[TriggerEventT]:
    """
    Collect events from the downstream trigger into batches of `max_size` items, or until `window_duration` elapses.
    """
    if stop_on_error is None:
        stop_on_error = trigger.stop_on_error if hasattr(trigger, "stop_on_error") else False  # type: ignore
    return Batch(trigger, max_size, window_duration, cast(bool, stop_on_error))


@dc.dataclass(frozen=True, slots=True)
class Every:
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


def every(period: timedelta | str, /, *, delay: DelayFactory = DEFAULT_JITTER, stop_on_error: bool = False) -> Every:
    """
    Trigger an event every `period`, with an (optional) additional `delay` (jitter).
    """
    return Every(ensure_td(period), ensure_delay_factory(delay), stop_on_error)


@dc.dataclass(frozen=True, slots=True)
class Recurrent:
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
                    logger.warning(
                        "Invalid delta override (expected %s, got %s), using default period for the next iteration",
                        timedelta,
                        type(iter_delay),
                    )
                    iter_interval = self.default_interval
            except Exception:  # noqa
                if self.stop_on_error:
                    raise
                logger.exception("Error during task execution, using default period for the next iteration")
                iter_interval = self.default_interval


def recurrent(
    default_interval: timedelta | str = timedelta(minutes=1),
    /,
    *,
    delay: DelayFactory = DEFAULT_JITTER,
    stop_on_error: bool = False,
) -> Recurrent:
    """
    Trigger an event every `default_period` (unless overwritten by the task), with an (optional) additional `delay`
    (jitter).
    """
    return Recurrent(ensure_td(default_interval), ensure_delay_factory(delay), stop_on_error)


@dc.dataclass(frozen=True, slots=True)
class After(Generic[T]):
    task: TaskT[T] | ScheduledTask[T, Any]
    delay: Callable[[], timedelta] = DEFAULT_JITTER
    """
    Additional delay for each iteration, in seconds.

    Can be a fixed value, a random interval, or a custom delay factory.
    """
    stop_on_error: bool = False

    def __repr__(self):
        task = repr(self.task) if isinstance(self.task, ScheduledTask) else task_full_name(self.task)
        return f"after({task!r}, delay={self.delay!r})"

    def __call__(self, scheduler_lifetime: SchedulerLifetime) -> AsyncGenerator[T, Any]:
        task_exec_flow = scheduler_lifetime.find_exec_for(self.task)
        if task_exec_flow is None:
            raise ValueError(f"Task {self.task} not found in the scheduler")

        return self._run(task_exec_flow)

    async def _run(self, task_exec_flow: TaskExecutionFlow[T, Any]):
        task = task_exec_flow.task
        async with task_exec_flow.subscribe() as task_executions:
            async for result in task_executions:
                iter_jitter = self.delay()
                if iter_jitter.total_seconds() > 0:
                    logger.debug("(after %s) Sleeping for %s", task.name, td_str(iter_jitter))
                    await sleep(iter_jitter)
                try:
                    yield result
                except Exception:  # noqa
                    if self.stop_on_error:
                        raise
                    logger.exception("Error during task execution")
                logger.debug("(after %s) Waiting for another source task iteration...", task.name)
            logger.debug("(after %s) Source task has completed, so do we", task.name)


def after(
    task: TaskT[T] | ScheduledTask[T, Any],
    /,
    *,
    delay: DelayFactory = DEFAULT_JITTER,
    stop_on_error: bool = False,
) -> After[T]:
    """
    Trigger an event every time `task` is completed, with an (optional) additional `delay` (jitter).
    """
    return After(task, ensure_delay_factory(delay), stop_on_error)
