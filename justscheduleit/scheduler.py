from __future__ import annotations

import dataclasses as dc
import inspect
import logging
import signal
import sys
import threading
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager
from enum import Enum
from functools import total_ordering
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    Awaitable,
    Callable,
    Generator,
    Generic,
    TypeVar,
    Union,
    final,
)

import anyio
from anyio import CancelScope, create_memory_object_stream, create_task_group, get_cancelled_exc_class, to_thread
from anyio.abc import TaskGroup
from anyio.from_thread import BlockingPortal, start_blocking_portal
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from typing_extensions import ParamSpec

from justscheduleit._utils import DC_CONFIG, choose_anyio_backend, first, task_full_name

__all__ = ["Scheduler", "RecurringTask", "SchedulerExecutionManager", "State", "default_task_error_handler"]

T = TypeVar("T")
TriggerEventT = TypeVar("TriggerEventT")
P = ParamSpec("P")

Trigger = Union[AsyncIterable[TriggerEventT], AsyncGenerator[TriggerEventT, T]]
DuplexTriggerFactory = Callable[[CancelScope, "_SchedulerExecution"], AsyncGenerator[TriggerEventT, T]]
SimplexTriggerFactory = Callable[[CancelScope, "_SchedulerExecution"], AsyncIterable[TriggerEventT]]
# TriggerFactory = Union[DuplexTriggerFactory, SimplexTriggerFactory]
TriggerFactory = Union[
    Callable[[CancelScope, "_SchedulerExecution"], AsyncGenerator[TriggerEventT, T]],
    Callable[[CancelScope, "_SchedulerExecution"], AsyncIterable[TriggerEventT]],
]

HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)
if sys.platform == "win32":  # pragma: py-not-win32
    HANDLED_SIGNALS += (signal.SIGBREAK,)  # Windows signal 21. Sent by Ctrl+Break.

logger = logging.getLogger("justscheduleit.scheduler")


def _wrap_to_async(func: Callable[P, T] | Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
    def call_in_thread(*args, **kwargs) -> Awaitable[T]:
        return to_thread.run_sync(func, *args, **kwargs)  # type: ignore

    return func if inspect.iscoroutinefunction(func) else call_in_thread  # type: ignore


@final
@total_ordering
class State(Enum):
    CREATED = 1
    STARTING = 2
    STARTED = 3
    SHUTTING_DOWN = 4  # Graceful
    STOPPING = 5  # Forced
    STOPPED = 6

    def __lt__(self, other: State) -> bool:
        return self.value < other.value


class _RecurringTaskExecution(Generic[T]):
    """
    Task execution context. Once started, cannot be started again.
    """

    scheduler_execution: _SchedulerExecution
    task: RecurringTask[T, Any]

    _result_stream: MemoryObjectSendStream[T]
    _results_reader: MemoryObjectReceiveStream[T]
    _stop_scope: CancelScope
    _observed: bool
    _started: bool

    __slots__ = (
        "scheduler_execution",
        "task",
        "_result_stream",
        "_results_reader",
        "_stop_scope",
        "_observed",
        "_started",
    )

    def __init__(self, scheduler_execution: _SchedulerExecution, task: RecurringTask[T, Any]):
        self.scheduler_execution = scheduler_execution
        self.task = task

        self._result_stream, self._results_reader = create_memory_object_stream[T]()
        self._stop_scope = CancelScope()
        self._observed = False
        self._started = False

    def subscribe(self) -> MemoryObjectReceiveStream[T]:
        # Make sure that there is at least one subscriber, otherwise we will just spam the memory buffer
        self._observed = True
        return self._results_reader.clone()

    async def __call__(self) -> None:
        if self._started:
            raise RuntimeError("Already started")

        self._started = True
        try:
            trigger = self.task.trigger(self._stop_scope, self.scheduler_execution)
            if inspect.isasyncgen(trigger):
                await self._start_with_feedback(trigger)
            else:
                await self._start(trigger)
        except get_cancelled_exc_class():  # Forced shutdown (the whole task group is cancelled)
            self._stop_scope.cancel()
            raise
        except Exception:
            self._stop_scope.cancel()
            raise

    async def _start(self, trigger: AsyncIterable[Any]) -> None:
        event_stream = trigger.__aiter__()
        try:
            async with self._result_stream:
                while not self._stop_scope.cancel_called:
                    with self._stop_scope:
                        event = await event_stream.__anext__()
                    if not self._stop_scope.cancel_called:
                        await self._execute(event)
        except (StopIteration, StopAsyncIteration):
            pass  # Just finish normally

    async def _start_with_feedback(self, trigger: AsyncGenerator[Any, T]) -> None:
        # Send the initial `None`, as per the (async) generator protocol
        result: T = None  # type: ignore
        try:
            async with self._result_stream:
                while not self._stop_scope.cancel_called:
                    with self._stop_scope:
                        event = await trigger.asend(result)
                    if not self._stop_scope.cancel_called:
                        result = await self._execute(event)
        except (StopIteration, StopAsyncIteration):
            pass  # Just finish normally
        finally:
            await trigger.aclose()

    async def _execute(self, event: Any) -> T:
        result = await self.task(event, self._stop_scope)
        if self._observed:
            await self._result_stream.send(result)

        return result

    def shutdown(self):
        """
        Graceful shutdown.

        1. Stop listening for new trigger events.
        2. Wait for the current task function call to complete (a task function with a CancelScope parameter can
           optimize the shutdown process internally).
        """
        self._stop_scope.cancel()


# @overload
# def create_task(name: str,
#                 trigger: AsyncIterable[TriggerEventT],
#                 target: Callable[..., Awaitable[T]],
#                 scheduler: Scheduler) -> RecurringTask[T, TriggerEventT]:
#     ...
#
#
# @overload
# def create_task(name: str,
#                 trigger: AsyncIterable[TriggerEventT],
#                 target: Callable[..., T],
#                 scheduler: Scheduler) -> RecurringTask[T, TriggerEventT]:
#     ...
#
#
# @overload
# def create_task(name: str,
#                 trigger: AsyncGenerator[TriggerEventT, T],
#                 target: Callable[..., Awaitable[T]],
#                 scheduler: Scheduler) -> RecurringTask[T, TriggerEventT]:
#     ...
#
#
# @overload
# def create_task(name: str,
#                 trigger: AsyncGenerator[TriggerEventT, T],
#                 target: Callable[..., T],
#                 scheduler: Scheduler) -> RecurringTask[T, TriggerEventT]:
#     ...
#
#
# def create_task(name, trigger, target, scheduler):
#     return RecurringTask(name, trigger, target, scheduler)


@final
@dc.dataclass(frozen=True, **DC_CONFIG)
class RecurringTask(Generic[T, TriggerEventT]):
    name: str
    trigger: TriggerFactory
    target: Callable[..., Awaitable[T]] | Callable[..., T]
    scheduler: Scheduler

    _async_target: Callable[..., Awaitable[T]] = dc.field(init=False)
    """
    The target function, maybe wrapped to an async one.
    """
    _callable: Callable[[TriggerEventT, CancelScope], Awaitable[T]] = dc.field(init=False)
    """
    Optimised callable for the target function (based on its signature).
    """

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.name} with {self.trigger} trigger>"

    def __post_init__(self):
        # Choose the right callable based on the target's signature (parameters)
        signature = inspect.signature(self.target)
        target_callable = self._call_no_args
        if len(signature.parameters) == 2:
            target_callable = self._call_with_event_and_cancellation
        elif len(signature.parameters) == 1:
            param_type = first(signature.parameters.values()).annotation
            if param_type == CancelScope:
                target_callable = self._call_with_cancellation
            else:
                target_callable = self._call_with_event

        object.__setattr__(self, "_async_target", _wrap_to_async(self.target))
        object.__setattr__(self, "_callable", target_callable)

    async def __call__(self, event: Any, cancel_scope: CancelScope) -> T:
        """
        Execute the task function, on the given trigger event.
        """
        return await self._callable(event, cancel_scope)

    def _call_no_args(self, _, __):
        return self._async_target()

    def _call_with_event(self, event, _):
        return self._async_target(event)

    def _call_with_cancellation(self, _, cancel_scope):
        return self._async_target(cancel_scope)

    def _call_with_event_and_cancellation(self, event, cancel_scope):
        return self._async_target(event, cancel_scope)


@final
class SchedulerExecutionManager:
    """
    Scheduler execution manager, for _outside_ control of the scheduler.
    """

    _execution: _SchedulerExecution
    _portal: BlockingPortal
    _thread_id: int

    __slots__ = ("_execution", "_portal", "_thread_id")

    def __init__(self, execution: _SchedulerExecution, portal: BlockingPortal):
        self._execution = execution
        self._portal = portal
        self._thread_id = threading.get_ident()

    def __repr__(self):
        return f"<{self.__class__.__name__} for {self._execution.scheduler}>"

    @property
    def completion_scope(self):
        return self._execution.completion_scope

    @property
    def state(self):
        return self._execution.state

    def shutdown(self) -> None:
        """
        Graceful shutdown.

        Stop listening for new events (timers, etc.) and complete currently executing tasks normally.
        """
        if threading.get_ident() == self._thread_id:
            self._execution.shutdown()
        else:
            # noinspection PyTypeChecker
            self._portal.call(self._execution.shutdown)

    def stop(self) -> None:
        """
        Forced shutdown.

        Cancel all the tasks immediately.
        """
        if threading.get_ident() == self._thread_id:
            self._execution.stop()
        else:
            # noinspection PyTypeChecker
            self._portal.call(self._execution.stop)


class _SchedulerExecution:
    scheduler: Scheduler
    tasks: dict[RecurringTask, _RecurringTaskExecution]
    state: State
    # TODO Test, make an example
    completion_scope: CancelScope
    """
    Signals when the execution is done (either completed or cancelled)
    """

    _task_group: TaskGroup

    __slots__ = ("scheduler", "tasks", "state", "completion_scope", "_task_group")

    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self.tasks = {task: _RecurringTaskExecution(self, task) for task in self.scheduler.tasks}

        self.state = State.CREATED
        self.completion_scope = CancelScope()
        self._task_group = create_task_group()

    def __repr__(self):
        # TODO Use repr for self.scheduler also
        return f"<{self.__class__.__name__} for {self.scheduler}> (state: {self.state})"

    def find_task_exec(self, task: Callable | RecurringTask) -> _RecurringTaskExecution | None:
        if isinstance(task, RecurringTask):
            return self.tasks.get(task)

        for task_exec in self.tasks.values():
            if task_exec.task.target == task:
                return task_exec

        return None

    async def serve(self) -> None:
        if self.state is not State.CREATED:
            raise RuntimeError("Already started")

        def start_tasks() -> None:
            logger.debug("Starting registered tasks...")
            for te in self.tasks.values():
                # noinspection PyTypeChecker
                self._task_group.start_soon(te)
            self.state = State.STARTED

        try:
            self.state = State.STARTING
            logger.debug("Starting scheduler...")
            async with self._task_group:
                if self.scheduler.lifespan is None:
                    start_tasks()
                else:
                    async with self.scheduler.lifespan:  # Startup / shutdown hooks
                        start_tasks()
        finally:
            self.state = State.STOPPED
            self.completion_scope.cancel()

    def shutdown(self) -> None:
        """
        Graceful shutdown (wait for all the tasks to complete).
        """
        if self.state >= State.SHUTTING_DOWN:
            return

        logger.debug("Graceful shutdown, waiting for all the tasks to complete...")
        for task in self.tasks.values():
            task.shutdown()

    def stop(self) -> None:
        """
        Forced shutdown (cancel all the tasks).
        """
        if self.state >= State.STOPPING:
            return

        logger.debug("Forced shutdown, cancelling all the tasks")
        self.state = State.STOPPING
        self._task_group.cancel_scope.cancel()


async def default_task_error_handler(exc: Exception, task: RecurringTask):
    logger.exception("Task %s failed", task.name, exc_info=exc)


async def _handle_signals_for(execution: SchedulerExecutionManager) -> None:
    with execution.completion_scope:
        # See https://anyio.readthedocs.io/en/stable/signals.html
        with anyio.open_signal_receiver(*HANDLED_SIGNALS) as signals:
            logger.debug("Listening to OS signals...")
            async for sig in signals:
                if execution.state < State.SHUTTING_DOWN:  # First Ctrl+C
                    logger.info("Shutting down...")
                    execution.shutdown()
                if sig == signal.SIGINT and execution.state is State.SHUTTING_DOWN:  # Ctrl+C again
                    logger.warning("Forced shutdown")
                    execution.stop()


TaskFn = Union[Callable[P, Awaitable[T]], Callable[P, T]]
# TaskFnDecorator = Callable[[Union[Callable[P, Awaitable[T]], Callable[P, T]]], RecurringTask[T, TriggerEventT]]
TaskFnDecorator = Union[
    Callable[[Callable[..., Awaitable[T]]], RecurringTask[T, TriggerEventT]],
    Callable[[Callable[..., T]], RecurringTask[T, TriggerEventT]],
]


@final
@dc.dataclass(eq=False, **DC_CONFIG)
class Scheduler:
    lifespan: AbstractAsyncContextManager | None = None
    tasks: list[RecurringTask] = dc.field(default_factory=list)

    # TODO Use
    error_handler: Callable[[Exception, RecurringTask], Awaitable] = dc.field(default=default_task_error_handler)

    def task(self, trigger: TriggerFactory, *, name: str | None = None) -> Callable[[Callable[P, T]], Callable[P, T]]:
        def decorator(func: Callable[P, T]) -> Callable[P, T]:
            task = RecurringTask(name if name else task_full_name(func), trigger, func, self)
            self.tasks.append(task)
            return func

        return decorator

    def __repr__(self):
        return f"<{self.__class__.__name__} with {len(self.tasks)} tasks>"

    def run(self):
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("Signals can only be installed on the main thread")

        # noinspection PyTypeChecker
        anyio.run(self._run, **choose_anyio_backend())

    async def _run(self):
        async with self.aserve() as execution:
            await _handle_signals_for(execution)

    @asynccontextmanager
    async def aserve(self) -> AsyncGenerator[SchedulerExecutionManager, Any]:
        async with BlockingPortal() as portal:
            async with create_task_group() as tg:
                execution = _SchedulerExecution(self)
                execution_manager = SchedulerExecutionManager(execution, portal)
                # noinspection PyTypeChecker
                tg.start_soon(execution.serve)
                yield execution_manager
                # App is done, stop the scheduler
                execution.shutdown()

    @contextmanager
    def serve(self) -> Generator[SchedulerExecutionManager, Any, None]:
        with start_blocking_portal(**choose_anyio_backend()) as thread:
            with thread.wrap_async_context_manager(self.aserve()) as execution_manager:
                yield execution_manager
