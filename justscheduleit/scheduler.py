from __future__ import annotations

import inspect
import logging
from collections.abc import Mapping, AsyncGenerator, AsyncIterable
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    TypeVar,
    Union,
    final, Optional, cast, )

import anyio
from anyio import CancelScope, create_memory_object_stream, to_thread, \
    create_task_group, TASK_STATUS_IGNORED, get_cancelled_exc_class
from anyio.abc import TaskStatus
from anyio.from_thread import BlockingPortal
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from typing_extensions import ParamSpec

from justscheduleit import hosting
from justscheduleit._utils import choose_anyio_backend, task_full_name, EventView, observe_event

__all__ = ["Scheduler", "SchedulerLifetime", "ScheduledTask", "TaskExecutionFlow", "arun", "run"]

from justscheduleit.hosting import ServiceLifetime, Host, HostLifetime

T = TypeVar("T")
TriggerEventT = TypeVar("TriggerEventT")
P = ParamSpec("P")

logger = logging.getLogger(__name__)


def _wrap_to_async(func: Callable[P, T] | Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
    def call_in_thread(*args, **kwargs) -> Awaitable[T]:
        return to_thread.run_sync(func, *args, **kwargs)  # type: ignore

    return func if inspect.iscoroutinefunction(func) else call_in_thread  # type: ignore


@final
class TaskExecutionFlow(Generic[T]):
    """
    Task execution context. Once started, cannot be started again.
    """

    task: ScheduledTask[T, Any]  # TODO TriggerEventT
    scheduler_lifetime: SchedulerLifetime

    _results: MemoryObjectSendStream[T]
    _results_reader: MemoryObjectReceiveStream[T]
    _observed: bool
    _scope: CancelScope | None
    """
    Graceful shutdown scope.

    1. Stop listening for new trigger events.
    2. Wait for the current task function call to complete (a task function with a CancelScope parameter can
       optimize the shutdown process internally).
    """

    __slots__ = ("task", "scheduler_lifetime", "_results", "_results_reader", "_observed", "_scope")

    def __init__(self, task: ScheduledTask[T, Any], scheduler_lifetime: SchedulerLifetime):
        self.task = task
        self.scheduler_lifetime = scheduler_lifetime

        self._results, self._results_reader = create_memory_object_stream[T]()
        self._observed = False
        self._scope = None

    def subscribe(self) -> MemoryObjectReceiveStream[T]:
        # Make sure that there is at least one subscriber, otherwise we will just spam the memory buffer
        self._observed = True
        return self._results_reader.clone()

    def _check_started(self):
        if self._scope is not None:
            raise RuntimeError("Task already started")

    async def serve(self, *, task_status: TaskStatus[CancelScope] = TASK_STATUS_IGNORED) -> None:
        self._check_started()

        shutdown_scope = self._scope = CancelScope()
        trigger = self.task.trigger(self.scheduler_lifetime)

        task_status.started(shutdown_scope)

        # Iterate manually, as we want to use the shutdown scope only for the waiting part
        if inspect.isasyncgen(trigger):
            await self._serve_with_feedback(trigger)
        else:  # AsyncIterable
            await self._serve(trigger)

    async def _serve(self, trigger: AsyncIterable[Any]) -> None:
        event_stream = trigger.__aiter__()
        shutdown_scope = cast(CancelScope, self._scope)
        async with self._results:
            try:
                while True:
                    with shutdown_scope:
                        event = await event_stream.__anext__()
                    if shutdown_scope.cancel_called:
                        break
                    await self._execute(event)
            except StopAsyncIteration:  # As per https://docs.python.org/3/reference/expressions.html#agen.__anext__
                pass  # Event stream has been exhausted

    async def _serve_with_feedback(self, trigger: AsyncGenerator[Any, T]) -> None:
        # Initialize the event stream, as per the (async) generator protocol
        move_next = trigger.asend(None)  # type: ignore
        shutdown_scope = cast(CancelScope, self._scope)
        async with self._results:
            try:
                while True:
                    with shutdown_scope:
                        event = await move_next
                    if shutdown_scope.cancel_called:
                        break
                    try:
                        result = await self._execute(event)
                        move_next = trigger.asend(result)
                    except get_cancelled_exc_class():  # noqa
                        raise  # Propagate cancellation
                    except Exception as exc:  # noqa
                        move_next = trigger.athrow(exc)
            except StopAsyncIteration:  # As per https://docs.python.org/3/reference/expressions.html#agen.asend
                pass  # Event stream has been exhausted
            finally:
                await trigger.aclose()

    async def _execute(self, event: Any) -> T:
        result = await self.task(event)
        if self._observed:
            await self._results.send(result)
        return result


@final
class ScheduledTask(Generic[T, TriggerEventT]):
    name: str
    trigger: TriggerFactory
    target: Callable[..., Awaitable[T]] | Callable[..., T]
    scheduler: Scheduler
    event_aware: bool

    _async_target: Callable[..., Awaitable[T]]
    """
    The target function, maybe wrapped to an async one.
    """

    __slots__ = ("name", "trigger", "target", "scheduler", "event_aware", "_async_target")

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.name!r}) with {self.trigger!r} trigger>"

    def __init__(self,
                 scheduler: Scheduler,
                 trigger: TriggerFactory,
                 target: Callable[..., Awaitable[T]] | Callable[..., T],
                 *,
                 name: str | None = None):
        self.name = name if name else task_full_name(target)
        self.trigger = trigger
        self.target = target
        self.scheduler = scheduler
        # Choose the right callable based on the target's signature (parameters)
        func_signature = inspect.signature(target)
        self.event_aware = len(func_signature.parameters) > 0

        self._async_target = _wrap_to_async(target)

    async def __call__(self, event: Any) -> T:
        """
        Execute the task function, on the given trigger event.
        """
        if self.event_aware:
            return await self._async_target(event)
        return await self._async_target()


@final
class SchedulerLifetime:
    """
    Scheduler execution manager, for _outside_ control of the scheduler.
    """

    _host_lifetime: HostLifetime
    _service_lifetime: ServiceLifetime

    scheduler: Scheduler
    tasks: Mapping[ScheduledTask, TaskExecutionFlow]

    __slots__ = ("scheduler", "_host_lifetime", "_service_lifetime", "tasks")

    def __init__(self, scheduler: Scheduler, service_lifetime: ServiceLifetime):
        self._host_lifetime = service_lifetime.host_lifetime
        self._service_lifetime = service_lifetime
        self.scheduler = scheduler
        self.tasks = {task: TaskExecutionFlow(task, self) for task in scheduler.tasks}

    def __repr__(self):
        return f"<{self.__class__.__name__} for {self.scheduler!r}>"

    @property
    def started(self) -> EventView:
        return self._service_lifetime.started

    @property
    def shutting_down(self) -> EventView:
        return self._service_lifetime.shutting_down

    @property
    def stopped(self) -> EventView:
        return self._service_lifetime.stopped

    @property
    def host_portal(self) -> BlockingPortal:
        return self._host_lifetime.portal

    def find_exec_for(self, task: Callable | ScheduledTask) -> TaskExecutionFlow | None:
        if isinstance(task, ScheduledTask):
            return self.tasks.get(task)

        for task_exec in self.tasks.values():
            if task_exec.task.target == task:
                return task_exec

        return None

    def shutdown(self):
        self._service_lifetime.shutdown()


Trigger = Union[
    AsyncGenerator[TriggerEventT, T],  # Duplex, two-way communication
    AsyncIterable[TriggerEventT],  # Simplex, one-way communication
]

DuplexTriggerFactory = Callable[[SchedulerLifetime], AsyncGenerator[TriggerEventT, T]]
SimplexTriggerFactory = Callable[[SchedulerLifetime], AsyncIterable[TriggerEventT]]
# TriggerFactory = Union[DuplexTriggerFactory, SimplexTriggerFactory]
TriggerFactory = Union[
    Callable[[SchedulerLifetime], AsyncGenerator[TriggerEventT, T]],
    Callable[[SchedulerLifetime], AsyncIterable[TriggerEventT]],
]


# async def no_task_error_handler(_, __):
#     return False  # Exception is not suppressed, to stop the execution flow
#
#
# async def logging_task_error_handler(task: ScheduledTask, exc: Exception) -> bool | None:
#     """
#     Default task error handler, which logs the exception and signals to continue the execution flow.
#
#     In your custom handlers, if you want to stop the execution, just return `False`. That means that the exception is
#     not suppressed. See :func:`no_task_error_handler` for an example. The same idea as with
#     :meth:`AbstractAsyncContextManager.__aexit__`.
#     """
#     logger.exception("Task %s execution failed", task.name, exc_info=exc)
#     return True



@final
class Scheduler:
    name: str
    tasks: list[ScheduledTask]

    _lifetime: SchedulerLifetime | None

    __slots__ = ("name", "tasks", "_tasks_host", "_lifetime")

    def __init__(self, name: str | None = None):
        self.name = name or "default_scheduler"
        self.tasks = []
        self._lifetime = None

    @property
    def lifetime(self) -> SchedulerLifetime:
        if self._lifetime is None:
            raise RuntimeError("Scheduler not running")
        return self._lifetime

    def __repr__(self):
        return f"<{self.__class__.__name__} with {len(self.tasks)} tasks>"

    def add_task(self,
                 trigger: TriggerFactory[TriggerEventT, T],
                 func: Callable[..., Awaitable[T]] | Callable[..., T],
                 *,
                 name: str | None = None) -> ScheduledTask[T, TriggerEventT]:
        task = ScheduledTask(self, trigger, func, name=name)
        self.tasks.append(task)
        return task

    def task(self, trigger: TriggerFactory, *, name: str | None = None) -> Callable[[Callable[P, T]], Callable[P, T]]:
        """
        Decorator to register a new task.
        """
        def decorator(func: Callable[P, T]) -> Callable[P, T]:
            self.add_task(trigger, func, name=name)
            return func

        return decorator

    async def serve(self, service_lifetime: ServiceLifetime) -> None:
        """
        Run as a hosted service.
        """
        logger.debug("Starting tasks host...")
        self._lifetime = scheduler_lifetime = SchedulerLifetime(self, service_lifetime)
        try:
            tasks_host = Host(f"{self.name}_tasks_host")
            for task, exec_flow in scheduler_lifetime.tasks.items():
                tasks_host.add_service(exec_flow.serve, name=task.name)
            tasks_host_lifetime: HostLifetime
            async with tasks_host.aserve_in(service_lifetime.host_portal) as tasks_host_lifetime:
                async with create_task_group() as tg:
                    # Service supervisor will cancel this scope on shutdown, instead of the whole service task
                    service_lifetime.graceful_shutdown_scope = tg.cancel_scope
                    # Host lifetime observers, to update the service state
                    observe_event(tg, tasks_host_lifetime.started, lambda: service_lifetime.set_started())
                    observe_event(tg, tasks_host_lifetime.shutting_down, lambda: service_lifetime.shutdown())
                    observe_event(tg, tasks_host_lifetime.stopped, lambda: service_lifetime.shutdown())
        finally:
            self._lifetime = None


async def arun(scheduler: Scheduler):
    host = Host(f"{scheduler.name}_host")
    host.services["scheduler"] = scheduler.serve
    await hosting.arun(host)


def run(scheduler: Scheduler):
    anyio.run(arun, scheduler, **choose_anyio_backend())
