from __future__ import annotations

import inspect
import logging
from collections.abc import AsyncGenerator, AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Generic,
    Protocol,
    TypeVar,
    Union,
    cast,
    final,
)

import anyio
from anyio import (
    CancelScope,
    create_memory_object_stream,
    create_task_group,
    get_cancelled_exc_class,
    to_thread,
)
from anyio.from_thread import BlockingPortal
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from typing_extensions import ParamSpec, overload

from justscheduleit._utils import NULL_CM, EventView, choose_anyio_backend, observe_event, task_full_name
from justscheduleit.hosting import Host, HostLifetime, ServiceLifetime

if TYPE_CHECKING:  # Optional dependencies
    # noinspection PyPackageRequirements
    from opentelemetry.trace import Tracer, TracerProvider

T = TypeVar("T")
TriggerEventT = TypeVar("TriggerEventT")
TaskT = Union[Callable[..., Awaitable[T]], Callable[..., T]]
P = ParamSpec("P")

__all__ = [
    "Scheduler",
    "SchedulerLifetime",
    "ScheduledTask",
    "TaskExecutionFlow",
    "TaskT",
    "Trigger",
    "TriggerFactory",
    "arun",
    "run",
    "aserve",
    "serve",
]

logger = logging.getLogger(__name__)


class TaskExecutionFlow(Protocol[T, TriggerEventT]):
    @property
    def task(self) -> ScheduledTask[T, TriggerEventT]: ...

    def subscribe(self) -> MemoryObjectReceiveStream[T]: ...


class _TaskExecutionFlow(Generic[T, TriggerEventT]):
    task: ScheduledTask[T, TriggerEventT]
    results: MemoryObjectSendStream[T]
    _results_reader: MemoryObjectReceiveStream[T]

    __slots__ = ("task", "results", "_results_reader")

    def __init__(self, task: ScheduledTask[T, Any]):
        self.task = task
        self.results, self._results_reader = create_memory_object_stream[T]()

    @property
    def is_observed(self) -> bool:
        return self._results_reader.statistics().open_receive_streams > 1

    def subscribe(self) -> MemoryObjectReceiveStream[T]:
        return self._results_reader.clone()


class _TaskExecution(Generic[T, TriggerEventT]):
    task: ScheduledTask[T, TriggerEventT]
    exec_flow: _TaskExecutionFlow[T, TriggerEventT]
    trigger: AsyncGenerator[TriggerEventT, T]

    _async_target: Callable[..., Awaitable[T]]
    """
    The target function, maybe wrapped (if the task's target is a sync function).
    """

    _scope: CancelScope | None
    """
    Graceful shutdown scope.

    1. Stop listening for new trigger events.
    2. Wait for the current task function call to complete (a task function with a CancelScope parameter can
       optimize the shutdown process internally).
    """

    _tracer: Tracer | None

    __slots__ = ("task", "exec_flow", "trigger", "_async_target", "_scope", "_tracer")

    def __init__(self, exec_flow: _TaskExecutionFlow[T, TriggerEventT], scheduler_lifetime: SchedulerLifetime):
        task = exec_flow.task
        self.task = task
        self.exec_flow = exec_flow
        self.trigger = task.trigger(scheduler_lifetime)

        t = task.target
        self._async_target = cast(
            Callable[..., Awaitable[T]], t if inspect.iscoroutinefunction(t) else partial(to_thread.run_sync, t)
        )

        self._scope = None
        self._tracer = scheduler_lifetime.scheduler._tracer  # noqa

    @property
    def has_started(self) -> bool:
        return self._scope is not None

    async def __call__(self, service_lifetime: ServiceLifetime) -> None:
        if self.has_started:
            raise RuntimeError("Task already started")

        self._scope = service_lifetime.graceful_shutdown_scope = shutdown_scope = CancelScope()
        service_lifetime.set_started()

        # Iterate manually, as we want to use the shutdown scope only for the waiting part

        trigger = self.trigger
        # Initialize the event stream, as per the (async) generator protocol
        move_next = trigger.asend(None)  # type: ignore
        async with self.exec_flow.results:
            try:
                while True:
                    with shutdown_scope:
                        event = await move_next
                    if shutdown_scope.cancel_called:
                        break
                    try:
                        result = await self._execute_task(event)
                        move_next = trigger.asend(result)
                    except get_cancelled_exc_class():  # noqa
                        raise  # Propagate cancellation
                    except Exception as exc:  # noqa
                        move_next = trigger.athrow(exc)
            except StopAsyncIteration:  # As per https://docs.python.org/3/reference/expressions.html#agen.asend
                pass  # Event stream has been exhausted
            finally:
                await trigger.aclose()

    async def _execute_task(self, event: Any) -> T:
        root_span = NULL_CM
        if self._tracer:
            # TODO Inside the thread, for sync tasks?..
            root_span = self._tracer.start_as_current_span(self.task.name)  # type: ignore
        with root_span:
            if self.task.event_aware:
                result = await self._async_target(event)
            else:
                result = await self._async_target()
        if self.exec_flow.is_observed:
            await self.exec_flow.results.send(result)
        return result


@final
class ScheduledTask(Generic[T, TriggerEventT]):
    name: str
    trigger: TriggerFactory[TriggerEventT, T]
    target: TaskT[T]
    event_aware: bool
    """
    Whether the target function accepts an event parameter.

    By default, detected based on the target's signature (number of parameters). Can be overridden (in case the
    autodetect value is incorrect).
    """

    __slots__ = ("name", "trigger", "target", "event_aware")

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.name!r}) with {self.trigger!r} trigger>"

    def __init__(
        self,
        trigger: TriggerFactory,
        target: TaskT[T],
        *,
        name: str | None = None,
    ):
        self.name = name if name else task_full_name(target)
        self.trigger = trigger
        self.target = target
        # Choose the right callable based on the target's signature (parameters)
        func_signature = inspect.signature(target)
        self.event_aware = len(func_signature.parameters) > 0


@final
class SchedulerLifetime:
    """
    Scheduler execution manager, for _outside_ control of the scheduler.
    """

    scheduler: Scheduler
    tasks: Mapping[ScheduledTask, TaskExecutionFlow]

    _service_lifetime: ServiceLifetime

    __slots__ = ("scheduler", "tasks", "_service_lifetime")

    def __init__(self, scheduler, exec_flows, service_lifetime):
        self.scheduler = scheduler
        self.tasks = {exec_flow.task: exec_flow for exec_flow in exec_flows}
        self._service_lifetime = service_lifetime

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
        return self._service_lifetime.host_lifetime.portal

    @overload
    def find_exec_for(self, task: TaskT[T]) -> TaskExecutionFlow[T, Any] | None: ...

    @overload
    def find_exec_for(self, task: ScheduledTask[T, TriggerEventT]) -> TaskExecutionFlow[T, TriggerEventT] | None: ...

    def find_exec_for(self, task: TaskT[T] | ScheduledTask[T, Any]) -> TaskExecutionFlow[T, Any] | None:
        if isinstance(task, ScheduledTask):
            return self.tasks.get(task)

        for task_exec in self.tasks.values():
            if task_exec.task.target == task:
                return task_exec

        return None

    def shutdown(self):
        self._service_lifetime.shutdown()


Trigger = AsyncGenerator[TriggerEventT, T]
TriggerFactory = Callable[[SchedulerLifetime], AsyncGenerator[TriggerEventT, T]]


@final
class Scheduler:
    name: str
    tasks: list[ScheduledTask]

    _lifetime: SchedulerLifetime | None
    _tracer: Tracer | None

    __slots__ = ("name", "tasks", "_lifetime", "_tracer")

    def __init__(self, name: str | None = None):
        self.name = name or "default_scheduler"
        self.tasks = []
        self._lifetime = None
        try:  # By default, try to enable OpenTelemetry instrumentation
            self.instrument()
        except RuntimeError:
            self._tracer = None

    def instrument(self, tracer_provider: TracerProvider | None = None):
        if self._is_running:
            raise RuntimeError("Cannot change instrumentation setting on a running scheduler")
        try:
            # noinspection PyPackageRequirements
            from opentelemetry.trace import get_tracer_provider

            tracer_provider = tracer_provider or get_tracer_provider()
            self._tracer = tracer_provider.get_tracer(__name__)  # TODO Version
        except ImportError:
            raise RuntimeError("OpenTelemetry package is not available") from None

    def uninstrument(self):
        if self._is_running:
            raise RuntimeError("Cannot change instrumentation setting on a running scheduler")
        self._tracer = None

    @property
    def _is_running(self) -> bool:
        return self._lifetime is not None

    @property
    def lifetime(self) -> SchedulerLifetime:
        if self._lifetime is None:
            raise RuntimeError("Scheduler not running")
        return self._lifetime

    def __repr__(self):
        return f"<{self.__class__.__name__} with {len(self.tasks)} tasks>"

    def add_task(
        self,
        trigger: TriggerFactory[TriggerEventT, T],
        func: TaskT[T],
        *,
        name: str | None = None,
    ) -> ScheduledTask[T, TriggerEventT]:
        task = ScheduledTask(trigger, func, name=name)
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

    async def execute(self, service_lifetime: ServiceLifetime) -> None:
        """
        Run as a hosted service.
        """
        logger.debug("Starting tasks host...")
        tasks = [_TaskExecutionFlow(task) for task in self.tasks]
        self._lifetime = scheduler_lifetime = SchedulerLifetime(self, tasks, service_lifetime)
        try:
            tasks_host = Host(f"{self.name}_tasks_host")
            for exec_flow in tasks:
                task_exec = _TaskExecution(exec_flow, scheduler_lifetime)
                tasks_host.services[exec_flow.task.name] = task_exec
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


def _host(scheduler: Scheduler):
    host = Host(f"{scheduler.name}_host")
    host.services["scheduler"] = scheduler.execute
    return host


@asynccontextmanager
async def aserve(scheduler: Scheduler) -> AsyncIterator[HostLifetime]:
    async with _host(scheduler).aserve() as host_lifetime:
        yield host_lifetime


@contextmanager
def serve(scheduler: Scheduler) -> Iterator[HostLifetime]:
    with _host(scheduler).serve() as host_lifetime:
        yield host_lifetime


async def arun(scheduler: Scheduler):
    from justscheduleit import hosting

    await hosting.arun(_host(scheduler))


def run(scheduler: Scheduler):
    anyio.run(arun, scheduler, **choose_anyio_backend())
