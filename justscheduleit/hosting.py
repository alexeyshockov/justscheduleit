from __future__ import annotations

import dataclasses as dc
import inspect
import logging
import signal
import threading
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from threading import Thread
from typing import Any, Awaitable, Callable, Optional, Protocol, TypeVar, Union, cast, final

import anyio
from anyio import (
    TASK_STATUS_IGNORED,
    BusyResourceError,
    CancelScope,
    Event,
    create_task_group,
    get_cancelled_exc_class,
    open_signal_receiver,
)
from anyio.abc import TaskGroup, TaskStatus
from anyio.from_thread import BlockingPortal, start_blocking_portal
from typing_extensions import Mapping, ParamSpec

from justscheduleit._utils import HANDLED_SIGNALS, EventView, choose_anyio_backend, observe_event, task_full_name

T = TypeVar("T")
TriggerEventT = TypeVar("TriggerEventT")
P = ParamSpec("P")

# Lifespan = Union[
#     AbstractAsyncContextManager,
#     AbstractContextManager,
# ]

HostedService = Union[
    # AbstractAsyncContextManager,
    # AbstractContextManager,
    Callable[P, Awaitable[T]], Callable[P, T]
]


logger = logging.getLogger(__name__)


class Asgi3Adapter:
    # Implement later, to run a host in an ASGI3-compatible server.
    # Not something really useful, as you can always run a host as a Starlette/FastAPI lifespan.
    pass


class HostLifetime(Protocol):
    # exit_code: int = 0  # TODO Support later

    @property
    def portal(self) -> BlockingPortal: ...

    @property
    def started(self) -> EventView: ...

    @property
    def shutting_down(self) -> EventView: ...

    @property
    def stopped(self) -> EventView: ...

    @property
    def same_thread(self) -> bool: ...

    def shutdown(self) -> None: ...

    def stop(self) -> None: ...


class _HostLifetime:
    thread_id: int
    portal: BlockingPortal
    _scope: CancelScope

    started: Event  # Ideally an event view, without set()...
    shutting_down: Event
    stopped: Event

    __slots__ = ("thread_id", "portal", "_scope", "started", "shutting_down", "stopped")

    def __init__(self, portal: BlockingPortal, scope: CancelScope):
        self.thread_id = threading.get_ident()
        self.portal = portal
        self._scope = scope

        self.started = Event()
        self.shutting_down = Event()
        self.stopped = Event()

    @property
    def same_thread(self) -> bool:
        return threading.get_ident() == self.thread_id

    def shutdown(self) -> None:
        if self.stopped.is_set() or self.shutting_down.is_set():
            return

        if self.same_thread:
            self.shutting_down.set()
        else:
            self.portal.start_task_soon(self.shutting_down.set)  # noqa

    def stop(self) -> None:
        if self.same_thread:
            self._scope.cancel()
        else:
            self.portal.start_task_soon(self._scope.cancel)  # noqa


class ServiceLifetime(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def host_lifetime(self) -> HostLifetime: ...

    @property
    def host_portal(self) -> BlockingPortal: ...

    @property
    def started(self) -> EventView: ...

    @property
    def shutting_down(self) -> EventView: ...

    @property
    def stopped(self) -> EventView: ...

    graceful_shutdown_scope: CancelScope | None
    """
    A scope for a graceful shutdown of the service.

    By default, when not set, the service task group will be canceled. If set, this scope will be canceled instead.
    """

    def set_started(self) -> None: ...

    def shutdown(self) -> None: ...


class _ServiceLifetime:  # (ServiceLifetime, ServiceStateManager)
    service: ServiceDescriptor
    host_lifetime: _HostLifetime

    started: Event
    shutting_down: Event
    stopped: Event

    graceful_shutdown_scope: CancelScope | None

    __slots__ = ("service", "host_lifetime", "started", "shutting_down", "stopped", "graceful_shutdown_scope")

    def __init__(self, service: ServiceDescriptor, host_lifetime: _HostLifetime):
        self.service = service
        self.host_lifetime = host_lifetime
        self.started = Event()
        self.shutting_down = Event()
        self.stopped = Event()
        self.graceful_shutdown_scope = None

    @property
    def name(self):
        return self.service.name

    @property
    def host_portal(self) -> BlockingPortal:
        return self.host_lifetime.portal

    def set_started(self) -> None:
        if self.host_lifetime.same_thread:
            self.started.set()
        else:
            self.host_portal.start_task_soon(self.started.set)  # type: ignore

    def shutdown(self):
        if self.host_lifetime.same_thread:
            self.shutting_down.set()
        else:
            self.host_portal.start_task_soon(self.shutting_down.set)  # type: ignore


Service = Callable[[ServiceLifetime], Awaitable[None]]


@final
class CoroutineService:
    class _ExecStatus(TaskStatus[Optional[CancelScope]]):
        def __init__(self, lifetime: ServiceLifetime):
            self.lifetime = lifetime

        def started(self, value: Optional[CancelScope] = None) -> None:
            self.lifetime.set_started()
            if value:
                self.lifetime.graceful_shutdown_scope = value

    func: Callable[..., Awaitable[Any]]
    task_status_aware: bool
    """
    Does the service function accept AnyIO task status argument or not.
    """

    __slots__ = ("func", "task_status_aware", "lifetime_aware")

    def __init__(self, func: Callable[..., Awaitable[Any]]):
        self.func = func
        # Try to detect if the function's additional capabilities, can be overridden by the user
        func_signature = inspect.signature(func)
        self.task_status_aware = "task_status" in func_signature.parameters

    async def execute(self, lifetime: ServiceLifetime) -> None:
        if self.task_status_aware:
            service_task_status = self._ExecStatus(lifetime)
            await self.func(task_status=service_task_status)
        else:
            lifetime.set_started()
            await self.func()


@final
class SyncService:
    class _Thread(Thread):
        def __init__(self, service: SyncService, lifetime: ServiceLifetime):
            self.service = service
            self.lifetime = lifetime
            self.completed = Event()
            self.exc: BaseException | None = None
            super().__init__(daemon=True, name=lifetime.name)

        async def ajoin(self):
            await self.completed.wait()
            if self.exc:
                raise self.exc

        def run(self):
            service = self.service
            lifetime = self.lifetime
            host_portal = lifetime.host_portal
            try:
                if service.lifetime_aware:
                    service.func(lifetime)
                else:
                    lifetime.set_started()
                    service.func()
            except Exception as exc:  # noqa
                self.exc = exc
            finally:
                try:
                    host_portal.start_task_soon(self.completed.set)  # type: ignore
                except RuntimeError:
                    logger.warning("A sync service has completed, but the portal is already closed")

    func: Callable[..., Any]
    lifetime_aware: bool
    """
    Does the service function accept ServiceLifetime argument or not.
    """

    __slots__ = ("func", "lifetime_aware")

    def __init__(self, func: Callable[..., Awaitable[Any]]):
        self.func = func
        # Try to detect if the function's additional capabilities, can be overridden by the user
        func_signature = inspect.signature(func)
        self.lifetime_aware = "service_lifetime" in func_signature.parameters

    async def execute(self, lifetime: ServiceLifetime) -> None:
        service_thread = self._Thread(self, lifetime)
        with CancelScope() as shutdown_scope:
            lifetime.graceful_shutdown_scope = shutdown_scope
            service_thread.start()
            await service_thread.ajoin()

        # Service is shutting down (the scope above has been cancelled), wait for the target function to complete in
        # the thread
        await service_thread.ajoin()
        # Unfortunately, there is no way to force a thread to stop, so we have to wait and hope that the target
        # function periodically checks the shutdown event


@final
class HostService:
    """
    Mount an existing host as a service.
    """

    host: Host

    __slots__ = ("host",)

    def __init__(self, host: Host):
        self.host = host

    async def execute(self, service_lifetime: ServiceLifetime) -> None:
        portal = service_lifetime.host_portal
        logger.debug("Starting host as a service...")
        host_lifetime: HostLifetime
        async with self.host.aserve(portal) as host_lifetime:
            async with create_task_group() as obs_tg:
                # Service supervisor will cancel this (inner) scope on shutdown, not the whole task. So the context
                # manager will exit normally, shutting down the host.
                service_lifetime.graceful_shutdown_scope = obs_tg.cancel_scope
                # Host lifetime observers, to update the service state
                observe_event(obs_tg, host_lifetime.started, lambda: service_lifetime.set_started())
                observe_event(obs_tg, host_lifetime.shutting_down, lambda: service_lifetime.shutdown())
                observe_event(obs_tg, host_lifetime.stopped, lambda: service_lifetime.shutdown())


def _create_service(target: HostedService) -> Service:
    # If the target is not callable, a TypeError will be raised
    target_signature = inspect.signature(target)

    if inspect.iscoroutinefunction(target):
        if len(target_signature.parameters) == 1 and "service_lifetime" in target_signature.parameters:
            return target  # type: ignore
        return CoroutineService(target).execute
    else:
        return SyncService(target).execute


@final
@dc.dataclass(frozen=True)  # Enable slots when Python 3.10+
class ServiceDescriptor:
    func: Service
    name: str

    # Enable kw_only when Python 3.10+, for optional arguments
    daemon: bool = False
    # start_timeout: float | None = None  # TODO Implement later


class _ServiceSupervisor:
    lifetime: _ServiceLifetime

    __slots__ = ("lifetime",)

    def __init__(self, lifetime: _ServiceLifetime):
        self.lifetime = lifetime

    def shutdown(self):
        self.lifetime.shutdown()

    async def execute(self):
        name = self.lifetime.service.name
        service_lifetime = self.lifetime
        service_func = self.lifetime.service.func

        def service_stopped():
            service_lifetime.stopped.set()
            logger.debug(f"{name} stopped")

        async def observe_service_started():
            await service_lifetime.started.wait()
            logger.debug(f"{name} started")

        async def observe_service_shutdown(scope: CancelScope):
            await service_lifetime.shutting_down.wait()
            if not service_lifetime.stopped.is_set():
                scope = service_lifetime.graceful_shutdown_scope if service_lifetime.graceful_shutdown_scope else scope
                scope.cancel()

        async with create_task_group() as service_tg:
            service_tg.start_soon(observe_service_started)
            service_tg.start_soon(observe_service_shutdown, service_tg.cancel_scope)
            try:
                logger.debug(f"Starting {name}...")
                await service_func(service_lifetime)
            except get_cancelled_exc_class():  # noqa
                raise  # Propagate the cancellation
            except Exception:  # noqa
                logger.exception(f"{name} crashed")
            finally:
                service_stopped()
                service_tg.cancel_scope.cancel()  # Shutdown all the observers


class _ServicesSupervisor:
    services: list[_ServiceSupervisor]
    host_lifetime: _HostLifetime

    __slots__ = ("services", "host_lifetime")

    def __init__(self, host: Host, host_lifetime: _HostLifetime):
        self.host_lifetime = host_lifetime
        self.services = [
            _ServiceSupervisor(_ServiceLifetime(service, host_lifetime)) for service in host.services.values()
        ]

    async def execute(self):  # noqa: C901 (ignore complexity)
        host_lifetime = self.host_lifetime
        services = self.services
        services_cnt = len(services)
        foreground_services_cnt = sum(not service.lifetime.service.daemon for service in services)
        services_started = 0
        all_services_started = Event()
        foreground_services_stopped = 0  # Non-daemon services
        all_foreground_services_stopped = Event()
        services_stopped = 0
        all_services_stopped = Event()

        async def observe_service_started(service_lifetime: _ServiceLifetime):
            nonlocal services_started
            await service_lifetime.started.wait()
            services_started += 1
            if services_started == services_cnt:
                # TODO Host start timeout
                all_services_started.set()

        async def observe_service_stopped(service_lifetime: _ServiceLifetime):
            nonlocal services_stopped, foreground_services_stopped
            await service_lifetime.stopped.wait()
            if not service_lifetime.service.daemon:
                foreground_services_stopped += 1
            services_stopped += 1
            if foreground_services_stopped == foreground_services_cnt:
                all_foreground_services_stopped.set()
            if services_stopped == services_cnt:
                all_services_stopped.set()

        def start():
            for service in services:
                services_tg.start_soon(observe_service_started, service.lifetime)
                services_tg.start_soon(observe_service_stopped, service.lifetime)
                services_tg.start_soon(service.execute)  # type: ignore

        def shutdown():
            for service in services:
                service.shutdown()

        def stop():
            services_tg.cancel_scope.cancel()

        async def observe_host_shutdown():
            await host_lifetime.shutting_down.wait()
            logger.debug("Shutting down...")
            shutdown()

        async def observe_all_services_started():
            await all_services_started.wait()
            logger.debug("All services started")
            host_lifetime.started.set()

        if not services:
            logger.warning("No services to run")
            return

        async with create_task_group() as services_tg:  # Maybe exec_tg from above...
            services_tg.start_soon(observe_host_shutdown)  # type: ignore
            services_tg.start_soon(observe_all_services_started)  # type: ignore

            logger.debug("Starting services...")
            start()

            await all_foreground_services_stopped.wait()
            logger.debug("All foreground services stopped, shutting down daemon services...")
            shutdown()

            await all_services_stopped.wait()
            logger.debug("All services stopped")
            stop()  # Shutdown all the observers


@final
class Host:
    name: str
    _services: dict[str, ServiceDescriptor]
    _lifetime: _HostLifetime | None

    __slots__ = ("name", "_services", "_lifetime")

    def __init__(self, name: str | None = None):
        self.name = name or "default_host"
        self._services = {}
        self._lifetime = None

    def __repr__(self):
        return f"<{self.__class__.__name__} services={self._services.keys()!r}>"

    @property
    def services(self) -> Mapping[str, ServiceDescriptor]:
        return self._services

    def _add_service(self, service: ServiceDescriptor) -> ServiceDescriptor:
        if service.name in self._services:
            raise ValueError(f"Service {service.name} is already registered")
        self._services[service.name] = service
        return service

    def add_service(self, func: Service, /, *, name: str | None = None, daemon: bool = False) -> ServiceDescriptor:
        """
        Register a service in the host.

        :param func: A service function.
        :param name: An (optional) uniq name for the service. If not provided, will be generated from the function name.
        :param daemon: If True, the service won't prevent the host from stopping when all non-daemon services are done.
        :return: The service descriptor.
        """
        return self._add_service(ServiceDescriptor(func, name if name else task_full_name(func), daemon))

    def service(self, name: str | None = None, /, *, daemon: bool = False) -> Callable[[Callable[P, T]], Callable[P, T]]:
        """
        Decorator to register an (async) function as a hosted service.
        """

        def decorator(func: Callable[P, T]) -> Callable[P, T]:
            self._add_service(
                ServiceDescriptor(_create_service(func), name=name if name else task_full_name(func), daemon=daemon)
            )
            return func

        return decorator

    @property
    def lifetime(self):
        if not self._lifetime:
            raise RuntimeError("Host is not running")

    def _check_running(self):
        if self._lifetime is not None:
            raise BusyResourceError("running")  # Like an AnyIO resource guard

    @asynccontextmanager
    async def aserve(self, portal: BlockingPortal | None = None) -> AsyncGenerator[HostLifetime, Any]:
        """
        Start the host in the current event loop.

        :param portal: An optional portal for the current event loop (thread), if already created.
        :return: A context manager that yields the host lifetime.
        """

        logger.debug("Starting host...")
        if portal is None:
            async with BlockingPortal() as portal, self._aserve_in(portal) as lifetime:
                yield lifetime
        else:
            async with create_task_group() as exec_tg, self._aserve_in(portal, exec_tg) as lifetime:
                yield lifetime

    @contextmanager
    def serve(self) -> Generator[HostLifetime, Any, None]:
        """
        Start the host in a separate thread, on a separate event loop.

        Intended mainly for integration with legacy apps. Like when you have an old (not async) app and want to run some
        hosted services around it.

        In general, do prefer :meth:`aserve` instead.
        """
        logger.debug("Starting host in a separate thread...")
        with start_blocking_portal(**choose_anyio_backend()) as thread:
            with thread.wrap_async_context_manager(self._aserve_in(thread)) as lifetime:
                yield lifetime

    @asynccontextmanager
    async def _aserve_in(self, portal: BlockingPortal, exec_tg: TaskGroup | None = None):
        exec_tg = portal._task_group if exec_tg is None else exec_tg  # noqa
        lifetime = cast(HostLifetime, await exec_tg.start(self._execute, portal, exec_tg))
        yield lifetime
        lifetime.shutdown()

    async def aexecute(
        self, portal: BlockingPortal | None = None, *, task_status: TaskStatus[HostLifetime] = TASK_STATUS_IGNORED
    ) -> None:
        logger.debug("Starting host...")
        if portal is None:
            async with BlockingPortal() as portal:
                exec_tg = portal._task_group  # noqa
                lifetime: HostLifetime = await exec_tg.start(self._execute, portal, exec_tg)
                task_status.started(lifetime)
        else:
            async with create_task_group() as exec_tg:
                lifetime: HostLifetime = await exec_tg.start(self._execute, portal, exec_tg)
                task_status.started(lifetime)

    async def _execute(
        self,
        portal: BlockingPortal,
        exec_tg: TaskGroup,
        *,
        task_status: TaskStatus[_HostLifetime] = TASK_STATUS_IGNORED,
    ):
        self._check_running()
        lifetime = self._lifetime = _HostLifetime(portal, exec_tg.cancel_scope)
        try:
            task_status.started(lifetime)
            services_supervisor = _ServicesSupervisor(self, lifetime)
            await services_supervisor.execute()
        finally:
            lifetime.stopped.set()
            logger.debug("Host stopped")
            self._lifetime = None


async def arun(host: Host):
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("Signals can only be installed on the main thread")

    async with create_task_group() as tg:
        host_lifetime: HostLifetime = await tg.start(host.aexecute)
        observe_event(tg, host_lifetime.stopped, lambda: tg.cancel_scope.cancel())
        with open_signal_receiver(*HANDLED_SIGNALS) as signals:
            async for sig in signals:
                if not host_lifetime.shutting_down.is_set():  # First Ctrl+C (or other termination signal)
                    logger.info("Shutting down...")
                    host_lifetime.shutdown()
                    continue
                if sig == signal.SIGINT:  # Ctrl+C again
                    logger.warning("Forced shutdown")
                    host_lifetime.stop()


def run(host: Host):
    anyio.run(arun, host, **choose_anyio_backend())
