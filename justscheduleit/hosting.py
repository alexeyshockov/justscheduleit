from __future__ import annotations

import inspect
import logging
import signal
import threading
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from threading import Thread
from typing import Any, Awaitable, Callable, Optional, Protocol, TypeVar, Union, final

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
from anyio.abc import TaskStatus
from anyio.from_thread import BlockingPortal, start_blocking_portal
from typing_extensions import ParamSpec

from justscheduleit._utils import HANDLED_SIGNALS, EventView, choose_anyio_backend, observe_event

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
    name: str
    host_lifetime: _HostLifetime

    started: Event
    shutting_down: Event
    stopped: Event

    graceful_shutdown_scope: CancelScope | None

    __slots__ = ("name", "host_lifetime", "started", "shutting_down", "stopped", "graceful_shutdown_scope")

    def __init__(self, host_lifetime: _HostLifetime, name: str):
        self.host_lifetime = host_lifetime
        self.name = name
        self.started = Event()
        self.shutting_down = Event()
        self.stopped = Event()
        self.graceful_shutdown_scope = None

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
    lifetime_aware: bool
    """
    Does the service function accept ServiceLifetime argument or not.
    """

    __slots__ = ("func", "task_status_aware", "lifetime_aware")

    def __init__(self, func: Callable[..., Awaitable[Any]]):
        self.func = func
        # Try to detect if the function's additional capabilities, can be overridden by the user
        func_signature = inspect.signature(func)
        self.task_status_aware = "task_status" in func_signature.parameters
        self.lifetime_aware = "service_lifetime" in func_signature.parameters

    async def execute(self, lifetime: ServiceLifetime) -> None:
        if self.task_status_aware:
            service_task_status = self._ExecStatus(lifetime)
            await self.func(task_status=service_task_status)
        elif self.lifetime_aware:
            await self.func(service_lifetime=lifetime)
        else:
            lifetime.set_started()
            await self.func()

        # TODO Service shutdown timeout


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
        logger.debug("Starting host as a service...")
        host_lifetime: HostLifetime
        async with self.host.aserve_in(service_lifetime.host_portal) as host_lifetime:
            async with create_task_group() as tg:
                # Service supervisor will cancel this scope on shutdown, instead of the whole service task
                service_lifetime.graceful_shutdown_scope = tg.cancel_scope
                # Host lifetime observers, to update the service state
                observe_event(tg, host_lifetime.started, lambda: service_lifetime.set_started())
                observe_event(tg, host_lifetime.shutting_down, lambda: service_lifetime.shutdown())
                observe_event(tg, host_lifetime.stopped, lambda: service_lifetime.shutdown())


def _create_service(target: HostedService) -> Service:
    if inspect.iscoroutinefunction(target):
        return CoroutineService(target).execute
    else:
        return SyncService(target).execute


@final
class Host:
    name: str
    services: dict[str, Service]
    _lifetime: _HostLifetime | None

    __slots__ = ("name", "services", "_lifetime")

    def __init__(self, name: str | None = None):
        self.name = name or "default_host"
        self.services = {}
        self._lifetime = None

    def __repr__(self):
        return f"<{self.__class__.__name__} services={self.services.keys()!r}>"

    def add_service(self, func: HostedService, name: str | None = None) -> Service:
        if name:
            service_name = name
        elif hasattr(func, "__name__"):
            service_name = func.__name__
        else:
            raise ValueError("Service name must be provided")

        if service_name in self.services:
            raise ValueError(f"Service {service_name} is already registered")
        service = self.services[service_name] = _create_service(func)
        return service

    def service(self, name: str | None = None) -> Callable[[Callable[P, T]], Callable[P, T]]:
        """
        Decorator to register a hosted service.
        """

        def decorator(func: Callable[P, T]) -> Callable[P, T]:
            self.add_service(func, name)
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
    async def aserve(self) -> AsyncGenerator[HostLifetime, Any]:
        logger.debug("Starting host...")
        async with BlockingPortal() as portal, self.aserve_in(portal) as lifetime:
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
            with thread.wrap_async_context_manager(self.aserve_in(thread)) as lifetime:
                yield lifetime

    @asynccontextmanager
    async def aserve_in(self, portal: BlockingPortal) -> AsyncGenerator[HostLifetime, Any]:
        self._check_running()
        try:
            async with create_task_group() as exec_tg:
                lifetime = self._lifetime = _HostLifetime(portal, exec_tg.cancel_scope)
                exec_tg.start_soon(self._execute, lifetime)
                yield lifetime
                lifetime.shutdown()
        finally:
            self._lifetime = None

    async def aexecute(self, *, task_status: TaskStatus[HostLifetime] = TASK_STATUS_IGNORED) -> None:
        self._check_running()
        try:
            logger.debug("Starting host...")
            async with BlockingPortal() as portal:
                exec_tg = portal._task_group  # noqa
                lifetime = self._lifetime = _HostLifetime(portal, exec_tg.cancel_scope)
                task_status.started(lifetime)
                await self._execute(lifetime)
        finally:
            self._lifetime = None

    async def _execute(self, lifetime: _HostLifetime) -> None:  # noqa: C901 (too complex)
        services = self.services.copy()  # Freeze the state
        service_lifetimes: dict[str, _ServiceLifetime] = {}
        services_started = 0
        all_services_started = Event()
        services_stopped = 0
        all_services_stopped = Event()

        def service_started(service_lifetime: _ServiceLifetime):
            nonlocal services_started
            logger.debug(f"{service_lifetime.name} started")
            services_started += 1
            if services_started == len(services):
                # TODO Host start timeout
                all_services_started.set()

        def service_stopped(service_lifetime: _ServiceLifetime):
            nonlocal services_stopped
            service_lifetime.stopped.set()
            logger.debug(f"{service_lifetime.name} stopped")
            services_stopped += 1
            if services_stopped == len(services):
                # TODO Host shutdown timeout
                all_services_stopped.set()

        async def observe_service_started(service_lifetime: _ServiceLifetime):
            await service_lifetime.started.wait()
            service_started(service_lifetime)

        async def observe_service_shutdown(service_lifetime: _ServiceLifetime, scope: CancelScope):
            await service_lifetime.shutting_down.wait()
            if not service_lifetime.stopped.is_set():
                scope = service_lifetime.graceful_shutdown_scope if service_lifetime.graceful_shutdown_scope else scope
                scope.cancel()

        async def supervise_service(name: str, service: Service):
            async with create_task_group() as service_tg:
                service_lifetime = service_lifetimes[name] = _ServiceLifetime(lifetime, name)
                service_tg.start_soon(observe_service_started, service_lifetime)
                service_tg.start_soon(observe_service_shutdown, service_lifetime, service_tg.cancel_scope)
                try:
                    logger.debug(f"Starting {name}...")
                    await service(service_lifetime)
                except get_cancelled_exc_class():  # noqa
                    raise  # Propagate the cancellation
                except Exception:  # noqa
                    logger.exception(f"{name} crashed")
                finally:
                    service_stopped(service_lifetime)
                    service_tg.cancel_scope.cancel()  # Shutdown all the observers

        def shutdown():
            for service in service_lifetimes.values():
                service.shutdown()

        def stop():
            tg.cancel_scope.cancel()

        async def observe_host_shutdown():
            await lifetime.shutting_down.wait()
            if not lifetime.started.is_set():
                logger.debug("Startup hasn't been completed, canceling everything")
                stop()
            else:
                logger.debug("Shutting down...")
                shutdown()

        async def observe_all_services_started():
            await all_services_started.wait()
            logger.debug("All services started")
            lifetime.started.set()

        try:
            if not services:
                logger.warning("No services to run")

            async with create_task_group() as tg:
                tg.start_soon(observe_host_shutdown)  # type: ignore
                tg.start_soon(observe_all_services_started)  # type: ignore

                logger.debug("Starting services...")
                for service_name, service_impl in services.items():
                    tg.start_soon(supervise_service, service_name, service_impl)

                await all_services_stopped.wait()
                logger.debug("All services stopped")
                tg.cancel_scope.cancel()  # Shutdown all the observers
        finally:
            lifetime.stopped.set()
            logger.debug("Host stopped")


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
