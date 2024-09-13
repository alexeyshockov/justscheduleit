from __future__ import annotations

import dataclasses as dc
from os import getenv
from typing import Any, Callable, cast

import uvicorn
from anyio import TASK_STATUS_IGNORED, Event, create_task_group
from anyio.abc import TaskStatus

from justscheduleit._utils import observe_event
from justscheduleit.hosting import ServiceLifetime


# TODO Http endpoint


class UvicornService:
    @dc.dataclass(frozen=True, slots=True)
    class _ServerLifetime:
        server: uvicorn.Server
        started: Event = dc.field(default_factory=Event)
        shutting_down: Event = dc.field(default_factory=Event)
        stopped: Event = dc.field(default_factory=Event)

        def shutdown(self):
            self.server.should_exit = True

    __slots__ = ("config",)

    def __init__(self, config: uvicorn.Config):
        self.config = config

    @classmethod
    def for_app(cls, app: Callable[..., Any]):
        return cls(
            uvicorn.Config(
                app,
                host=getenv("UVICORN_HOST", "127.0.0.1"),
                port=int(getenv("UVICORN_PORT", "8000")),
                # log_config=None,  # Do not override the logging configuration
            )
        )

    async def __call__(self, service_lifetime: ServiceLifetime):
        async with create_task_group() as tg:
            # It is hard to use server.serve() directly, because it overrides the signal handlers. A workaround is to
            # call it in a separate thread, but currently it looks like an overkill.
            server_lifetime = cast(UvicornService._ServerLifetime, await tg.start(self._serve))

            async with create_task_group() as obs_tg:
                # Service supervisor will cancel this (inner) task group on shutdown, not the whole task
                service_lifetime.graceful_shutdown_scope = obs_tg.cancel_scope
                observe_event(obs_tg, server_lifetime.started, lambda: service_lifetime.set_started())
                observe_event(obs_tg, server_lifetime.shutting_down, lambda: service_lifetime.shutdown())
                observe_event(obs_tg, server_lifetime.stopped, lambda: service_lifetime.shutdown())

            server_lifetime.shutdown()

    # See uvicorn.Server._serve() for the original implementation
    async def _serve(self, task_status: TaskStatus[_ServerLifetime] = TASK_STATUS_IGNORED):
        server = uvicorn.Server(config=self.config)
        server_lifetime = self._ServerLifetime(server)
        task_status.started(server_lifetime)
        try:
            config = self.config
            if not config.loaded:
                config.load()

            server.lifespan = config.lifespan_class(config)

            await server.startup()
            server_lifetime.started.set()
            await server.main_loop()
            server_lifetime.shutting_down.set()
            await server.shutdown()
        finally:
            server_lifetime.stopped.set()
