#!/usr/bin/env python

import logging
import time

import anyio
from anyio import TASK_STATUS_IGNORED, CancelScope
from anyio.abc import TaskStatus

from justscheduleit.hosting import Host, ServiceLifetime

logging.basicConfig()
logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

host = Host()


@host.service()
def a_sync_service(service_lifetime: ServiceLifetime):
    print("Service started")
    service_lifetime.set_started()
    while not service_lifetime.stopped:
        print("Service running")
        time.sleep(1)
    print("Service stopped")


@host.service()
async def an_async_func():
    print(f"{an_async_func.__name__} started")
    while True:
        print(f"{an_async_func.__name__} running")
        await anyio.sleep(1)


@host.service()
async def an_async_service(service_lifetime: ServiceLifetime):
    print(f"{an_async_service.__name__} started")
    service_lifetime.set_started()
    while True:
        print(f"{an_async_service.__name__} running")
        await anyio.sleep(1)


@host.service()
async def a_graceful_async_service(service_lifetime: ServiceLifetime):
    print(f"{a_graceful_async_service.__name__} started")
    with CancelScope() as scope:
        service_lifetime.set_started()
        service_lifetime.graceful_shutdown_scope = scope
        while not scope.cancel_called:
            print(f"{a_graceful_async_service.__name__} running")
            await anyio.sleep(1)
    print(f"{a_graceful_async_service.__name__} gracefully shut down")


@host.service()
async def an_anyio_func(*, task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
    print(f"{an_anyio_func.__name__} started")
    task_status.started()
    while True:
        print(f"{an_anyio_func.__name__} running")
        await anyio.sleep(1)


@host.service()
async def a_graceful_anyio_func(*, task_status: TaskStatus[CancelScope] = TASK_STATUS_IGNORED):
    print(f"{a_graceful_anyio_func.__name__} started")
    with CancelScope() as scope:
        task_status.started(scope)
        while not scope.cancel_called:
            print(f"{a_graceful_anyio_func.__name__} running")
            await anyio.sleep(1)
    print(f"{a_graceful_anyio_func.__name__} gracefully shut down")


async def an_async_service_tpl(n: int):
    print(f"Async Service {n} started")
    while True:
        print(f"Async Service {n} running")
        await anyio.sleep(1)


# host.service("first_service")(partial(an_async_service_tpl, 1))
# host.service("second_service")(partial(an_async_service_tpl, 2))


if __name__ == "__main__":
    from justscheduleit import hosting

    exit(hosting.run(host))
