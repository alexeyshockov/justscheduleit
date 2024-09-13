#!/usr/bin/env python

import logging
import time

import anyio

from justscheduleit.hosting import Host, ServiceLifetime

logging.basicConfig()
logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

host = Host()


@host.service()
def a_sync_service(service_lifetime: ServiceLifetime):
    print(f"{a_sync_service.__name__} started")
    service_lifetime.set_started()
    # DON'T do infinite loops in a sync service function, as there is no way to interrupt it from the host.
    # Always check the service_lifetime.shutting_down event to see if the service should stop.
    while not service_lifetime.shutting_down.is_set():
        print(f"{a_sync_service.__name__} running")
        time.sleep(1)
        raise RuntimeError("This is a test error from _sync_")
    print(f"{a_sync_service.__name__} stopped")


@host.service()
async def an_async_func():
    print(f"{an_async_func.__name__} started")
    try:
        while True:
            print(f"{an_async_func.__name__} running")
            await anyio.sleep(1)
            # raise RuntimeError("This is a test error from _async_")
    finally:
        print(f"{an_async_func.__name__} done")


if __name__ == "__main__":
    from justscheduleit import hosting

    exit(hosting.run(host))
