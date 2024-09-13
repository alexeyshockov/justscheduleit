#!/usr/bin/env python

import time

from examples.host.failing_service import host
from justscheduleit.hosting import ServiceLifetime


@host.service(main=True)
def a_main_sync_service(service_lifetime: ServiceLifetime):
    print(f"{a_main_sync_service.__name__} started")
    service_lifetime.set_started()
    crash_countdown = 5
    while not service_lifetime.shutting_down.is_set():
        print(f"{a_main_sync_service.__name__} running")
        time.sleep(1)
        crash_countdown -= 1
        if crash_countdown == 0:
            raise RuntimeError("This is a test error from _sync_")
    print(f"{a_main_sync_service.__name__} stopped")


if __name__ == "__main__":
    from justscheduleit import hosting

    exit(hosting.run(host))
