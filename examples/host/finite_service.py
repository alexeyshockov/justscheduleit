#!/usr/bin/env python

import logging
import time

from justscheduleit.hosting import Host, ServiceLifetime

logging.basicConfig()

logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

host = Host()


@host.service()
def a_sync_service(service_lifetime: ServiceLifetime):
    print("Service started")
    service_lifetime.set_started()
    print("Service running")
    time.sleep(10)
    print("Service is done")
    # The host should also stop after this point, as all the services have stopped.


if __name__ == "__main__":
    from justscheduleit import hosting

    hosting.run(host)
