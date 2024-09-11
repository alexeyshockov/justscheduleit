#!/usr/bin/env python

import logging
from datetime import timedelta

import anyio
from fastapi import FastAPI  # noqa

from justscheduleit import Scheduler, every
from justscheduleit.hosting import Host
from justscheduleit.http import UvicornService

logging.basicConfig()

logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

host = Host()


@host.service()
async def background_queue_consumer():
    print(f"{background_queue_consumer.__name__} started")
    try:
        while True:
            print(f"{background_queue_consumer.__name__} running")
            await anyio.sleep(1)
    finally:
        print(f"{background_queue_consumer.__name__} done")


scheduler = Scheduler()
host.add_service(scheduler, name="scheduler")


@scheduler.task(every(timedelta(seconds=3), delay=(1, 5)))
async def heavy_background_task():
    print("Some work here")


# Or Starlette
http_api = FastAPI()
host.add_service(UvicornService.for_app(http_api), name="http_api")


@http_api.get("/predict")
async def predict():
    return {"result": "some prediction"}


@http_api.get("/health")
async def health_check():
    services = host.lifetime.service_lifetimes
    statuses = {name: service.status for name, service in services.items()}
    return {"status": "UP", "services": statuses}


if __name__ == "__main__":
    from justscheduleit import hosting

    hosting.run(host)
