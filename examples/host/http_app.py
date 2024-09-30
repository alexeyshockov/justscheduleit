#!/usr/bin/env python

import logging
from datetime import timedelta

import anyio
from fastapi import FastAPI
from starlette.responses import JSONResponse

from justscheduleit import Scheduler, every
from justscheduleit.hosting import Host
from justscheduleit.http import UvicornService

logging.basicConfig()
logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

host = Host()

http_api = FastAPI()
host.add_service(UvicornService.for_app(http_api), name="http_api")

scheduler = Scheduler()
host.add_service(scheduler, name="scheduler")


@host.service()
async def background_queue_consumer():
    print(f"{background_queue_consumer.__name__} started")
    try:
        while True:
            print(f"{background_queue_consumer.__name__} running")
            await anyio.sleep(1)
    finally:
        print(f"{background_queue_consumer.__name__} done")


@scheduler.task(every(timedelta(seconds=3), delay=(1, 5)))
async def heavy_background_task():
    print("Some work here")


@http_api.get("/predict")
async def predict():
    return {"result": "some prediction"}


@http_api.get("/health")
async def health_check() -> JSONResponse:
    return JSONResponse(host.lifetime.status)


if __name__ == "__main__":
    from justscheduleit import hosting

    exit(hosting.run(host))
