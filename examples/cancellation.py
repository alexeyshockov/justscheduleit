#!/usr/bin/env python

import logging
from asyncio import CancelledError
from datetime import timedelta

import anyio

from justscheduleit import Scheduler, every

logging.basicConfig()
logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

scheduler = Scheduler()


@scheduler.task(every(timedelta(seconds=5), delay=None))
async def long_async_task():
    print(f"{long_async_task.__name__} is triggered!")
    try:
        await anyio.sleep(15)
    except CancelledError:
        # Won't be cancelled in the normal flow (graceful shutdown)
        print(f"Forced shutdown!")
        raise
    print(f"{long_async_task.__name__} has finished!")


if __name__ == "__main__":
    import justscheduleit

    justscheduleit.run(scheduler)
