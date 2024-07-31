import logging
from datetime import timedelta

import anyio
from anyio import CancelScope

from justscheduleit import Scheduler, every

logging.basicConfig()

logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

scheduler = Scheduler()


@scheduler.task(every(timedelta(seconds=5), jitter=None))
async def graceful_long_async_task(cancel_scope: CancelScope):
    print(f"{graceful_long_async_task} is triggered!")
    with cancel_scope:
        await anyio.sleep(15)
    print(f"{graceful_long_async_task} has finished!")


@scheduler.task(every(timedelta(seconds=5), jitter=None))
async def long_async_task():
    print(f"{long_async_task} is triggered!")
    await anyio.sleep(15)
    print(f"{long_async_task} has finished!")


async def main():
    async with scheduler.aserve():
        duration = 5
        print(f"Main thread is running for {duration} seconds...")
        await anyio.sleep(duration)
        print("Main thread is done, exiting the app")


if __name__ == "__main__":
    # noinspection PyTypeChecker
    anyio.run(main)
