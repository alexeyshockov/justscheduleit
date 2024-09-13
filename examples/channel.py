#!/usr/bin/env python

import logging

import anyio

from justscheduleit import Scheduler, every
from justscheduleit.cond import take_first
from justscheduleit.hosting import Host

logging.basicConfig()
logging.getLogger("justscheduleit").setLevel(logging.DEBUG)


channel_writer, channel_reader = anyio.create_memory_object_stream[str]()

scheduler = Scheduler()

host = Host()
host.add_service(scheduler, name="scheduler")


# Does not hold the host alive, if the scheduler is done
@host.service(daemon=True)
async def print_channel():
    """
    A hosted service, to read the channel in the background.
    """
    async with channel_reader as channel:
        async for message in channel:
            print(f"Message received: {message}")


@scheduler.task(take_first(3, every("3s", delay=(1, 3))))
async def scheduled_background_task():
    print("Scheduled work here, writing to the channel...")
    await channel_writer.send("hello from the background task!")


if __name__ == "__main__":
    from justscheduleit import hosting

    exit(hosting.run(host))
