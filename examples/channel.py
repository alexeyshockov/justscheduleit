#!/usr/bin/env python

import logging

import anyio

from justscheduleit import Scheduler, every
from justscheduleit.hosting import Host

logging.basicConfig()

logging.getLogger("justscheduleit").setLevel(logging.DEBUG)


channel_writer, channel_reader = anyio.create_memory_object_stream[str]()

host = Host()
scheduler = host.services["scheduler"] = Scheduler()


@host.service()
async def print_channel():
    """
    A hosted service, to read the channel in the background.
    """
    async with channel_reader as channel:
        async for message in channel:
            print(f"Message received: {message}")


@scheduler.task(every("3s", delay=(1, 5)))
async def scheduled_background_task():
    print("Scheduled work here, writing to the channel...")
    await channel_writer.send("hello from the background task!")


if __name__ == "__main__":
    from justscheduleit import hosting

    hosting.run(host)
