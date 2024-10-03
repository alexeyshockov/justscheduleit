#!/usr/bin/env python

import logging
from collections.abc import Sequence

import anyio

from justscheduleit import Scheduler, every
from justscheduleit.cond import batch, stream, take_first
from justscheduleit.hosting import Host

logging.basicConfig()
logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

channel_writer, channel_reader = anyio.create_memory_object_stream[str]()

scheduler = Scheduler()

host = Host()
host.add_service(scheduler, name="scheduler")


@scheduler.task(batch(30, 50, stream.for_each(channel_reader)))
async def print_channel_batch(items: Sequence[str]):
    print(f"Batch received: {items}")


@scheduler.task(take_first(100, every("1s", delay=None)))
async def scheduled_background_task():
    print("Scheduled work here, writing to the channel...")
    await channel_writer.send("hello from the background task!")


if __name__ == "__main__":
    from justscheduleit import hosting

    exit(hosting.run(host))
