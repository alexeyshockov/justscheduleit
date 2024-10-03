#!/usr/bin/env python

import logging
from collections.abc import AsyncIterator
from typing import Sequence, TypeVar

import anyio
from anyio import move_on_after
from anyio.streams.memory import MemoryObjectReceiveStream

from justscheduleit import Scheduler, every
from justscheduleit.cond import take_first
from justscheduleit.hosting import Host

T = TypeVar("T")

logging.basicConfig()
logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

channel_writer, channel_reader = anyio.create_memory_object_stream[str]()

scheduler = Scheduler()

host = Host()
host.add_service(scheduler, name="scheduler")


async def read_batch(channel: MemoryObjectReceiveStream[T], max_size: int, window_duration: float) -> Sequence[T]:
    batch = []
    with move_on_after(window_duration):
        while len(batch) < max_size:
            message = await channel.receive()
            batch.append(message)
    return batch


async def read_batches(
    channel: MemoryObjectReceiveStream[T], max_size: int, window_duration: float
) -> AsyncIterator[Sequence[T]]:
    from anyio import ClosedResourceError, EndOfStream

    while True:
        try:
            batch = await read_batch(channel, max_size, window_duration)
            if batch:
                yield batch
        except (EndOfStream, ClosedResourceError):
            break


@host.service(daemon=True)
async def print_channel_batch():
    """
    A hosted service, to read the channel in the background.
    """
    async with channel_reader as channel:
        async for batch in read_batches(channel, 3, 5):
            print(f"Received batch: {batch}")


@scheduler.task(take_first(100, every("1s", delay=None)))
async def scheduled_background_task():
    print("Scheduled work here, writing to the channel...")
    await channel_writer.send("hello from the background task!")


if __name__ == "__main__":
    from justscheduleit import hosting

    exit(hosting.run(host))
