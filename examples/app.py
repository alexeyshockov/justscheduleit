#!/usr/bin/env python

import logging
import random
from datetime import timedelta

import justscheduleit
from justscheduleit import Scheduler, after, every, recurrent

logging.basicConfig()

logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

scheduler = Scheduler()


@scheduler.task(every(timedelta(seconds=3), jitter=(0, 10)))
async def task1():
    """
    A simple repeating task that returns a random number.
    """
    print("task1 running!")
    return random.randint(1, 22)


@scheduler.task(after(task1, jitter=None))
async def task2(r: str):
    """
    A task that runs after task1 and prints its result.
    """
    print(f"task2 here! task1 result: {r}")


@scheduler.task(recurrent(jitter=None))
async def task3():
    """
    A more complex repeating task, which period is dynamically changed.
    """
    print("task3 here!")
    return timedelta(seconds=random.randint(3, 9))


if __name__ == "__main__":
    justscheduleit.run(scheduler)
