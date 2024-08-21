#!/usr/bin/env python

import logging
import random
from datetime import timedelta

import justscheduleit
from justscheduleit import Scheduler, every
from justscheduleit.cond import take_first

logging.basicConfig()

logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

scheduler = Scheduler()


@scheduler.task(take_first(3, every(timedelta(seconds=3), delay=(0, 3))))
async def task1():
    """
    A simple repeating task that returns a random number.
    """
    print("task1 running!")
    return random.randint(1, 22)


if __name__ == "__main__":
    justscheduleit.run(scheduler)
