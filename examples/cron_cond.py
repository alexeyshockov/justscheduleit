#!/usr/bin/env python

import logging

from justscheduleit import Scheduler
from justscheduleit.cond.cron import cron

logging.basicConfig()

logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

scheduler = Scheduler()


# Runs every 5 minutes, with a random delay (jitter)
@scheduler.task(cron("*/5 * * * *", delay=(0, 10)))
async def cron_job():
    print("cron task is triggered!")


if __name__ == "__main__":
    import justscheduleit

    justscheduleit.run(scheduler)
