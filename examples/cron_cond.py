import logging

from justscheduleit import Scheduler
from justscheduleit.cron_cond import cron

logging.basicConfig()

logging.getLogger("justscheduleit").setLevel(logging.DEBUG)

scheduler = Scheduler()


# Run every 5 minutes, with a random jitter
@scheduler.task(cron("*/5 * * * *", jitter=(0, 10)))
async def cron_job():
    print("cron task is triggered!")


if __name__ == "__main__":
    scheduler.run()
