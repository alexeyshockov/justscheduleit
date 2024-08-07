from __future__ import annotations

import dataclasses as dc
from typing import AsyncIterator

from croniter import croniter

from justscheduleit._utils import DC_CONFIG, random_jitter, sleep
from justscheduleit.cond import logger  # noqa
from justscheduleit.scheduler import SchedulerLifetime

__all__ = ["cron"]


# noinspection PyPep8Naming
@dc.dataclass(frozen=True, **DC_CONFIG)  # TODO Check repr()
class cron:
    """
    Triggers according to the cron schedule, with a random `jitter`.
    """

    schedule: str | croniter
    jitter: tuple[int, int] | None = (1, 10)

    async def __call__(self, _: SchedulerLifetime) -> AsyncIterator[None]:
        schedule = self.schedule if isinstance(self.schedule, croniter) else croniter(self.schedule)
        schedule_expr = " ".join(schedule.expressions)

        iter_n = 1
        while True:
            cur = schedule.cur
            iter_delay = schedule.get_next() - cur

            iter_jitter = random_jitter(self.jitter)
            iter_total_delay = iter_delay + iter_jitter.total_seconds()
            logger.debug(
                "(cron %s, iter %s) Sleeping for %s (until next: %s, jitter: %s)",
                schedule_expr,
                iter_n,
                iter_total_delay,
                iter_delay,
                iter_jitter,
            )
            await sleep(iter_total_delay)
            yield
            iter_n += 1
