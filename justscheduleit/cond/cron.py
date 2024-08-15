from __future__ import annotations

import dataclasses as dc
from collections.abc import AsyncGenerator
from typing import Any, final

from croniter import croniter

from justscheduleit._utils import random_jitter, sleep
from justscheduleit.cond._core import logger
from justscheduleit.scheduler import SchedulerLifetime

__all__ = ["cron"]


# noinspection PyPep8Naming
@final
@dc.dataclass(frozen=True)  # Enable kw_only when Python 3.10+
class cron:
    """
    Triggers according to the cron schedule, with a random `jitter`.
    """

    schedule: str | croniter
    jitter: tuple[int, int] | None = dc.field(default=(1, 10))  # Enable kw_only when Python 3.10+
    stop_on_error: bool = dc.field(default=False)  # Enable kw_only when Python 3.10+

    # TODO Check repr()

    async def __call__(self, _: SchedulerLifetime) -> AsyncGenerator[None, Any]:
        schedule = self.schedule if isinstance(self.schedule, croniter) else croniter(self.schedule)
        schedule_expr = " ".join(schedule.expressions)

        iter_n = 1
        while True:
            iter_n += 1
            cur = schedule.cur
            iter_delay = schedule.get_next() - cur  # get_next() mutates the iterator (schedule object)

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
            try:
                yield
            except Exception:  # noqa
                if self.stop_on_error:
                    raise
                logger.exception("Error during task execution")
