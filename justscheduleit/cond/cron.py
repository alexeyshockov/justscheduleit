from __future__ import annotations

import dataclasses as dc
from collections.abc import AsyncGenerator, Callable
from datetime import timedelta
from typing import Any

from croniter import croniter

from justscheduleit._utils import DelayFactory, ensure_delay_factory, sleep, td_str
from justscheduleit.cond._core import DEFAULT_JITTER, logger
from justscheduleit.scheduler import SchedulerLifetime

__all__ = ["cron"]


@dc.dataclass(frozen=True, slots=True)
class Cron:
    """
    Triggers according to the cron schedule, with an (optional) additional `delay` (jitter).
    """

    schedule: croniter
    delay: Callable[[], timedelta] = DEFAULT_JITTER
    stop_on_error: bool = False

    # TODO Check repr()

    async def __call__(self, _: SchedulerLifetime) -> AsyncGenerator[None, Any]:
        # Clone, to be reproducible?..
        schedule = self.schedule
        schedule_expr = " ".join(schedule.expressions)

        iter_n = 1
        while True:
            iter_n += 1
            cur = schedule.cur
            # get_next() mutates the iterator (schedule object)
            iter_interval = timedelta(seconds=schedule.get_next() - cur)
            iter_jitter = self.delay()
            iter_delay = iter_interval + iter_jitter
            logger.debug(
                "(cron %s, iter %s) Sleeping for %s (including %s delay)",
                schedule_expr,
                iter_n,
                td_str(iter_delay),
                td_str(iter_jitter),
            )
            await sleep(iter_delay)
            try:
                yield
            except Exception:  # noqa
                if self.stop_on_error:
                    raise
                logger.exception("Error during task execution")


def cron(schedule: str | croniter, /, *, delay: DelayFactory = DEFAULT_JITTER, stop_on_error: bool = False) -> Cron:
    return Cron(
        schedule if isinstance(schedule, croniter) else croniter(schedule), ensure_delay_factory(delay), stop_on_error
    )
