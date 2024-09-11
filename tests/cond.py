from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from croniter import croniter

from justscheduleit import every
from justscheduleit.cond.cron import cron


@pytest.mark.asyncio
async def test_cron_trigger():
    base = datetime(2022, 1, 1)
    schedule = croniter("*/5 * * * *", base)
    jitter = (1, 10)
    trigger_factory = cron(schedule=schedule, jitter=jitter)

    with patch("anyio.sleep", new=AsyncMock()) as mock_sleep:
        # https://docs.python.org/3/library/unittest.mock.html#where-to-patch
        # https://pytest-mock.readthedocs.io/en/latest/usage.html#where-to-patch
        with patch("justscheduleit.cron_cond.random_jitter", return_value=timedelta(seconds=5)) as mock_jitter:
            async_iter = trigger_factory(MagicMock()).__aiter__()
            await async_iter.__anext__()

            mock_jitter.assert_called_with(jitter)
            mock_sleep.assert_called_once_with(305.0)


@pytest.mark.asyncio
async def test_every_trigger():
    trigger_factory = every(timedelta(seconds=5), delay=None)

    with patch("anyio.sleep", new=AsyncMock()) as mock_sleep:
        # async_iter = trigger_factory(MagicMock()).__aiter__()
        # await async_iter.__anext__()
        #
        # mock_sleep.assert_called_once_with(5.0)
        for _ in trigger_factory(MagicMock()):
            mock_sleep.assert_called_once_with(5.0)
            break
