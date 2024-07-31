from datetime import timedelta, datetime
from unittest.mock import patch, MagicMock, AsyncMock

import pytest
from croniter import croniter

from justscheduleit.cond import every
from justscheduleit.cron_cond import cron


@pytest.mark.asyncio
async def test_cron_trigger():
    base = datetime(2022, 1, 1)
    schedule = croniter("*/5 * * * *", base)
    jitter = (1, 10)
    test_cron = cron(schedule=schedule, jitter=jitter)

    with patch("anyio.sleep", new=AsyncMock()) as mock_sleep:
        # https://docs.python.org/3/library/unittest.mock.html#where-to-patch
        # https://pytest-mock.readthedocs.io/en/latest/usage.html#where-to-patch
        with patch("justscheduleit.cron_cond.random_jitter", return_value=timedelta(seconds=5)) as mock_jitter:
            async_iter = test_cron(None, MagicMock()).__aiter__()
            await async_iter.__anext__()

            mock_jitter.assert_called_with(jitter)
            mock_sleep.assert_called_once_with(305.0)


@pytest.mark.asyncio
async def test_every_trigger():
    test_every = every(timedelta(seconds=5), jitter=None)

    with patch("anyio.sleep", new=AsyncMock()) as mock_sleep:
        async_iter = test_every(None, MagicMock()).__aiter__()
        await async_iter.__anext__()

        mock_sleep.assert_called_once_with(5.0)
