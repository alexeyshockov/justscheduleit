from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from croniter import croniter

from justscheduleit.cond.cron import cron

# This is the same as using the @pytest.mark.anyio on all test functions in the module
pytestmark = pytest.mark.anyio


async def test_cron_trigger():
    base = datetime(2022, 1, 1)
    schedule = croniter("*/5 * * * *", base)
    trigger_factory = cron(schedule, delay=3)

    # https://docs.python.org/3/library/unittest.mock.html#where-to-patch
    # https://pytest-mock.readthedocs.io/en/latest/usage.html#where-to-patch
    with patch("anyio.sleep", new=AsyncMock()) as mock_sleep:
        scheduler_lifetime = MagicMock()
        async_iter = trigger_factory(scheduler_lifetime).__aiter__()
        await async_iter.__anext__()

        mock_sleep.assert_called_once_with(303.0)
