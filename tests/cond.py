from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from justscheduleit import every

# This is the same as using the @pytest.mark.anyio on all test functions in the module
pytestmark = pytest.mark.anyio


async def test_every_trigger():
    trigger_factory = every(timedelta(seconds=5), delay=None)

    with patch("anyio.sleep", new=AsyncMock()) as mock_sleep:
        scheduler_lifetime = MagicMock()
        async for _ in trigger_factory(scheduler_lifetime):
            mock_sleep.assert_called_once_with(5.0)
            break
