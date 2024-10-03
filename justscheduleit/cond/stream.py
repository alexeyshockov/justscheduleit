from __future__ import annotations

import dataclasses as dc
from collections.abc import AsyncGenerator
from typing import Any, Generic, TypeVar

from anyio.streams.memory import MemoryObjectReceiveStream

from justscheduleit.cond._core import logger
from justscheduleit.scheduler import SchedulerLifetime

__all__ = ["for_each"]

T = TypeVar("T")  # Task result type


@dc.dataclass(frozen=True, slots=True)
class ForEach(Generic[T]):
    _reader: MemoryObjectReceiveStream[T]
    stop_on_error: bool = False

    def __repr__(self):
        return f"stream.for_each({self._reader}, stop_on_error={self.stop_on_error})"

    async def __call__(self, _: SchedulerLifetime) -> AsyncGenerator[T, Any]:
        async with self._reader as stream:
            async for item in stream:
                try:
                    yield item
                except Exception:  # noqa
                    logger.exception("Error during task execution")
                    if self.stop_on_error:
                        raise


def for_each(stream: MemoryObjectReceiveStream[T], /, *, stop_on_error: bool = False) -> ForEach[T]:
    """
    Trigger an event for each item received from the stream.

    :param stream: The stream to read from.
    :param stop_on_error: Whether to stop the whole execution flow on error (or just log it and continue).
    """
    return ForEach(stream, stop_on_error=stop_on_error)
