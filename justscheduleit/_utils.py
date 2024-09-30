from __future__ import annotations

import dataclasses as dc
import random
import signal
import sys
from contextlib import AbstractContextManager, nullcontext
from datetime import timedelta
from typing import Any, Callable, Protocol, TypedDict, TypeVar, Union, cast

import anyio
from anyio import Event
from anyio.abc import TaskGroup
from typing_extensions import NotRequired, Self

T = TypeVar("T")

TD_ZERO = timedelta(0)

NULL_CM: AbstractContextManager[None] = nullcontext()

HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)
if sys.platform == "win32":  # pragma: py-not-win32
    HANDLED_SIGNALS += (signal.SIGBREAK,)  # Windows signal 21. Sent by Ctrl+Break.


# TODO Rename (callable_id, task_id, something else?..)
def task_full_name(func: Callable) -> str:
    try:
        module = func.__module__
        name = func.__qualname__
    except AttributeError:  # A callable object (with a __call__ method) case
        func_t = type(func)
        module = func_t.__module__
        name = func_t.__qualname__
    if module is None or module == "__builtin__" or module == "__main__":
        return name
    return module + "." + name


def ensure_td(value: timedelta | str) -> timedelta:
    if isinstance(value, timedelta):
        return value
    elif isinstance(value, str):
        try:
            import pytimeparse2

            use_dateutil = pytimeparse2.HAS_RELITIVE_TIMEDELTA

            try:
                # Make sure to get timedelta, not relativedelta from dateutil
                pytimeparse2.HAS_RELITIVE_TIMEDELTA = False
                return cast(timedelta, pytimeparse2.parse(value, as_timedelta=True))
            finally:
                pytimeparse2.HAS_RELITIVE_TIMEDELTA = use_dateutil
        except ImportError:
            raise ValueError("pytimeparse2 package is required to parse a time period string") from None
    else:
        raise ValueError(f"Invalid time period: {value!r}")


def td_str(td: timedelta) -> str:
    try:
        from humanize import precisedelta

        # 23 seconds or 0.24 seconds
        return precisedelta(td)
    except ImportError:
        # 0:00:23 or 0:00:00.240000
        return str(td)


DelayFactory = Union[Callable[[], timedelta], tuple[int, int], tuple[float, float], int, float, timedelta, None]


@dc.dataclass(frozen=True, slots=True)
class RandomDelay:  # https://en.wikipedia.org/wiki/Jitter#Types
    bounds: tuple[int, int] | tuple[float, float]

    def __repr__(self):
        return f"{self.__class__.__name__}{self.bounds!r}"

    def __str__(self):
        return f"random{self.bounds!r}"

    def __call__(self) -> timedelta:
        a, b = self.bounds
        delay = random.randint(a, b) if isinstance(a, int) and isinstance(b, int) else random.uniform(a, b)
        return timedelta(seconds=delay)


@dc.dataclass(frozen=True, slots=True)
class FixedDelay:
    value: timedelta

    @classmethod
    def create(cls, value: int | float | timedelta | None) -> Self:
        if value is None or value == 0:
            delay = TD_ZERO
        elif isinstance(value, (int, float)):
            delay = timedelta(seconds=value)
        elif isinstance(value, timedelta):
            delay = value
        else:
            raise ValueError(f"Invalid delay: {value!r}")

        return cls(delay)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.value.total_seconds()})"

    def __str__(self):
        return str(self.value.total_seconds())

    def __call__(self) -> timedelta:
        return self.value


def ensure_delay_factory(delay: DelayFactory) -> Callable[[], timedelta]:
    if isinstance(delay, tuple):  # tuple[int, int] | tuple[float, float]
        return RandomDelay(delay)  # type: ignore
    elif callable(delay):
        return delay
    else:  # int | float | timedelta | None
        return FixedDelay.create(delay)


async def sleep(interval: timedelta | int | float | None) -> None:
    interval_sec = interval.total_seconds() if isinstance(interval, timedelta) else 0 if interval is None else interval
    if interval_sec < 0:
        # Raise a warning?..
        interval_sec = 0
    await anyio.sleep(interval_sec)


class AsyncBackendConfig(TypedDict):
    backend: str
    backend_options: NotRequired[dict[str, Any]]


def choose_anyio_backend() -> AsyncBackendConfig:  # pragma: no cover
    try:
        import uvloop  # noqa  # type: ignore
    except ImportError:
        return {"backend": "asyncio"}
    else:
        return {"backend": "asyncio", "backend_options": {"use_uvloop": True}}


class EventView(Protocol):
    """
    Read-only view on an async event.
    """

    async def wait(self) -> None: ...

    def is_set(self) -> bool: ...


def observe_event(tg: TaskGroup, source: Event | EventView, target: Callable) -> None:
    async def wait_and_set() -> None:
        await source.wait()
        target()

    tg.start_soon(wait_and_set)  # type: ignore
