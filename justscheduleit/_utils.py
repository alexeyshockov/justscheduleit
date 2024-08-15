from __future__ import annotations

import random
import signal
import sys
from contextlib import AbstractContextManager, nullcontext
from datetime import timedelta
from typing import Any, Callable, Protocol, TypedDict, TypeVar

import anyio
from anyio import Event
from anyio.abc import TaskGroup
from typing_extensions import NotRequired

T = TypeVar("T")

TD_ZERO = timedelta(0)

NULL_CM: AbstractContextManager[None] = nullcontext()

HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)
if sys.platform == "win32":  # pragma: py-not-win32
    HANDLED_SIGNALS += (signal.SIGBREAK,)  # Windows signal 21. Sent by Ctrl+Break.


def task_full_name(func: Callable) -> str:  # Task ID?..
    module = func.__module__
    name = func.__qualname__
    if module is None or module == "__builtin__" or module == "__main__":
        return name
    return module + "." + name


def random_jitter(jitter: tuple[int, int] | None) -> timedelta:
    return TD_ZERO if jitter is None else timedelta(seconds=random.randint(*jitter))


async def sleep(period: timedelta | int | float | None) -> None:
    if period is None:
        return
    elif isinstance(period, timedelta):
        await anyio.sleep(period.total_seconds())
    else:
        await anyio.sleep(period)


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
