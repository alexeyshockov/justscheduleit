from __future__ import annotations

import random
import signal
import sys
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import Any, Callable, Iterable, TypedDict, TypeVar, Protocol

import anyio
from anyio import create_task_group, Event
from anyio.abc import TaskGroup
from typing_extensions import NotRequired

T = TypeVar("T")

DC_CONFIG: dict[str, bool] = {}
if sys.version_info >= (3, 10):
    DC_CONFIG["slots"] = True

TD_ZERO = timedelta(0)

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


def first(collection: Iterable[T]) -> T:
    return next(iter(collection))


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

    async def wait(self) -> None:
        ...

    def is_set(self) -> bool:
        ...


def observe_event(tg: TaskGroup, source: Event | EventView, target: Callable) -> None:
    async def wait_and_set() -> None:
        await source.wait()
        target()

    tg.start_soon(wait_and_set)  # type: ignore


# class CancellableTaskGroup(AbstractAsyncContextManager[TaskGroup]):
#     _event: anyio.Event
#     _task_group: TaskGroup
#
#     def __init__(self, event: anyio.Event):
#         self._event = event
#         self._task_group = create_task_group()
#
#     async def _monitor(self) -> None:
#         await self._event.wait()
#         self._task_group.cancel_scope.cancel()
#
#     async def __aenter__(self) -> TaskGroup:
#         await self._task_group.__aenter__()
#         # noinspection PyTypeChecker
#         self._task_group.start_soon(self._monitor)
#         return self._task_group
#
#     async def __aexit__(self, __exc_type, __exc_value, __traceback) -> bool | None:
#         return await self._task_group.__aexit__(__exc_type, __exc_value, __traceback)
