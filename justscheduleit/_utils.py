from __future__ import annotations

import random
import sys
from datetime import timedelta
from typing import Any, Callable, Iterable, TypedDict, TypeVar

import anyio
from typing_extensions import NotRequired

T = TypeVar("T")

DC_CONFIG: dict[str, bool] = {}
if sys.version_info >= (3, 10):
    DC_CONFIG["slots"] = True

TD_ZERO = timedelta(0)


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
