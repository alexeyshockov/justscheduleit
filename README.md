# JustScheduleIt

[![PyPI package](https://img.shields.io/pypi/v/JustScheduleIt?label=JustScheduleIt)](https://pypi.org/project/JustScheduleIt/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/JustScheduleIt)](https://pypi.org/project/JustScheduleIt/)

Simple in-process task scheduler for Python apps.

Use it if:

- you need to schedule background tasks in the same process, like to update a shared dataframe every hour
  from S3

Take something else if:

- you need to schedule persistent/distributed tasks, that should be executed in a separate process (take a look at
  Celery)

## Installation

```shell
$ pip install justscheduleit
```

## Usage

### Just schedule a task

```python
from datetime import timedelta

from justscheduleit import Scheduler, every, run

scheduler = Scheduler()


@scheduler.task(every(timedelta(minutes=1), delay=(0, 10)))
def task():
    print("Hello, world!")


run(scheduler)
```

### `sync` and `async` tasks

The scheduler supports both `sync` and `async` functions. A `sync` function will be executed in a separate thread,
using [
`anyio.to_thread.run_sync()`](https://anyio.readthedocs.io/en/stable/threads.html#running-a-function-in-a-worker-thread),
so it won't block the scheduler (other tasks).

### (Advanced) Hosting

Scheduler is built around Host abstraction. A host is a supervisor that runs 1 or more services, usually as the
application entry point.

A scheduler itself is a hosted service. The default `justscheduleit.run()` internally just creates a host with one
service, the passed scheduler, and runs it.

## Alternatives

There are a lot of alternatives, but most of them are either too complex (like Rocketry), does not support async (like
schedule), or just abandoned.

- https://pypi.org/project/simple-scheduler/
- https://pypi.org/project/scheduler/
- APScheduler
    - https://github.com/tarsil/asyncz as a slim version
- [schedule](https://github.com/dbader/schedule) — does not support async tasks
- https://github.com/aio-libs/aiojobs
- https://pypi.org/project/aio-recurring/
- https://github.com/quantmind/aio-fluid/tree/main/fluid/scheduler#tasks

And some (probably) abandoned ones:

- Rocketry
- https://pypi.org/project/recurring/
- https://pypi.org/project/aio-scheduler/
- https://pypi.org/project/async-sched/ (also a very strange one, with a remote interface...)
- https://github.com/adriangb/asgi-background

## Contributing

### Naming convention

- Classes for async-only use cases
    - `async def serve()` returns an async context manager
    - `async def execute()` "blocks" the execution flow (until everything is done)
- Classes for both sync- and async- use cases
    - `def serve()` returns a context manager
    - `async def aserve()` returns an async context manager
    - `async def aexecute()` "blocks" the execution flow (until everything is done)
