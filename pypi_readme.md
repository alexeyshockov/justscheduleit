# JustScheduleIt

Simple in-process task scheduler for Python apps.

Use it if:

- you need to schedule background tasks in the same process, like to update a shared dataframe every hour
  from S3

Take something else if:

- you need to schedule persistent/distributed tasks, that should be executed in a separate process (take a look at
  Celery)

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
