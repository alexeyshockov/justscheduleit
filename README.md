# JustScheduleIt

Simple in-process task scheduler for Python.

Use it if:
- you need to schedule a background tasks in the same process, like to update a shared (Pandas) dataframe every hour from S3

Take something else if:
- you need to schedule persistent / distributed tasks (take a look at Celery)

## Installation

```shell
$ pip3 install justscheduleit
```

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

And some (probably) abandoned ones:

- Rocketry
- https://pypi.org/project/recurring/
- https://pypi.org/project/aio-scheduler/
- https://pypi.org/project/async-sched/ (also a very strange one, with a remote interface...)
- https://github.com/adriangb/asgi-background
