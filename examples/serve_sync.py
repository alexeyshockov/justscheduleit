#!/usr/bin/env python

import time

from examples.app import scheduler


def main():
    # Scheduler in a _separate_ thread, _separate_ event loop
    with scheduler.serve() as scheduler_exec:
        try:
            while True:
                time.sleep(1)
                print("Main thread is running...")
        except KeyboardInterrupt:
            scheduler_exec.shutdown()
        finally:
            print("Main thread is done, exiting the app")


def main_h():
    # Scheduler in a _separate_ thread, _separate_ event loop
    with scheduler.serve() as scheduler_exec:
        try:
            while True:
                time.sleep(1)
                print("Main thread is running...")
        except KeyboardInterrupt:
            scheduler_exec.shutdown()
        finally:
            print("Main thread is done, exiting the app")


if __name__ == "__main__":
    main()
