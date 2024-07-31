import time

from examples.app import scheduler


def main():
    # Scheduler in a _separate_ thread, _separate_ event loop
    with scheduler.serve() as scheduler_exec:  # noqa
        for _ in range(10):
            time.sleep(1)
            print("Main thread is running...")
        print("Main thread is done, exiting the app")


if __name__ == "__main__":
    main()
