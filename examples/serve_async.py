import anyio

from examples.app import scheduler


async def main():
    # Scheduler in the same thread, same event loop
    async with scheduler.aserve() as scheduler_exec:  # noqa
        for _ in range(10):
            await anyio.sleep(1)
            print("Main thread is running...")
        print("Main thread is done, exiting the app")


if __name__ == "__main__":
    # noinspection PyTypeChecker
    anyio.run(main)
