from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import FastAPI  # noqa

from justscheduleit import Scheduler, every

scheduler = Scheduler()


@scheduler.task(every(timedelta(seconds=3), jitter=(1, 5)))
async def heavy_background_task():
    print("Some work here")


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with scheduler.aserve():
        yield


# Or Starlette
app = FastAPI(lifespan=lifespan)


@app.get("/predict")
async def predict():
    return {"result": "some prediction"}


if __name__ == "__main__":
    import uvicorn
    from uvicorn.config import LOGGING_CONFIG

    log_config = LOGGING_CONFIG
    log_config["loggers"]["justscheduleit"] = {"level": "DEBUG", "handlers": ["default"]}

    uvicorn.run(app, log_config=log_config)
