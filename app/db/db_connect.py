from contextlib import asynccontextmanager

import redis.asyncio as redis
from pymongo import AsyncMongoClient

from core.config import settings
from db.indexes import create_indexes
from workers.queue import create_consumer_group


@asynccontextmanager
async def lifespan(app):

    mongo_client = AsyncMongoClient(
        settings.MONGO_URI
    )

    db = mongo_client[
        settings.MONGO_DATABASE
    ]

    redis_client = redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
    )

    try:

        await db.command("ping")
        await redis_client.ping()

        await create_indexes(db)

        await create_consumer_group(
            redis_client
        )

        app.state.mongo_client = mongo_client
        app.state.db = db
        app.state.redis = redis_client

        print("MongoDB connected")
        print("Redis connected")

        yield

    finally:

        await redis_client.aclose()

        mongo_client.close()