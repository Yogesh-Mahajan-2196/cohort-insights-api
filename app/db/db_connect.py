from contextlib import asynccontextmanager
from db.indexes import create_indexes
from core.config import settings
from pymongo import AsyncMongoClient
import redis.asyncio as redis
from fastapi import FastAPI
from workers.queue import create_consumer_group


@asynccontextmanager
async def lifespan(app: FastAPI):

    # MongoDB Connection
    mongo_client = AsyncMongoClient(settings.MONGO_URI)

    # Redis Connection
    redis_client = redis.from_url(
        settings.REDIS_URL,
        decode_responses = True
    )

    # Strpre connections inside FastAPI
    app.state.mongo_client = mongo_client
    app.state.db = mongo_client[settings.MONGO_DATABASE]
    app.state.redis = redis_client

    # Test Mongo Connection
    await mongo_client.admin.command("ping")

    # Test Redis Connection
    await redis_client.ping()

    # Create MongoDB indexes
    await create_indexes(app.state.db)

    await create_consumer_group(redis_client)

    print("Mongo connected")
    print("Redis connected")
    print("MongoDB indexes created")
    print("Redis consumer group ready")
    
    yield

    await redis_client.aclose()
    await mongo_client.close()

    print("Connection closed")