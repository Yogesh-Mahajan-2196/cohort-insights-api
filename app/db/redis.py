import redis.asyncio as redis
from core.config import settings
from fastapi import Request

redis_client = redis.from_url(
    settings.REDIS_URL,
    decode_responses=True,
    socket_timeout=None,
)

def get_redis(request: Request):
    return request.app.state.redis

