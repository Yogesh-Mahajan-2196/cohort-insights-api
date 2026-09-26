import json
import logging

import redis.asyncio as redis

from core.config import settings


logger = logging.getLogger(__name__)

CACHE_PREFIX = "document_cache:"
CACHE_TTL = settings.REDIS_CACHE_TTL


def get_cache_key(content_hash: str) -> str:
    return f"{CACHE_PREFIX}{content_hash}"


async def get_cached_result(
    redis_client: redis.Redis,
    content_hash: str,
):
    key = get_cache_key(content_hash)

    try:
        cached = await redis_client.get(key)

        if not cached:
            return None

        result = json.loads(cached)

        if not isinstance(result, dict):
            return None

        if not isinstance(result.get("summary"), str):
            return None

        if not isinstance(result.get("tags", []), list):
            return None

        return result

    except (redis.RedisError, json.JSONDecodeError) as exc:
        logger.warning(
            "Redis cache read failed for key=%s: %s",
            key,
            exc,
        )
        return None


async def set_cached_result(
    redis_client: redis.Redis,
    content_hash: str,
    result: dict,
    ttl: int = CACHE_TTL,
) -> bool:
    key = get_cache_key(content_hash)

    try:
        await redis_client.set(
            key,
            json.dumps(result),
            ex=ttl,
        )
        return True

    except redis.RedisError as exc:
        logger.warning(
            "Redis cache write failed for key=%s: %s",
            key,
            exc,
        )
        return False
