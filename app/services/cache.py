import json
import logging

import redis.asyncio as redis


logger = logging.getLogger(__name__)

CACHE_PREFIX = "document_cache:"
CACHE_TTL = 3600


def get_cache_key(content_hash: str) -> str:
    return f"{CACHE_PREFIX}{content_hash}"


async def get_cached_result(
    redis_client: redis.Redis,
    content_hash: str,
):
    """
    Get a completed document result from Redis cache.

    Returns:
        dict | None
    """

    key = get_cache_key(content_hash)

    try:
        cached = await redis_client.get(key)

        if not cached:
            return None

        return json.loads(cached)

    except (redis.RedisError, json.JSONDecodeError) as exc:
        # Cache failure should not break document processing.
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
    """
    Store a completed document result in Redis.

    Returns:
        True  -> cache stored successfully
        False -> cache operation failed
    """

    key = get_cache_key(content_hash)

    try:
        await redis_client.set(
            key,
            json.dumps(result),
            ex=ttl,
        )

        return True

    except redis.RedisError as exc:
        # Cache failure should not make a successfully processed
        # document fail.
        logger.warning(
            "Redis cache write failed for key=%s: %s",
            key,
            exc,
        )

        return False