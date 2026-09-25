import redis.asyncio as redis


ACTIVE_JOB_PREFIX = "active_jobs:"
MAX_ACTIVE_JOBS = 3
ACTIVE_JOB_TTL = 3600


RESERVE_SCRIPT = """
local key = KEYS[1]
local document_id = ARGV[1]
local version = ARGV[2]
local limit = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local existing = redis.call("HGET", key, document_id)

if existing then
    redis.call("HSET", key, document_id, version)
    redis.call("EXPIRE", key, ttl)
    return 1
end

local count = redis.call("HLEN", key)

if count >= limit then
    return 0
end

redis.call(
    "HSET",
    key,
    document_id,
    version
)

redis.call(
    "EXPIRE",
    key,
    ttl
)

return 1
"""


RELEASE_SCRIPT = """
local key = KEYS[1]
local document_id = ARGV[1]
local version = ARGV[2]

local existing = redis.call(
    "HGET",
    key,
    document_id
)

if not existing then
    return 0
end

if existing ~= version then
    return 0
end

redis.call(
    "HDEL",
    key,
    document_id
)

if redis.call("HLEN", key) == 0 then
    redis.call("DEL", key)
end

return 1
"""


async def reserve_job(
    redis_client,
    user_id: str,
    document_id: str,
    version: int,
) -> bool:

    key = f"{ACTIVE_JOB_PREFIX}{user_id}"

    result = await redis_client.eval(
        RESERVE_SCRIPT,
        1,
        key,
        document_id,
        str(version),
        MAX_ACTIVE_JOBS,
        ACTIVE_JOB_TTL,
    )

    return bool(result)


async def release_job(
    redis_client,
    user_id: str,
    document_id: str,
    version: int,
) -> bool:

    key = f"{ACTIVE_JOB_PREFIX}{user_id}"

    result = await redis_client.eval(
        RELEASE_SCRIPT,
        1,
        key,
        document_id,
        str(version),
    )

    return bool(result)


async def update_active_job_version(
    redis_client,
    user_id: str,
    document_id: str,
    version: int,
):
    key = f"{ACTIVE_JOB_PREFIX}{user_id}"

    await redis_client.hset(
        key,
        document_id,
        str(version),
    )

    await redis_client.expire(
        key,
        ACTIVE_JOB_TTL,
    )