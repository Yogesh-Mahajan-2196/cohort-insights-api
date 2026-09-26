import json

from core.config import settings


JOB_INFO_PREFIX = "job:"
ACTIVE_JOB_PREFIX = "active_jobs:"

MAX_ACTIVE_JOBS = settings.MAX_ACTIVE_JOBS_PER_USER
ACTIVE_JOB_TTL = 3600


# return values:
# 0 = limit reached
# 1 = new reservation created
# 2 = this document already owns a reservation
# 3 = a newer version already owns the reservation
RESERVE_SCRIPT = """
local key = KEYS[1]
local document_id = ARGV[1]
local version = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local existing = redis.call("HGET", key, document_id)

if existing then
    local existing_version = tonumber(existing)

    if existing_version > version then
        return 3
    end

    redis.call("HSET", key, document_id, tostring(version))
    redis.call("EXPIRE", key, ttl)
    return 2
end

local count = redis.call("HLEN", key)

if count >= limit then
    return 0
end

redis.call("HSET", key, document_id, tostring(version))
redis.call("EXPIRE", key, ttl)

return 1
"""


RELEASE_SCRIPT = """
local key = KEYS[1]
local document_id = ARGV[1]
local version = tonumber(ARGV[2])

local existing = redis.call("HGET", key, document_id)

if not existing then
    return 0
end

if tonumber(existing) ~= version then
    return 0
end

redis.call("HDEL", key, document_id)

if redis.call("HLEN", key) == 0 then
    redis.call("DEL", key)
end

return 1
"""


# Update only when Redis still contains the version we expect.
# This prevents an older concurrent update from downgrading the slot.
UPDATE_VERSION_SCRIPT = """
local key = KEYS[1]
local document_id = ARGV[1]
local old_version = tonumber(ARGV[2])
local new_version = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local existing = redis.call("HGET", key, document_id)

if not existing then
    return 0
end

if tonumber(existing) ~= old_version then
    return 0
end

redis.call("HSET", key, document_id, tostring(new_version))
redis.call("EXPIRE", key, ttl)

return 1
"""


# Store job information as a Redis hash. The version guard prevents
# an old worker from changing a newer job back to processing/enriching.
JOB_INFO_SCRIPT = """
local key = KEYS[1]
local version = tonumber(ARGV[1])
local current = redis.call("HGET", key, "version")

if current and tonumber(current) > version then
    return 0
end

redis.call("HSET", key,
    "user_id", ARGV[2],
    "client_doc_ref", ARGV[3],
    "document_id", ARGV[4],
    "version", tostring(version),
    "status", ARGV[5]
)

redis.call("EXPIRE", key, tonumber(ARGV[6]))
return 1
"""


async def reserve_job(
    redis_client,
    user_id: str,
    document_id: str,
    version: int,
) -> int:
    key = f"{ACTIVE_JOB_PREFIX}{user_id}"

    result = await redis_client.eval(
        RESERVE_SCRIPT,
        1,
        key,
        document_id,
        str(version),
        str(MAX_ACTIVE_JOBS),
        str(ACTIVE_JOB_TTL),
    )

    result = int(result)

    print(
        f"[LIMIT] reserve user={user_id} "
        f"document={document_id} version={version} "
        f"result={result}"
    )

    return result


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

    released = bool(result)

    print(
        f"[LIMIT] release user={user_id} "
        f"document={document_id} version={version} "
        f"released={released}"
    )

    return released


async def update_active_job_version(
    redis_client,
    user_id: str,
    document_id: str,
    old_version: int,
    new_version: int,
) -> bool:
    key = f"{ACTIVE_JOB_PREFIX}{user_id}"

    result = await redis_client.eval(
        UPDATE_VERSION_SCRIPT,
        1,
        key,
        document_id,
        str(old_version),
        str(new_version),
        str(ACTIVE_JOB_TTL),
    )

    return bool(result)


async def set_job_info(
    redis_client,
    user_id: str,
    client_doc_ref: str | None,
    document_id: str,
    version: int,
    status: str,
):
    reference = client_doc_ref or document_id
    key = f"{JOB_INFO_PREFIX}{user_id}:{reference}"

    result = await redis_client.eval(
        JOB_INFO_SCRIPT,
        1,
        key,
        str(version),
        user_id,
        client_doc_ref or "",
        document_id,
        status,
        str(ACTIVE_JOB_TTL),
    )

    print(
        f"[JOB] {key} status={status} version={version} "
        f"updated={bool(result)}"
    )

    return bool(result)


async def update_job_info(
    redis_client,
    user_id: str,
    client_doc_ref: str | None,
    document_id: str,
    version: int,
    status: str,
):
    return await set_job_info(
        redis_client=redis_client,
        user_id=user_id,
        client_doc_ref=client_doc_ref,
        document_id=document_id,
        version=version,
        status=status,
    )


async def delete_job_info(
    redis_client,
    user_id: str,
    client_doc_ref: str | None,
    document_id: str | None = None,
):
    reference = client_doc_ref or document_id
    if not reference:
        return

    key = f"{JOB_INFO_PREFIX}{user_id}:{reference}"
    await redis_client.delete(key)
