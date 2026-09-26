import json

from core.config import settings


JOB_INFO_PREFIX = "job:"
ACTIVE_JOB_PREFIX = "active_jobs:"

MAX_ACTIVE_JOBS = settings.MAX_ACTIVE_JOBS_PER_USER
ACTIVE_JOB_TTL = 3600


# =========================================================
# RESERVE ACTIVE JOB
# =========================================================

RESERVE_SCRIPT = """
local key = KEYS[1]

local document_id = ARGV[1]
local version = ARGV[2]
local limit = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

-- Same document already owns a slot.
-- Just update its version.
local existing = redis.call(
    "HGET",
    key,
    document_id
)

if existing then

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
end

-- Count active jobs.
local count = redis.call(
    "HLEN",
    key
)

-- User has reached the limit.
if count >= limit then
    return 0
end

-- Reserve new slot.
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


# =========================================================
# RELEASE ACTIVE JOB
# =========================================================

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

-- Do not allow an old version to release
-- a newer version's slot.
if existing ~= version then
    return 0
end

redis.call(
    "HDEL",
    key,
    document_id
)

if redis.call("HLEN", key) == 0 then
    redis.call(
        "DEL",
        key
    )
end

return 1
"""


# =========================================================
# RESERVE
# =========================================================

async def reserve_job(
    redis_client,
    user_id: str,
    document_id: str,
    version: int,
) -> bool:

    key = (
        f"{ACTIVE_JOB_PREFIX}"
        f"{user_id}"
    )

    result = await redis_client.eval(
        RESERVE_SCRIPT,
        1,
        key,
        document_id,
        str(version),
        MAX_ACTIVE_JOBS,
        ACTIVE_JOB_TTL,
    )

    reserved = bool(result)

    print(
        f"[LIMIT] reserve_job "
        f"user={user_id} "
        f"document={document_id} "
        f"version={version} "
        f"reserved={reserved}"
    )

    return reserved


# =========================================================
# RELEASE
# =========================================================

async def release_job(
    redis_client,
    user_id: str,
    document_id: str,
    version: int,
) -> bool:

    key = (
        f"{ACTIVE_JOB_PREFIX}"
        f"{user_id}"
    )

    result = await redis_client.eval(
        RELEASE_SCRIPT,
        1,
        key,
        document_id,
        str(version),
    )

    released = bool(result)

    print(
        f"[LIMIT] release_job "
        f"user={user_id} "
        f"document={document_id} "
        f"version={version} "
        f"released={released}"
    )

    return released


# =========================================================
# UPDATE ACTIVE VERSION
# =========================================================

async def update_active_job_version(
    redis_client,
    user_id: str,
    document_id: str,
    version: int,
):
    key = (
        f"{ACTIVE_JOB_PREFIX}"
        f"{user_id}"
    )

    await redis_client.hset(
        key,
        document_id,
        str(version),
    )

    await redis_client.expire(
        key,
        ACTIVE_JOB_TTL,
    )


# =========================================================
# JOB INFO
# =========================================================

async def set_job_info(
    redis_client,
    user_id: str,
    client_doc_ref: str | None,
    document_id: str,
    version: int,
    status: str,
):
    """
    Keep human-readable job information in Redis.

    If client_doc_ref exists:
        job:user-001:doc-001

    Otherwise:
        job:user-001:<document_id>
    """

    reference = (
        client_doc_ref
        if client_doc_ref
        else document_id
    )

    key = (
        f"{JOB_INFO_PREFIX}"
        f"{user_id}:"
        f"{reference}"
    )

    data = {
        "user_id": user_id,
        "client_doc_ref": client_doc_ref,
        "document_id": document_id,
        "version": version,
        "status": status,
    }

    await redis_client.set(
        key,
        json.dumps(data),
        ex=ACTIVE_JOB_TTL,
    )

    print(
        f"[JOB] {key} -> {data}"
    )


async def update_job_info(
    redis_client,
    user_id: str,
    client_doc_ref: str | None,
    document_id: str,
    version: int,
    status: str,
):
    await set_job_info(
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
    reference = (
        client_doc_ref
        if client_doc_ref
        else document_id
    )

    if not reference:
        return

    key = (
        f"{JOB_INFO_PREFIX}"
        f"{user_id}:"
        f"{reference}"
    )

    await redis_client.delete(key)