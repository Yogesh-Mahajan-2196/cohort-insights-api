ACTIVE_JOB_PREFIX = "active_jobs:"
MAX_ACTIVE_JOBS = 3
ACTIVE_JOB_TTL = 3600


RESERVE_SCRIPT = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local maximum = tonumber(ARGV[1])

if current >= maximum then
    return 0
end

local new_value = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])

return new_value
"""


RELEASE_SCRIPT = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')

if current <= 1 then
    redis.call('DEL', KEYS[1])
    return 0
end

return redis.call('DECR', KEYS[1])
"""


async def reserve_job(redis, user_id: str) -> bool:
    key = f"{ACTIVE_JOB_PREFIX}{user_id}"

    result = await redis.eval(
        RESERVE_SCRIPT,
        1,
        key,
        MAX_ACTIVE_JOBS,
        ACTIVE_JOB_TTL,
    )

    return int(result) > 0


async def release_job(redis, user_id: str):
    key = f"{ACTIVE_JOB_PREFIX}{user_id}"

    await redis.eval(
        RELEASE_SCRIPT,
        1,
        key,
    )


async def get_active_jobs(redis, user_id: str) -> int:
    key = f"{ACTIVE_JOB_PREFIX}{user_id}"

    value = await redis.get(key)

    if value is None:
        return 0

    return int(value)