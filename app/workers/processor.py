import asyncio
import logging
import random
from datetime import datetime, timezone
import time
import redis.asyncio as redis
from bson import ObjectId
from pymongo import AsyncMongoClient

from core.config import settings
from services.cache import set_cached_result
from services.limits import (
    release_job,
    update_job_info,
    delete_job_info,
)
from workers.queue import (
    GROUP_NAME,
    STREAM_NAME,
    create_consumer_group,
)
from workers.text_extractor import extract_tags

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger("workers.processor")

# =========================================================
# CONFIG
# =========================================================

STAGE_2_MAX_RETRIES = 2
STAGE_2_BASE_BACKOFF = 1

# Redis message is considered abandoned after 60 seconds.
REDIS_MESSAGE_MIN_IDLE_MS = 180_000

# =========================================================
# HELPERS
# =========================================================

def utc_now():
    return datetime.now(timezone.utc)


def get_object_id(document_id: str) -> ObjectId:
    if not ObjectId.is_valid(document_id):
        raise ValueError(
            f"Invalid document_id: {document_id}"
        )

    return ObjectId(document_id)


# =========================================================
# STAGE 1 FAILURE
# =========================================================

async def mark_processing_failed(
    db,
    document_id: str,
    version: int,
) -> bool:
    """
    Fail Stage 1 only if THIS exact version is still
    processing.

    Returns True when the current version was actually
    changed to failed.
    """

    result = await db.documents.update_one(
        {
            "_id": get_object_id(document_id),
            "content_version": version,

            "status": "processing",

            "processing.status": "processing",
            "processing.content_version": version,
        },
        {
            "$set": {
                "status": "failed",
                "failed_stage": "processing",

                "processing.status": "failed",
                "processing.content_version": version,

                "updated_at": utc_now(),
            }
        },
    )

    return result.modified_count == 1


# =========================================================
# STAGE 2 FAILURE
# =========================================================

async def mark_enriching_failed(
    db,
    document_id: str,
    version: int,
) -> bool:
    """
    Fail Stage 2 only if THIS exact version is still
    in the enriching state.
    """

    result = await db.documents.update_one(
        {
            "_id": get_object_id(document_id),
            "content_version": version,

            "status": "enriching",

            "processing.status": "completed",
            "processing.content_version": version,
        },
        {
            "$set": {
                "status": "failed",
                "failed_stage": "enriching",

                "enriching.status": "failed",
                "enriching.content_version": version,

                "updated_at": utc_now(),
            }
        },
    )

    return result.modified_count == 1


# =========================================================
# STAGE 1
# =========================================================

async def process_stage(
    db,
    redis_client,
    document_id: str,
    version: int,
):
    """
    Stage 1 executes exactly once for a Redis job.

    Returns:

        "success"
        "failed"
        "stale"
    """

    object_id = get_object_id(
        document_id
    )

    # =====================================================
    # 1. Atomically claim this exact version
    # =====================================================

    result = await db.documents.update_one(
        {
            "_id": object_id,
            "content_version": version,
            "status": "queued",
        },
        {
            "$set": {
                "status": "processing",

                "processing.status": "processing",
                "processing.content_version": version,

                "updated_at": utc_now(),
            }
        },
    )

    # -----------------------------------------------------
    # Another worker already claimed this version
    # -----------------------------------------------------

    if result.modified_count != 1:

        logger.info(
            "[%s] Stage 1 stale/already claimed "
            "for version %s",
            document_id,
            version,
        )

        return "stale"

    # =====================================================
    # 2. Read the document
    # =====================================================

    document = await db.documents.find_one(
        {
            "_id": object_id,
            "content_version": version,

            "status": "processing",

            "processing.status": "processing",
            "processing.content_version": version,
        }
    )

    # -----------------------------------------------------
    # Document disappeared or became stale
    # -----------------------------------------------------

    if not document:

        logger.info(
            "[%s] Stage 1 document disappeared "
            "or became stale. Version %s",
            document_id,
            version,
        )

        return "stale"

    # =====================================================
    # 3. Update Redis job status
    #
    # IMPORTANT:
    # document is available now.
    # =====================================================

    await update_job_info(
        redis_client=redis_client,

        user_id=document["user_id"],

        client_doc_ref=document.get(
            "client_doc_ref"
        ),

        document_id=document_id,

        version=version,

        status="processing",
    )

    logger.info(
        "[%s] Stage 1 started. Version %s",
        document_id,
        version,
    )

    # =====================================================
    # 4. Simulate Stage 1 processing
    # =====================================================

    await asyncio.sleep(
        random.uniform(10, 20)
    )

    # =====================================================
    # 5. Simulated failure
    # =====================================================

    # TEMPORARILY DISABLED WHILE TESTING.
    #
    # Once everything works, you can change this back to:
    #
    # if random.random() < 0.10:

    if False:

        failed = await mark_processing_failed(
            db=db,
            document_id=document_id,
            version=version,
        )

        if failed:

            logger.warning(
                "[%s] Stage 1 failed. Version %s",
                document_id,
                version,
            )

            return "failed"

        return "stale"

    # =====================================================
    # 6. Generate summary
    # =====================================================

    summary = (
        f"Processed document "
        f"'{document['title']}'. "
        f"Content summary: "
        f"{document['content'][:200]}"
    )

    # =====================================================
    # 7. Complete Stage 1
    #
    # Only update if this exact version is still
    # processing.
    # =====================================================

    result = await db.documents.update_one(
        {
            "_id": object_id,
            "content_version": version,

            "status": "processing",

            "processing.status": "processing",
            "processing.content_version": version,
        },
        {
            "$set": {
                "processing.status": "completed",

                "processing.summary": summary,

                "processing.content_version": version,

                "status": "enriching",

                "updated_at": utc_now(),
            }
        },
    )

    # -----------------------------------------------------
    # Version became stale while Stage 1 was running
    # -----------------------------------------------------

    if result.modified_count != 1:

        logger.info(
            "[%s] Stage 1 result discarded. "
            "Version %s became stale.",
            document_id,
            version,
        )

        return "stale"

    # =====================================================
    # 8. Update Redis status
    #
    # processing -> enriching
    # =====================================================

    await update_job_info(
        redis_client=redis_client,

        user_id=document["user_id"],

        client_doc_ref=document.get(
            "client_doc_ref"
        ),

        document_id=document_id,

        version=version,

        status="enriching",
    )

    logger.info(
        "[%s] Stage 1 completed. Version %s",
        document_id,
        version,
    )

    return "success"


# =========================================================
# STAGE 2 - ONE ATTEMPT
# =========================================================

async def enrich_stage(
    db,
    document_id: str,
    version: int,
    redis_client,
):
    """
    Execute ONE Stage 2 attempt.

    Returns:

        "success"
        "failed"
        "stale"
    """

    object_id = get_object_id(
        document_id
    )

    # -----------------------------------------------------
    # Stage 2 can only start after Stage 1 completed for
    # exactly this version.
    # -----------------------------------------------------

    document = await db.documents.find_one(
        {
            "_id": object_id,
            "content_version": version,

            "status": "enriching",

            "processing.status": "completed",
            "processing.content_version": version,
        }
    )

    if not document:

        logger.info(
            "[%s] Stage 2 stale. Version %s",
            document_id,
            version,
        )

        return "stale"

    logger.info(
        "[%s] Stage 2 attempt started. Version %s",
        document_id,
        version,
    )

    # -----------------------------------------------------
    # Simulate enrichment.
    # -----------------------------------------------------

    await asyncio.sleep(
        random.uniform(5, 15)
    )

    # -----------------------------------------------------
    # Simulated 10% failure.
    # -----------------------------------------------------

    if random.random() < 0.10:

        logger.warning(
            "[%s] Stage 2 attempt failed. Version %s",
            document_id,
            version,
        )

        return "failed"

    # -----------------------------------------------------
    # Generate tags.
    # -----------------------------------------------------

    summary = document[
        "processing"
    ]["summary"]


    tags = extract_tags(
        content=document["content"],
        summary=summary,
    )

    if not tags:
        tags = []

    # -----------------------------------------------------
    # Complete Stage 2 atomically.
    # -----------------------------------------------------

    result = await db.documents.update_one(
        {
            "_id": object_id,
            "content_version": version,

            "status": "enriching",

            "processing.status": "completed",
            "processing.content_version": version,
        },
        {
            "$set": {
                "enriching.status": "completed",
                "enriching.tags": tags,
                "enriching.content_version": version,

                "status": "completed",
                "failed_stage": None,

                "updated_at": utc_now(),
            }
        },
    )

    if result.modified_count != 1:

        logger.info(
            "[%s] Stage 2 result discarded. "
            "Version %s became stale.",
            document_id,
            version,
        )

        return "stale"

    # -----------------------------------------------------
    # Cache only after successful Stage 2.
    #
    # Cache failure MUST NOT make the Mongo document fail.
    # -----------------------------------------------------

    try:

        await set_cached_result(
            redis_client=redis_client,
            content_hash=document["content_hash"],
            result={
                "summary": summary,
                "tags": tags,
            },
        )

    except Exception:

        logger.exception(
            "[%s] Redis cache write failed "
            "for version %s",
            document_id,
            version,
        )

    logger.info(
        "[%s] Stage 2 completed. Version %s",
        document_id,
        version,
    )

    return "success"


# =========================================================
# RELEASE JOB SAFELY
# =========================================================

async def release_terminal_job(
    db,
    redis_client,
    document_id: str,
    version: int,
):
    """
    Release the active slot only when the SAME version is
    currently terminal.

    This prevents an old worker from releasing a newer
    version's active slot.
    """

    document = await db.documents.find_one(
        {
            "_id": get_object_id(document_id),

            "content_version": version,

            "status": {
                "$in": [
                    "completed",
                    "failed",
                ]
            },
        },
        {
            "user_id": 1,
            "content_version": 1,
            "status": 1,
            "client_doc_ref": 1,
        },
    )

    if not document:

        logger.info(
            "[%s] Slot not released. "
            "Version %s is not terminal/current.",
            document_id,
            version,
        )

        return False

    await release_job(
        redis_client,
        document["user_id"],
        document_id,
        version,
    )

    await update_job_info(
        redis_client=redis_client,
        user_id=document["user_id"],
        client_doc_ref=document.get(
            "client_doc_ref"
        ),
        document_id=document_id,
        version=version,
        status=document["status"],
    )

    logger.info(
        "[%s] Released active slot. Version %s",
        document_id,
        version,
    )

    return True


# =========================================================
# PROCESS ONE DOCUMENT
# =========================================================

async def process_document(
    db,
    redis_client,
    document_id: str,
    version: int,
):
    """
    Stage 1:
        one attempt only

    Stage 2:
        retry independently

    This means a temporary Stage 2 failure does NOT rerun
    the expensive Stage 1 operation.
    """

    # =====================================================
    # STAGE 1
    # =====================================================

    stage1_result = await process_stage(
        db=db,
        redis_client=redis_client,
        document_id=document_id,
        version=version,
    )

    if stage1_result == "stale":

        # IMPORTANT:
        # Do not release anything.
        #
        # A newer version may own the active slot.
        return "stale"

    if stage1_result == "failed":

        await release_terminal_job(
            db=db,
            redis_client=redis_client,
            document_id=document_id,
            version=version,
        )

        return "failed"

    # =====================================================
    # STAGE 2 RETRIES
    # =====================================================

    for attempt in range(
        STAGE_2_MAX_RETRIES + 1
    ):

        stage2_result = await enrich_stage(
            db=db,
            document_id=document_id,
            version=version,
            redis_client=redis_client,
        )

        # -------------------------------------------------
        # Success
        # -------------------------------------------------

        if stage2_result == "success":

            await release_terminal_job(
                db=db,
                redis_client=redis_client,
                document_id=document_id,
                version=version,
            )

            return "success"

        # -------------------------------------------------
        # Stale
        # -------------------------------------------------

        if stage2_result == "stale":

            # Never release a slot here.
            #
            # Another version may own it.
            return "stale"

        # -------------------------------------------------
        # Stage 2 failed
        # -------------------------------------------------

        if (
            attempt
            >= STAGE_2_MAX_RETRIES
        ):

            failed = (
                await mark_enriching_failed(
                    db=db,
                    document_id=document_id,
                    version=version,
                )
            )

            if failed:

                await release_terminal_job(
                    db=db,
                    redis_client=redis_client,
                    document_id=document_id,
                    version=version,
                )

                return "failed"

            # Document became stale between the attempt
            # and failure update.
            return "stale"

        # -------------------------------------------------
        # Exponential backoff
        #
        # attempt 0 -> 1 sec
        # attempt 1 -> 2 sec
        # -------------------------------------------------

        backoff = (
            STAGE_2_BASE_BACKOFF
            * (2 ** attempt)
        )

        logger.info(
            "[%s] Retrying Stage 2 in %s seconds. "
            "Retry %s/%s",
            document_id,
            backoff,
            attempt + 1,
            STAGE_2_MAX_RETRIES,
        )

        await asyncio.sleep(
            backoff
        )

    return "failed"


# =========================================================
# PARSE REDIS MESSAGE
# =========================================================

def parse_message(data: dict):
    document_id = data.get(
        "document_id"
    )

    raw_version = data.get(
        "content_version"
    )

    if not document_id:
        raise ValueError(
            "Redis message missing document_id"
        )

    if raw_version is None:
        raise ValueError(
            "Redis message missing content_version"
        )

    if not ObjectId.is_valid(
        document_id
    ):
        raise ValueError(
            f"Invalid document_id: {document_id}"
        )

    try:
        version = int(
            raw_version
        )
    except (
        TypeError,
        ValueError,
    ) as exc:
        raise ValueError(
            f"Invalid content_version: "
            f"{raw_version}"
        ) from exc

    return (
        document_id,
        version,
    )


# =========================================================
# HANDLE ONE REDIS MESSAGE
# =========================================================

async def handle_message(
    db,
    redis_client,
    message_id,
    data,
):
    document_id, version = parse_message(
        data
    )

    logger.info(
        "Received message=%s document=%s version=%s",
        message_id,
        document_id,
        version,
    )

    result = await process_document(
        db=db,
        redis_client=redis_client,
        document_id=document_id,
        version=version,
    )

    # -----------------------------------------------------
    # Normal outcomes can be ACKed:
    #
    # success
    # failed
    # stale
    #
    # An unexpected exception does NOT reach here.
    # -----------------------------------------------------

    await redis_client.xack(
        STREAM_NAME,
        GROUP_NAME,
        message_id,
    )

    logger.info(
        "ACK message=%s document=%s "
        "version=%s result=%s",
        message_id,
        document_id,
        version,
        result,
    )

    return result


# =========================================================
# CLAIM ABANDONED REDIS MESSAGES
# =========================================================

async def claim_pending_messages(
    redis_client,
    consumer_name: str,
):
    """
    Recover messages abandoned by crashed workers.

    XAUTOCLAIM requires a Redis version supporting it.
    """

    claimed_messages = []

    start_id = "0-0"

    while True:

        result = await redis_client.xautoclaim(
            name=STREAM_NAME,
            groupname=GROUP_NAME,
            consumername=consumer_name,
            min_idle_time=REDIS_MESSAGE_MIN_IDLE_MS,
            start_id=start_id,
            count=10,
        )

        next_start_id = result[0]
        messages = result[1]

        claimed_messages.extend(
            messages
        )

        if (
            not messages
            or next_start_id == "0-0"
        ):
            break

        start_id = next_start_id

    return claimed_messages


# =========================================================
# WORKER
# =========================================================

async def worker():

    mongo_client = AsyncMongoClient(
        settings.MONGO_URI
    )

    db = mongo_client[
        settings.MONGO_DATABASE
    ]

    redis_client = redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_timeout=None,
    )

    await redis_client.ping()

    print(
        f"[REDIS] Worker connected to Redis: "
        f"{settings.REDIS_URL}"
    )

    await create_consumer_group(
        redis_client
    )

    consumer_name = (
        f"worker-{random.randint(1000, 999999)}"
    )

    logger.info(
        "Worker started: %s",
        consumer_name,
    )

    try:
        last_heartbeat = time.monotonic()
        while True:

            if time.monotonic() - last_heartbeat >= 10:
                logger.info(
                    "Worker alive: %s | waiting for jobs",
                    consumer_name,
                )
                last_heartbeat = time.monotonic()

            # =================================================
            # 1. Recover abandoned messages
            # =================================================

            try:

                pending_messages = (
                    await claim_pending_messages(
                        redis_client,
                        consumer_name,
                    )
                )

                for (
                    message_id,
                    data,
                ) in pending_messages:

                    try:

                        await handle_message(
                            db=db,
                            redis_client=redis_client,
                            message_id=message_id,
                            data=data,
                        )

                    except Exception:

                        # IMPORTANT:
                        #
                        # Do NOT ACK.
                        #
                        # The message remains pending and
                        # can be reclaimed later.
                        logger.exception(
                            "Failed processing "
                            "reclaimed message=%s",
                            message_id,
                        )

            except Exception:

                logger.exception(
                    "Failed to reclaim pending "
                    "Redis messages"
                )

            # =================================================
            # 2. Read new messages
            # =================================================

            try:

                messages = (
                    await redis_client.xreadgroup(
                        groupname=GROUP_NAME,
                        consumername=consumer_name,
                        streams={
                            STREAM_NAME: ">"
                        },
                        count=1,
                        block=1000,
                    )
                )

            except Exception:

                logger.exception(
                    "Redis XREADGROUP failed"
                )

                await asyncio.sleep(
                    2
                )

                continue

            if not messages:
                continue

            # =================================================
            # 3. Process messages
            # =================================================

            for (
                stream_name,
                stream_messages,
            ) in messages:

                for (
                    message_id,
                    data,
                ) in stream_messages:

                    try:

                        await handle_message(
                            db=db,
                            redis_client=redis_client,
                            message_id=message_id,
                            data=data,
                        )

                    except Exception:

                        # IMPORTANT:
                        #
                        # No XACK here.
                        #
                        # The message remains in the
                        # Pending Entries List.
                        logger.exception(
                            "Unhandled worker error. "
                            "Message %s remains pending.",
                            message_id,
                        )

    finally:

        logger.info(
            "Worker shutting down: %s",
            consumer_name,
        )

        await redis_client.aclose()
        await mongo_client.close()


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    asyncio.run(worker())
