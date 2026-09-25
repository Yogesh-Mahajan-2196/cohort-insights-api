import asyncio
import random

import redis.asyncio as redis

from bson import ObjectId
from pymongo import AsyncMongoClient

from core.config import settings
from services.cache import set_cached_result
from services.limits import release_job
from workers.queue import (
    STREAM_NAME,
    GROUP_NAME,
)


async def process_stage(
    db,
    document_id: str,
    version: int,
):
    object_id = ObjectId(document_id)

    # ---------------------------------------------------------
    # Atomically claim the current queued version.
    # ---------------------------------------------------------
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
            }
        },
    )

    if result.modified_count == 0:
        print(
            f"[{document_id}] "
            f"Version {version} is stale or already processed."
        )

        return "stale"

    document = await db.documents.find_one(
        {
            "_id": object_id,
            "content_version": version,
        }
    )

    if not document:
        return "stale"

    print(
        f"[{document_id}] "
        f"Stage 1 started for version {version}"
    )

    sleep_time = random.uniform(10, 20)

    await asyncio.sleep(sleep_time)

    # ---------------------------------------------------------
    # Simulated 10% failure
    # ---------------------------------------------------------
    if random.random() < 0.10:

        result = await db.documents.update_one(
            {
                "_id": object_id,
                "content_version": version,
                "processing.content_version": version,
                "processing.status": "processing",
            },
            {
                "$set": {
                    "status": "failed",
                    "failed_stage": "processing",
                    "processing.status": "failed",
                    "processing.content_version": version,
                }
            },
        )

        if result.modified_count:
            print(
                f"[{document_id}] "
                f"Stage 1 failed for version {version}"
            )

            return "failed"

        return "stale"

    # ---------------------------------------------------------
    # Generate mock summary
    # ---------------------------------------------------------
    summary = (
        "Mock summary for document: "
        f"{document['content'][:200]}"
    )

    # ---------------------------------------------------------
    # Save only if version is still current
    # ---------------------------------------------------------
    result = await db.documents.update_one(
        {
            "_id": object_id,
            "content_version": version,
            "processing.content_version": version,
            "processing.status": "processing",
        },
        {
            "$set": {
                "processing.status": "completed",
                "processing.summary": summary,
                "processing.content_version": version,

                "status": "enriching",

                "updated_at": __import__(
                    "datetime"
                ).datetime.now(
                    __import__(
                        "datetime"
                    ).timezone.utc
                ),
            }
        },
    )

    if result.modified_count == 0:
        print(
            f"[{document_id}] "
            f"Stage 1 result ignored. "
            f"Version {version} is stale."
        )

        return "stale"

    print(
        f"[{document_id}] "
        f"Stage 1 completed for version {version}"
    )

    return "success"


async def enrich_stage(
    db,
    redis_client,
    document_id: str,
    version: int,
):
    object_id = ObjectId(document_id)

    # ---------------------------------------------------------
    # Read only the current version
    # ---------------------------------------------------------
    document = await db.documents.find_one(
        {
            "_id": object_id,
            "content_version": version,
            "processing.status": "completed",
            "processing.content_version": version,
        }
    )

    if not document:
        print(
            f"[{document_id}] "
            f"Stage 2 skipped. "
            f"Version {version} is stale."
        )

        return "stale"

    print(
        f"[{document_id}] "
        f"Stage 2 started for version {version}"
    )

    sleep_time = random.uniform(5, 15)

    await asyncio.sleep(sleep_time)

    # ---------------------------------------------------------
    # Simulated 10% failure
    # ---------------------------------------------------------
    if random.random() < 0.10:

        result = await db.documents.update_one(
            {
                "_id": object_id,
                "content_version": version,
                "processing.status": "completed",
                "processing.content_version": version,
            },
            {
                "$set": {
                    "status": "failed",
                    "failed_stage": "enriching",
                    "enriching.status": "failed",
                    "enriching.content_version": version,
                }
            },
        )

        if result.modified_count:
            print(
                f"[{document_id}] "
                f"Stage 2 failed for version {version}"
            )

            return "failed"

        return "stale"

    # ---------------------------------------------------------
    # Generate mock tags based on stage-1 summary/content
    # ---------------------------------------------------------
    text = (
        document["processing"]["summary"]
        + " "
        + document["content"]
    ).lower()

    possible_tags = [
        "python",
        "fastapi",
        "backend",
        "redis",
        "mongodb",
        "api",
    ]

    tags = [
        tag
        for tag in possible_tags
        if tag in text
    ]

    if not tags:
        tags = [
            "backend",
            "api",
        ]

    # ---------------------------------------------------------
    # Atomically complete stage 2
    # ---------------------------------------------------------
    result = await db.documents.update_one(
        {
            "_id": object_id,
            "content_version": version,
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

                "updated_at": __import__(
                    "datetime"
                ).datetime.now(
                    __import__(
                        "datetime"
                    ).timezone.utc
                ),
            }
        },
    )

    if result.modified_count == 0:
        print(
            f"[{document_id}] "
            f"Stage 2 result ignored. "
            f"Version {version} is stale."
        )

        return "stale"

    # ---------------------------------------------------------
    # ONLY cache after successful Stage 2
    # ---------------------------------------------------------
    await set_cached_result(
        redis_client,
        document["content_hash"],
        {
            "summary": document["processing"]["summary"],
            "tags": tags,
        },
    )

    print(
        f"[{document_id}] "
        f"Stage 2 completed for version {version}"
    )

    return "success"


async def process_document(
    db,
    redis_client,
    document_id: str,
    version: int,
):
    result = await process_stage(
        db,
        document_id,
        version,
    )

    # ---------------------------------------------------------
    # Stale job:
    # NEVER release active slot.
    #
    # The newer version owns that slot.
    # ---------------------------------------------------------
    if result == "stale":
        return

    # ---------------------------------------------------------
    # Stage 1 failed.
    # Current version owns slot and is now terminal.
    # ---------------------------------------------------------
    if result == "failed":

        document = await db.documents.find_one(
            {
                "_id": ObjectId(document_id),
                "content_version": version,
                "status": "failed",
            }
        )

        if document:
            await release_job(
                redis_client,
                document["user_id"],
            )

        return

    # ---------------------------------------------------------
    # Stage 2
    # ---------------------------------------------------------
    result = await enrich_stage(
        db,
        redis_client,
        document_id,
        version,
    )

    if result == "stale":
        return

    # ---------------------------------------------------------
    # Release only if THIS version reached terminal state.
    # ---------------------------------------------------------
    document = await db.documents.find_one(
        {
            "_id": ObjectId(document_id),
            "content_version": version,
            "status": {
                "$in": [
                    "completed",
                    "failed",
                ]
            },
        }
    )

    if document:
        await release_job(
            redis_client,
            document["user_id"],
        )


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

    consumer_name = (
        f"worker-{random.randint(1000, 9999)}"
    )

    print(
        f"Worker started: {consumer_name}"
    )

    while True:

        messages = await redis_client.xreadgroup(
            groupname=GROUP_NAME,
            consumername=consumer_name,
            streams={
                STREAM_NAME: ">"
            },
            count=1,
            block=1000,
        )

        if not messages:
            continue

        for stream_name, stream_messages in messages:

            for message_id, data in stream_messages:

                document_id = data[
                    "document_id"
                ]

                version = int(
                    data["content_version"]
                )

                print(
                    f"Received job {message_id} "
                    f"for document {document_id} "
                    f"version {version}"
                )

                try:

                    await process_document(
                        db,
                        redis_client,
                        document_id,
                        version,
                    )

                    await redis_client.xack(
                        STREAM_NAME,
                        GROUP_NAME,
                        message_id,
                    )

                except Exception as exc:

                    print(
                        f"Worker error for "
                        f"{document_id}: {exc}"
                    )


if __name__ == "__main__":
    asyncio.run(worker())