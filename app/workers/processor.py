import asyncio
import random

from bson import ObjectId
from pymongo import AsyncMongoClient
import redis.asyncio as redis

from core.config import settings
from workers.queue import (
    STREAM_NAME,
    GROUP_NAME,
)


CONSUMER_NAME = "worker-1"


async def process_stage(
    db,
    document_id: str,
    version: int,
):
    """
    Stage 1:
    processing
    """

    object_id = ObjectId(document_id)

    # -----------------------------------------
    # Mark processing
    # -----------------------------------------

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
            }
        },
    )

    if result.modified_count == 0:
        return False

    # -----------------------------------------
    # Simulate processing
    # -----------------------------------------

    delay = random.uniform(10, 20)

    print(
        f"[{document_id}] "
        f"Stage 1 started. "
        f"Sleeping {delay:.2f}s"
    )

    await asyncio.sleep(delay)

    # -----------------------------------------
    # Random failure ~10%
    # -----------------------------------------

    if random.random() < 0.10:

        await db.documents.update_one(
            {
                "_id": object_id,
                "content_version": version,
            },
            {
                "$set": {
                    "status": "failed",
                    "failed_stage": "processing",
                    "processing.status": "failed",
                }
            },
        )

        print(
            f"[{document_id}] "
            f"Stage 1 FAILED"
        )

        return False

    # -----------------------------------------
    # Generate mock summary
    # -----------------------------------------

    document = await db.documents.find_one(
        {
            "_id": object_id,
            "content_version": version,
        }
    )

    if not document:
        return False

    content = document["content"]

    summary = (
        f"Mock summary for document: "
        f"{content[:100]}"
    )

    # -----------------------------------------
    # Save Stage 1 result
    # -----------------------------------------

    result = await db.documents.update_one(
        {
            "_id": object_id,
            "content_version": version,
            "status": "processing",
        },
        {
            "$set": {
                "processing.status": "completed",
                "processing.summary": summary,
                "processing.content_version": version,
                "status": "enriching",
            }
        },
    )

    if result.modified_count == 0:
        return False

    print(
        f"[{document_id}] "
        f"Stage 1 completed"
    )

    return True


async def enrich_stage(
    db,
    document_id: str,
    version: int,
):
    """
    Stage 2:
    enriching
    """

    object_id = ObjectId(document_id)

    document = await db.documents.find_one(
        {
            "_id": object_id,
            "content_version": version,
            "processing.content_version": version,
            "processing.status": "completed",
        }
    )

    if not document:
        print(
            f"[{document_id}] "
            f"Stage 2 skipped - version changed"
        )
        return False

    # -----------------------------------------
    # Simulate enriching
    # -----------------------------------------

    delay = random.uniform(5, 15)

    print(
        f"[{document_id}] "
        f"Stage 2 started. "
        f"Sleeping {delay:.2f}s"
    )

    await asyncio.sleep(delay)

    # -----------------------------------------
    # Random failure ~10%
    # -----------------------------------------

    if random.random() < 0.10:

        await db.documents.update_one(
            {
                "_id": object_id,
                "content_version": version,
            },
            {
                "$set": {
                    "status": "failed",
                    "failed_stage": "enriching",
                    "enriching.status": "failed",
                }
            },
        )

        print(
            f"[{document_id}] "
            f"Stage 2 FAILED"
        )

        return False

    # -----------------------------------------
    # Generate mock tags
    # -----------------------------------------

    tags = [
        "python",
        "fastapi",
        "backend",
    ]

    # -----------------------------------------
    # Save Stage 2 result
    # -----------------------------------------

    result = await db.documents.update_one(
        {
            "_id": object_id,
            "content_version": version,
            "processing.content_version": version,
        },
        {
            "$set": {
                "enriching.status": "completed",
                "enriching.tags": tags,
                "enriching.content_version": version,
                "status": "completed",
                "failed_stage": None,
            }
        },
    )

    if result.modified_count == 0:
        return False

    print(
        f"[{document_id}] "
        f"Stage 2 completed"
    )

    return True


async def process_document(
    db,
    document_id: str,
    version: int,
):
    """
    Execute both stages.
    """

    success = await process_stage(
        db,
        document_id,
        version,
    )

    if not success:
        return

    await enrich_stage(
        db,
        document_id,
        version,
    )


async def worker():

    mongo_client = AsyncMongoClient(
        settings.MONGO_URI
    )

    redis_client = redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
    )

    db = mongo_client[
        settings.MONGO_DATABASE
    ]

    print("Worker started")

    while True:

        messages = await redis_client.xreadgroup(
            groupname=GROUP_NAME,
            consumername=CONSUMER_NAME,
            streams={
                STREAM_NAME: ">"
            },
            count=1,
            block=1000,
        )

        if not messages:
            continue

        for stream_name, entries in messages:

            for message_id, data in entries:

                document_id = data["document_id"]
                version = int(
                    data["content_version"]
                )

                print(
                    f"Received job "
                    f"{message_id} "
                    f"for document "
                    f"{document_id}"
                )

                try:

                    await process_document(
                        db=db,
                        document_id=document_id,
                        version=version,
                    )

                    await redis_client.xack(
                        STREAM_NAME,
                        GROUP_NAME,
                        message_id,
                    )

                except Exception as exc:

                    print(
                        f"Worker error: {exc}"
                    )


async def main():
    await worker()


if __name__ == "__main__":
    asyncio.run(main())