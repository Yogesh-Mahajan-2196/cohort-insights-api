import hashlib
from datetime import datetime, timezone

from bson import ObjectId
from fastapi import HTTPException

from models.documents import build_document
from services.cache import get_cached_result
from services.limits import reserve_job
from workers.queue import enqueue_document


def calculate_content_hash(content: str) -> str:
    return hashlib.sha256(
        content.encode("utf-8")
    ).hexdigest()


def build_cached_document(
    user_id: str,
    title: str,
    content: str,
    content_hash: str,
    cached_result: dict,
    client_doc_ref: str | None = None,
):
    document = build_document(
        user_id=user_id,
        title=title,
        content=content,
        content_hash=content_hash,
        client_doc_ref=client_doc_ref,
    )

    document["status"] = "completed"

    document["processing"] = {
        "status": "completed",
        "summary": cached_result["summary"],
        "content_version": 1,
    }

    document["enriching"] = {
        "status": "completed",
        "tags": cached_result["tags"],
        "content_version": 1,
    }

    return document


async def create_document(
    db,
    redis,
    user_id: str,
    title: str,
    content: str,
    client_doc_ref: str | None = None,
):
    # ---------------------------------------------------------
    # 1. Calculate content hash
    # ---------------------------------------------------------
    content_hash = calculate_content_hash(content)

    # ---------------------------------------------------------
    # 2. Check client_doc_ref FIRST
    # ---------------------------------------------------------
    if client_doc_ref:
        existing = await db.documents.find_one(
            {
                "client_doc_ref": client_doc_ref,
            }
        )

        if existing:
            # Same ref + same content = idempotent retry
            if existing["content_hash"] == content_hash:
                return existing

            # Same ref + different content = conflict
            raise HTTPException(
                status_code=409,
                detail=(
                    "client_doc_ref already exists "
                    "with different content"
                ),
            )

    # ---------------------------------------------------------
    # 3. Redis content cache
    # ---------------------------------------------------------
    cached = await get_cached_result(
        redis,
        content_hash,
    )

    if cached:
        document = build_cached_document(
            user_id=user_id,
            title=title,
            content=content,
            content_hash=content_hash,
            cached_result=cached,
            client_doc_ref=client_doc_ref,
        )

        result = await db.documents.insert_one(document)
        document["_id"] = result.inserted_id

        return document

    # ---------------------------------------------------------
    # 4. MongoDB cache fallback
    # ---------------------------------------------------------
    cached_document = await db.documents.find_one(
        {
            "content_hash": content_hash,
            "status": "completed",
            "processing.status": "completed",
            "enriching.status": "completed",
            "processing.content_version": {
                "$eq": "$content_version"
            },
        }
    )

    # The expression above is not suitable for normal Mongo
    # equality matching, so we perform a safer application-level
    # verification below.
    if cached_document:
        processing = cached_document.get("processing", {})
        enriching = cached_document.get("enriching", {})

        if (
            processing.get("content_version")
            == cached_document.get("content_version")
            and enriching.get("content_version")
            == cached_document.get("content_version")
            and processing.get("summary")
        ):
            cached_result = {
                "summary": processing["summary"],
                "tags": enriching.get("tags", []),
            }

            document = build_cached_document(
                user_id=user_id,
                title=title,
                content=content,
                content_hash=content_hash,
                cached_result=cached_result,
                client_doc_ref=client_doc_ref,
            )

            result = await db.documents.insert_one(document)
            document["_id"] = result.inserted_id

            return document

    # ---------------------------------------------------------
    # 5. Reserve active processing slot
    # ---------------------------------------------------------
    reserved = await reserve_job(
        redis,
        user_id,
    )

    if not reserved:
        raise HTTPException(
            status_code=429,
            detail="Maximum 3 active documents allowed per user",
        )

    # ---------------------------------------------------------
    # 6. Create queued document
    # ---------------------------------------------------------
    document = build_document(
        user_id=user_id,
        title=title,
        content=content,
        content_hash=content_hash,
        client_doc_ref=client_doc_ref,
    )

    try:
        result = await db.documents.insert_one(document)

        document["_id"] = result.inserted_id

        # -----------------------------------------------------
        # 7. Add job to Redis Stream
        # -----------------------------------------------------
        await enqueue_document(
            redis=redis,
            document_id=str(document["_id"]),
            version=document["content_version"],
        )

        return document

    except Exception:
        # Prevent leaked Redis slot if Mongo/Redis operation fails.
        from services.limits import release_job

        await release_job(
            redis,
            user_id,
        )

        raise


def build_document_response(document):
    current_version = document["content_version"]

    processing = document.get("processing", {})
    enriching = document.get("enriching", {})

    processing_version = processing.get("content_version")
    enriching_version = enriching.get("content_version")

    processing_current = (
        processing.get("status") == "completed"
        and processing_version == current_version
    )

    enriching_current = (
        enriching.get("status") == "completed"
        and enriching_version == current_version
    )

    result_current = (
        processing_current
        and enriching_current
        and document.get("status") == "completed"
    )

    return {
        "document_id": str(document["_id"]),
        "user_id": document["user_id"],
        "title": document["title"],
        "status": document["status"],

        "content_version": current_version,
        "content_hash": document["content_hash"],

        "processing": {
            "status": processing.get("status", "queued"),
            "content_version": processing_version,
        },

        "enriching": {
            "status": enriching.get("status", "queued"),
            "content_version": enriching_version,
        },

        "summary": (
            processing.get("summary")
            if processing_current
            else None
        ),

        "tags": (
            enriching.get("tags", [])
            if enriching_current
            else []
        ),

        "failed_stage": document.get("failed_stage"),

        "is_stale": not result_current,
    }


async def update_document(
    db,
    redis,
    document_id: str,
    user_id: str,
    content: str,
):
    if not ObjectId.is_valid(document_id):
        raise HTTPException(
            status_code=404,
            detail="Document not found",
        )

    object_id = ObjectId(document_id)

    document = await db.documents.find_one(
        {
            "_id": object_id,
            "user_id": user_id,
        }
    )

    if not document:
        raise HTTPException(
            status_code=404,
            detail="Document not found",
        )

    new_hash = calculate_content_hash(content)

    # Same content = no-op.
    if new_hash == document["content_hash"]:
        return document

    old_version = document["content_version"]
    new_version = old_version + 1

    current_status = document["status"]

    # If document is already active, it already owns a Redis slot.
    already_active = current_status in {
        "queued",
        "processing",
        "enriching",
    }

    reserved = False

    # Completed/failed documents need a new active slot.
    if not already_active:
        reserved = await reserve_job(
            redis,
            user_id,
        )

        if not reserved:
            raise HTTPException(
                status_code=429,
                detail=(
                    "Maximum 3 active documents "
                    "allowed per user"
                ),
            )

    try:
        # Optimistic concurrency:
        # another PATCH may have changed this document already.
        result = await db.documents.update_one(
            {
                "_id": object_id,
                "user_id": user_id,
                "content_version": old_version,
            },
            {
                "$set": {
                    "content": content,
                    "content_hash": new_hash,
                    "content_version": new_version,

                    "status": "queued",
                    "failed_stage": None,

                    "processing": {
                        "status": "queued",
                        "summary": None,
                        "content_version": None,
                    },

                    "enriching": {
                        "status": "queued",
                        "tags": [],
                        "content_version": None,
                    },

                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

        if result.modified_count != 1:
            if reserved:
                from services.limits import release_job

                await release_job(
                    redis,
                    user_id,
                )

            raise HTTPException(
                status_code=409,
                detail=(
                    "Document was modified by another request. "
                    "Please retry."
                ),
            )

        # New version gets a new queue message.
        await enqueue_document(
            redis=redis,
            document_id=document_id,
            version=new_version,
        )

    except HTTPException:
        raise

    except Exception:
        if reserved:
            from services.limits import release_job

            await release_job(
                redis,
                user_id,
            )

        raise

    updated_document = await db.documents.find_one(
        {
            "_id": object_id,
        }
    )

    return updated_document