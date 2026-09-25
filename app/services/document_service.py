import hashlib
from datetime import datetime, timezone

from bson import ObjectId
from fastapi import HTTPException
from pymongo.errors import DuplicateKeyError

from models.documents import build_document
from services.cache import (
    get_cached_result,
    set_cached_result,
)
from services.limits import (
    reserve_job,
    release_job,
    update_active_job_version,
)
from workers.queue import enqueue_document


ACTIVE_STATUSES = {
    "queued",
    "processing",
    "enriching",
}


def calculate_content_hash(content: str) -> str:
    return hashlib.sha256(
        content.encode("utf-8")
    ).hexdigest()


def utc_now():
    return datetime.now(timezone.utc)


def build_document_response(document: dict) -> dict:
    current_version = document["content_version"]

    processing = document.get("processing") or {}
    enriching = document.get("enriching") or {}

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
        document.get("status") == "completed"
        and processing_current
        and enriching_current
    )

    return {
        "document_id": str(document["_id"]),
        "user_id": document["user_id"],
        "title": document["title"],

        "status": document["status"],

        "content_version": current_version,
        "content_hash": document["content_hash"],

        "processing": {
            "status": processing.get(
                "status",
                "queued",
            ),
            "content_version": processing_version,
        },

        "enriching": {
            "status": enriching.get(
                "status",
                "queued",
            ),
            "content_version": enriching_version,
        },

        # Never expose an old result for a newer content version.
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

        "failed_stage": document.get(
            "failed_stage"
        ),

        "is_stale": not result_current,
    }


def build_cached_document(
    user_id: str,
    title: str,
    content: str,
    content_hash: str,
    cached_result: dict,
    client_doc_ref: str | None = None,
) -> dict:
    """
    Build a new completed document from an already-computed result.

    This does not consume an active processing slot because the
    document does not need background processing.
    """

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
        "tags": cached_result.get("tags", []),
        "content_version": 1,
    }

    return document


async def _insert_cached_document(
    db,
    user_id: str,
    title: str,
    content: str,
    content_hash: str,
    cached_result: dict,
    client_doc_ref: str | None = None,
):
    """
    Insert a completed document generated from cache.

    DuplicateKeyError is intentionally allowed to propagate so the
    caller can handle client_doc_ref races.
    """

    document = build_cached_document(
        user_id=user_id,
        title=title,
        content=content,
        content_hash=content_hash,
        cached_result=cached_result,
        client_doc_ref=client_doc_ref,
    )

    await db.documents.insert_one(document)

    return document


async def _get_mongo_cached_result(
    db,
    content_hash: str,
):
    """
    Find a genuinely completed/current result for this content hash.

    A document is usable as a cache only when:
      - the document is completed
      - processing is completed
      - enriching is completed
      - processing version == document version
      - enriching version == document version
    """

    cached_document = await db.documents.find_one(
        {
            "content_hash": content_hash,
            "status": "completed",
            "processing.status": "completed",
            "enriching.status": "completed",
        }
    )

    if not cached_document:
        return None

    current_version = cached_document.get(
        "content_version"
    )

    processing = cached_document.get(
        "processing"
    ) or {}

    enriching = cached_document.get(
        "enriching"
    ) or {}

    if (
        processing.get("content_version")
        != current_version
    ):
        return None

    if (
        enriching.get("content_version")
        != current_version
    ):
        return None

    if "summary" not in processing:
        return None

    return {
        "summary": processing["summary"],
        "tags": enriching.get("tags", []),
    }


async def _find_existing_by_client_ref(
    db,
    client_doc_ref: str,
):
    return await db.documents.find_one(
        {
            "client_doc_ref": client_doc_ref,
        }
    )


async def _handle_duplicate_client_ref(
    db,
    user_id: str,
    client_doc_ref: str,
    content_hash: str,
):
    """
    Resolve a client_doc_ref race after a DuplicateKeyError.
    """

    existing = await _find_existing_by_client_ref(
        db,
        client_doc_ref,
    )

    if not existing:
        raise HTTPException(
            status_code=409,
            detail="Duplicate document reference",
        )

    # Do not reveal another user's document.
    if existing["user_id"] != user_id:
        raise HTTPException(
            status_code=404,
            detail="Document not found",
        )

    # Same reference + same content = idempotent retry.
    if existing["content_hash"] == content_hash:
        return existing

    # Same reference + different content = conflict.
    raise HTTPException(
        status_code=409,
        detail=(
            "client_doc_ref already exists "
            "with different content"
        ),
    )


async def create_document(
    db,
    redis_client,
    user_id: str,
    title: str,
    content: str,
    client_doc_ref: str | None = None,
):
    content_hash = calculate_content_hash(content)

    # =========================================================
    # 1. client_doc_ref idempotency check
    # =========================================================

    if client_doc_ref:
        existing = await db.documents.find_one(
            {
                "client_doc_ref": client_doc_ref,
            }
        )

        if existing:
            if existing["user_id"] != user_id:
                raise HTTPException(
                    status_code=404,
                    detail="Document not found",
                )

            if existing["content_hash"] == content_hash:
                return existing

            raise HTTPException(
                status_code=409,
                detail=(
                    "client_doc_ref already exists "
                    "with different content"
                ),
            )

    # =========================================================
    # 2. Redis result cache
    # =========================================================

    cached_result = await get_cached_result(
        redis_client,
        content_hash,
    )

    if cached_result:
        try:
            return await _insert_cached_document(
                db=db,
                user_id=user_id,
                title=title,
                content=content,
                content_hash=content_hash,
                cached_result=cached_result,
                client_doc_ref=client_doc_ref,
            )

        except DuplicateKeyError:
            if client_doc_ref:
                return await _handle_duplicate_client_ref(
                    db=db,
                    user_id=user_id,
                    client_doc_ref=client_doc_ref,
                    content_hash=content_hash,
                )

            raise

    # =========================================================
    # 3. Mongo completed-result cache
    # =========================================================

    cached_result = await _get_mongo_cached_result(
        db,
        content_hash,
    )

    if cached_result:
        await set_cached_result(
            redis_client,
            content_hash,
            cached_result,
        )

        try:
            return await _insert_cached_document(
                db=db,
                user_id=user_id,
                title=title,
                content=content,
                content_hash=content_hash,
                cached_result=cached_result,
                client_doc_ref=client_doc_ref,
            )

        except DuplicateKeyError:
            if client_doc_ref:
                return await _handle_duplicate_client_ref(
                    db=db,
                    user_id=user_id,
                    client_doc_ref=client_doc_ref,
                    content_hash=content_hash,
                )

            raise

    # =========================================================
    # 4. Create document and reserve active slot
    # =========================================================

    document = build_document(
        user_id=user_id,
        title=title,
        content=content,
        content_hash=content_hash,
        client_doc_ref=client_doc_ref,
    )

    document_id = str(document["_id"])
    version = document["content_version"]

    reserved = await reserve_job(
        redis_client=redis_client,
        user_id=user_id,
        document_id=document_id,
        version=version,
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
        # -----------------------------------------------------
        # Insert document
        # -----------------------------------------------------

        await db.documents.insert_one(
            document
        )

        # -----------------------------------------------------
        # Enqueue job
        # -----------------------------------------------------

        try:
            await enqueue_document(
                redis_client=redis_client,
                document_id=document_id,
                version=version,
            )

        except Exception:
            # There is no usable job anymore.
            await db.documents.delete_one(
                {
                    "_id": document["_id"],
                    "content_version": version,
                }
            )

            raise HTTPException(
                status_code=503,
                detail="Unable to enqueue document",
            )

        # Important:
        # Do NOT release the slot here.
        #
        # The worker owns the active slot until the complete
        # processing pipeline finishes.

        return document

    except DuplicateKeyError:
        # Release the reservation because this request did not
        # create a usable active job.
        await release_job(
            redis_client,
            user_id,
            document_id,
            version,
        )

        if client_doc_ref:
            return await _handle_duplicate_client_ref(
                db=db,
                user_id=user_id,
                client_doc_ref=client_doc_ref,
                content_hash=content_hash,
            )

        raise HTTPException(
            status_code=409,
            detail="Duplicate document",
        )

    except HTTPException:
        await release_job(
            redis_client,
            user_id,
            document_id,
            version,
        )
        raise

    except Exception:
        await release_job(
            redis_client,
            user_id,
            document_id,
            version,
        )
        raise


async def update_document(
    db,
    redis_client,
    document_id: str,
    user_id: str,
    content: str,
):
    """
    Update document content using optimistic concurrency.

    Every content change creates a new content_version.

    Old workers may still finish, but worker-side version checks
    must prevent them from modifying the newer version.
    """

    # =========================================================
    # 1. Validate ObjectId
    # =========================================================

    if not ObjectId.is_valid(document_id):
        raise HTTPException(
            status_code=404,
            detail="Document not found",
        )

    object_id = ObjectId(document_id)

    # =========================================================
    # 2. Load current document
    # =========================================================

    current = await db.documents.find_one(
        {
            "_id": object_id,
            "user_id": user_id,
        }
    )

    if not current:
        raise HTTPException(
            status_code=404,
            detail="Document not found",
        )

    # =========================================================
    # 3. Same content = idempotent/no-op
    # =========================================================

    new_hash = calculate_content_hash(content)

    if new_hash == current["content_hash"]:
        return current

    old_version = current["content_version"]
    new_version = old_version + 1

    old_status = current.get("status")

    was_active = (
        old_status in ACTIVE_STATUSES
    )

    # =========================================================
    # 4. Reserve slot only when old document wasn't active
    # =========================================================

    reserved_new_slot = False

    if not was_active:
        reserved_new_slot = await reserve_job(
            redis_client=redis_client,
            user_id=user_id,
            document_id=document_id,
            version=new_version,
        )

        if not reserved_new_slot:
            raise HTTPException(
                status_code=429,
                detail=(
                    "Maximum 3 active documents "
                    "allowed per user"
                ),
            )

    update_succeeded = False

    try:
        # =====================================================
        # 5. Optimistic concurrency update
        # =====================================================

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

                    # Invalidate old processing result immediately.
                    "processing": {
                        "status": "queued",
                        "summary": None,
                        "content_version": None,
                    },

                    # Invalidate old enrichment result immediately.
                    "enriching": {
                        "status": "queued",
                        "tags": [],
                        "content_version": None,
                    },

                    "updated_at": utc_now(),
                }
            },
        )

        if result.modified_count != 1:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Document was updated by another request"
                ),
            )

        update_succeeded = True

        # =====================================================
        # 6. If already active, move slot to new version
        # =====================================================

        if was_active:
            await update_active_job_version(
                redis_client=redis_client,
                user_id=user_id,
                document_id=document_id,
                version=new_version,
            )

        # =====================================================
        # 7. Enqueue new version
        # =====================================================

        try:
            await enqueue_document(
                redis_client=redis_client,
                document_id=document_id,
                version=new_version,
            )

        except Exception:
            await db.documents.update_one(
                {
                    "_id": object_id,
                    "content_version": new_version,
                },
                {
                    "$set": {
                        "status": "failed",
                        "failed_stage": "processing",
                        "updated_at": utc_now(),
                    }
                },
            )

            await release_job(
                redis_client=redis_client,
                user_id=user_id,
                document_id=document_id,
                version=new_version,
            )

            raise HTTPException(
                status_code=503,
                detail="Unable to enqueue updated document",
            )
        
        return await db.documents.find_one(
            {
                "_id": object_id,
                "user_id": user_id,
            }
        )

    except HTTPException:
        raise

    except Exception:
        raise

    finally:
        # If we reserved a brand-new slot and the update did not
        # successfully become an active queued job, release it.
        #
        # If enqueue succeeded, the worker owns the lifecycle and
        # will eventually release it.
        if reserved_new_slot and not update_succeeded:
            await release_job(
                redis_client,
                user_id,
                document_id,
                new_version,
            )
