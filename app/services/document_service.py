import hashlib
from datetime import datetime, timezone

from bson import ObjectId
from fastapi import HTTPException
from pymongo.errors import DuplicateKeyError

from models.documents import build_document
from services.cache import get_cached_result, set_cached_result
from services.limits import (
    MAX_ACTIVE_JOBS,
    reserve_job,
    release_job,
    set_job_info,
    update_active_job_version,
    delete_job_info,
)
from workers.queue import enqueue_document


ACTIVE_STATUSES = {"queued", "processing", "enriching"}


def calculate_content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


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
        "client_doc_ref": document.get("client_doc_ref"),
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


def _is_valid_cached_result(result: dict | None) -> bool:
    return (
        isinstance(result, dict)
        and isinstance(result.get("summary"), str)
        and isinstance(result.get("tags", []), list)
    )


def build_cached_document(
    user_id: str,
    title: str,
    content: str,
    content_hash: str,
    cached_result: dict,
    client_doc_ref: str | None = None,
) -> dict:
    document = build_document(
        user_id=user_id,
        title=title,
        content=content,
        content_hash=content_hash,
        client_doc_ref=client_doc_ref,
    )

    document["status"] = "completed"
    document["failed_stage"] = None

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


async def _get_mongo_cached_result(db, content_hash: str):
    document = await db.documents.find_one(
        {
            "content_hash": content_hash,
            "status": "completed",
            "processing.status": "completed",
            "enriching.status": "completed",
        }
    )

    if not document:
        return None

    current_version = document.get("content_version")
    processing = document.get("processing") or {}
    enriching = document.get("enriching") or {}

    if processing.get("content_version") != current_version:
        return None

    if enriching.get("content_version") != current_version:
        return None

    summary = processing.get("summary")
    tags = enriching.get("tags", [])

    if not isinstance(summary, str) or not isinstance(tags, list):
        return None

    return {
        "summary": summary,
        "tags": tags,
    }


async def _find_existing_by_client_ref(db, client_doc_ref: str):
    return await db.documents.find_one(
        {"client_doc_ref": client_doc_ref}
    )


async def _resolve_duplicate_client_ref(
    db,
    user_id: str,
    client_doc_ref: str,
    content_hash: str,
):
    existing = await _find_existing_by_client_ref(
        db,
        client_doc_ref,
    )

    if not existing:
        raise HTTPException(
            status_code=409,
            detail="Duplicate document reference",
        )

    if existing["user_id"] != user_id:
        raise HTTPException(
            status_code=404,
            detail="Document not found",
        )

    if existing["content_hash"] == content_hash:
        return existing, False

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

    # ---------------------------------------------------------
    # 1. Idempotency by client_doc_ref.
    # ---------------------------------------------------------
    if client_doc_ref:
        existing = await _find_existing_by_client_ref(
            db,
            client_doc_ref,
        )

        if existing:
            if existing["user_id"] != user_id:
                raise HTTPException(
                    status_code=404,
                    detail="Document not found",
                )

            if existing["content_hash"] == content_hash:
                return existing, False

            raise HTTPException(
                status_code=409,
                detail=(
                    "client_doc_ref already exists "
                    "with different content"
                ),
            )

    # ---------------------------------------------------------
    # 2. Redis completed-result cache.
    # Cache hits do not create a background job.
    # ---------------------------------------------------------
    cached_result = await get_cached_result(
        redis_client,
        content_hash,
    )

    if _is_valid_cached_result(cached_result):
        try:
            document = await _insert_cached_document(
                db=db,
                user_id=user_id,
                title=title,
                content=content,
                content_hash=content_hash,
                cached_result=cached_result,
                client_doc_ref=client_doc_ref,
            )

            await set_job_info(
                redis_client=redis_client,
                user_id=user_id,
                client_doc_ref=client_doc_ref,
                document_id=str(document["_id"]),
                version=1,
                status="completed",
            )

            return document, True

        except DuplicateKeyError:
            if client_doc_ref:
                return await _resolve_duplicate_client_ref(
                    db,
                    user_id,
                    client_doc_ref,
                    content_hash,
                )
            raise HTTPException(
                status_code=409,
                detail="Duplicate document",
            )

    # ---------------------------------------------------------
    # 3. Mongo completed-result cache.
    # ---------------------------------------------------------
    cached_result = await _get_mongo_cached_result(
        db,
        content_hash,
    )

    if _is_valid_cached_result(cached_result):
        await set_cached_result(
            redis_client,
            content_hash,
            cached_result,
        )

        try:
            document = await _insert_cached_document(
                db=db,
                user_id=user_id,
                title=title,
                content=content,
                content_hash=content_hash,
                cached_result=cached_result,
                client_doc_ref=client_doc_ref,
            )

            await set_job_info(
                redis_client=redis_client,
                user_id=user_id,
                client_doc_ref=client_doc_ref,
                document_id=str(document["_id"]),
                version=1,
                status="completed",
            )

            return document, True

        except DuplicateKeyError:
            if client_doc_ref:
                return await _resolve_duplicate_client_ref(
                    db,
                    user_id,
                    client_doc_ref,
                    content_hash,
                )
            raise HTTPException(
                status_code=409,
                detail="Duplicate document",
            )

    # ---------------------------------------------------------
    # 4. New background job.
    # ---------------------------------------------------------
    document = build_document(
        user_id=user_id,
        title=title,
        content=content,
        content_hash=content_hash,
        client_doc_ref=client_doc_ref,
    )

    document_id = str(document["_id"])
    version = document["content_version"]

    reservation = await reserve_job(
        redis_client=redis_client,
        user_id=user_id,
        document_id=document_id,
        version=version,
    )

    if reservation == 0:
        raise HTTPException(
            status_code=429,
            detail=(
                f"Maximum {MAX_ACTIVE_JOBS} active "
                "job(s) allowed per user"
            ),
        )

    if reservation == 3:
        raise HTTPException(
            status_code=409,
            detail="A newer version of this document is already active",
        )

    try:
        await set_job_info(
            redis_client=redis_client,
            user_id=user_id,
            client_doc_ref=client_doc_ref,
            document_id=document_id,
            version=version,
            status="queued",
        )

        await db.documents.insert_one(document)

        try:
            await enqueue_document(
                redis_client=redis_client,
                document_id=document_id,
                version=version,
            )
        except Exception as exc:
            await db.documents.delete_one(
                {
                    "_id": document["_id"],
                    "content_version": version,
                }
            )
            await delete_job_info(
                redis_client=redis_client,
                user_id=user_id,
                client_doc_ref=client_doc_ref,
                document_id=document_id,
            )
            raise HTTPException(
                status_code=503,
                detail="Unable to enqueue document",
            ) from exc

        return document, True

    except DuplicateKeyError:
        await release_job(
            redis_client,
            user_id,
            document_id,
            version,
        )
        await delete_job_info(
            redis_client=redis_client,
            user_id=user_id,
            client_doc_ref=client_doc_ref,
            document_id=document_id,
        )

        if client_doc_ref:
            return await _resolve_duplicate_client_ref(
                db,
                user_id,
                client_doc_ref,
                content_hash,
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
        await delete_job_info(
            redis_client=redis_client,
            user_id=user_id,
            client_doc_ref=client_doc_ref,
            document_id=document_id,
        )
        raise

    except Exception:
        await release_job(
            redis_client,
            user_id,
            document_id,
            version,
        )
        await delete_job_info(
            redis_client=redis_client,
            user_id=user_id,
            client_doc_ref=client_doc_ref,
            document_id=document_id,
        )
        raise


async def update_document(
    db,
    redis_client,
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

    new_hash = calculate_content_hash(content)

    if new_hash == current["content_hash"]:
        return current

    old_version = current["content_version"]
    new_version = old_version + 1
    old_status = current.get("status")
    was_active = old_status in ACTIVE_STATUSES

    # If the document is terminal, a new active slot is needed.
    # Reservation result 1 means this request created the slot.
    # Result 2 means the document already owns the slot.
    reserved_new_slot = False

    if not was_active:
        reservation = await reserve_job(
            redis_client=redis_client,
            user_id=user_id,
            document_id=document_id,
            version=new_version,
        )

        if reservation == 0:
            raise HTTPException(
                status_code=429,
                detail=(
                    f"Maximum {MAX_ACTIVE_JOBS} active "
                    "job(s) allowed per user"
                ),
            )

        if reservation == 3:
            raise HTTPException(
                status_code=409,
                detail="A newer version of this document is already active",
            )

        reserved_new_slot = reservation == 1

    update_succeeded = False

    try:
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
                    "updated_at": utc_now(),
                }
            },
        )

        if result.modified_count != 1:
            raise HTTPException(
                status_code=409,
                detail="Document was updated by another request",
            )

        update_succeeded = True

        # For an already-active document, move the slot only if it
        # still belongs to old_version. Never downgrade a newer slot.
        if was_active:
            await update_active_job_version(
                redis_client=redis_client,
                user_id=user_id,
                document_id=document_id,
                old_version=old_version,
                new_version=new_version,
            )

        await set_job_info(
            redis_client=redis_client,
            user_id=user_id,
            client_doc_ref=current.get("client_doc_ref"),
            document_id=document_id,
            version=new_version,
            status="queued",
        )

        try:
            await enqueue_document(
                redis_client=redis_client,
                document_id=document_id,
                version=new_version,
            )
        except Exception as exc:
            # The update exists but cannot be processed. Mark it terminal
            # and release only this exact version.
            await db.documents.update_one(
                {
                    "_id": object_id,
                    "content_version": new_version,
                },
                {
                    "$set": {
                        "status": "failed",
                        "failed_stage": "processing",
                        "processing.status": "failed",
                        "processing.content_version": new_version,
                        "updated_at": utc_now(),
                    }
                },
            )

            await release_job(
                redis_client,
                user_id,
                document_id,
                new_version,
            )

            await set_job_info(
                redis_client=redis_client,
                user_id=user_id,
                client_doc_ref=current.get("client_doc_ref"),
                document_id=document_id,
                version=new_version,
                status="failed",
            )

            raise HTTPException(
                status_code=503,
                detail="Unable to enqueue updated document",
            ) from exc

        updated = await db.documents.find_one(
            {
                "_id": object_id,
                "user_id": user_id,
                "content_version": new_version,
            }
        )

        if not updated:
            raise HTTPException(
                status_code=500,
                detail="Updated document could not be loaded",
            )

        return updated

    except HTTPException:
        raise

    except Exception:
        raise

    finally:
        # Release only if this request itself created the reservation
        # and the Mongo update did not become a valid active job.
        if reserved_new_slot and not update_succeeded:
            await release_job(
                redis_client,
                user_id,
                document_id,
                new_version,
            )
