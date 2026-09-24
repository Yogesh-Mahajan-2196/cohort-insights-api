import hashlib
from fastapi import HTTPException
from db import redis
from bson import ObjectId
from workers.queue import enqueue_document
from models.documents import build_document
from datetime import datetime, timezone

def calculate_content_hash(content:str):
    return hashlib.sha256(
        content.encode("utf-8")
    ).hexdigest()


async def create_document(
    db,
    redis,
    user_id: int,
    title: str,
    content: str,
    client_doc_ref: str | None = None
):
    
    content_hash = calculate_content_hash(content)

    # Check client_doc_ref
    if client_doc_ref:
        existing = await db.document.find_one(
            {
                "client_doc_ref" : client_doc_ref
            }
        )

        if existing:
            if existing['content_hash'] == content_hash:
                return existing

            raise HTTPException(
                status_code=409,
                detail="client_doc_ref already exists with different content"
            )

    # Content cache check
    cached = await db.documents.find_one(
        {
            "content_hash": content_hash,
            "status": "completed",
        }
    )

    if cached:

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
            "summary": cached["processing"]["summary"],
            "content_version": 1,
        }

        document["enriching"] = {
            "status": "completed",
            "tags": cached["enriching"]["tags"],
            "content_version": 1,
        }

        result = await db.documents.insert_one(document)

        document["_id"] = result.inserted_id

        return document

    # Create new document

    document = build_document(
        user_id=user_id,
        title=title,
        content=content,
        content_hash=content_hash,
        client_doc_ref=client_doc_ref
    )

    result = await db.documents.insert_one(document)

    document["id"] = result.inserted_id

    # Add job to redis stream

    await enqueue_document(
        redis=redis,
        document_id=str(document["_id"]),
        version=document["content_version"],
    )


    return document

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

    result_current = processing_current and enriching_current

    return {
        "document_id": str(document["_id"]),
        "user_id": document["user_id"],
        "title": document["title"],
        "status": document["status"],
        "content_version": current_version,
        "content_hash": document["content_hash"],

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

    new_version = document["content_version"] + 1

    new_hash = hashlib.sha256(
        content.encode("utf-8")
    ).hexdigest()

    await db.documents.update_one(
        {
            "_id": object_id,
            "user_id": user_id,
            "content_version": document["content_version"],
            
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

    await enqueue_document(
        redis=redis,
        document_id=document_id,
        version=new_version,
    )

    updated_document = await db.documents.find_one(
        {"_id": object_id}
    )

    return updated_document