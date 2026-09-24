import hashlib
from fastapi import HTTPException
from db import redis
from workers.queue import enqueue_document
from models.documents import build_document

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

