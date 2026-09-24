from fastapi import APIRouter, Depends, status

from schemas.document import (
    DocumentCreate,
    DocumentResponse,
)
from services.document_service import create_document
from db.mongodb import get_database
from db.redis import get_redis

document_router = APIRouter(
    prefix="/documents",
    tags=["Documents"],
)


@document_router.post(
    "",
    response_model=DocumentResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_document_api(
    payload: DocumentCreate,
    db=Depends(get_database),
    redis=Depends(get_redis),
):

    document = await create_document(
        db=db,
        redis=redis,
        user_id=payload.user_id,
        title=payload.title,
        content=payload.content,
        client_doc_ref=payload.client_doc_ref,
    )

    return {
        "document_id": str(document["_id"]),
        "user_id": document["user_id"],
        "title": document["title"],
        "status": document.get("status"),
        "content_version": document.get(
            "content_version",
            1,
        ),
        "content_hash": document["content_hash"],
        "summary": document.get("processing", {}).get("summary"),
        "tags": document.get("enriching", {}).get("tags", []),
        "failed_stage": document.get(
            "failed_stage"
        ),
    }