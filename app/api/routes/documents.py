from fastapi import APIRouter, Depends, status

from schemas.document import (
    DocumentCreate,
    DocumentResponse,
    DocumentUpdate,
)
from services.document_service import build_document_response, create_document, update_document
from db.mongodb import get_database
from db.redis import get_redis
from bson import ObjectId
from fastapi import HTTPException


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


@document_router.get("/documents/{document_id}")
async def get_document_api(
    document_id: str,
    user_id: str,
    db=Depends(get_database),
):
    if not ObjectId.is_valid(document_id):
        raise HTTPException(
            status_code=404,
            detail="Document not found",
        )

    document = await db.documents.find_one(
        {
            "_id": ObjectId(document_id),
            "user_id": user_id,
        }
    )

    if not document:
        raise HTTPException(
            status_code=404,
            detail="Document not found",
        )

    return build_document_response(document)

@document_router.patch("/documents/{document_id}")
async def update_document_api(
    document_id: str,
    data: DocumentUpdate,
    user_id: str,
    db=Depends(get_database),
    redis=Depends(get_redis),
):
    document = await update_document(
        db=db,
        redis=redis,
        document_id=document_id,
        user_id=user_id,
        content=data.content,
        
    )

    return build_document_response(document)