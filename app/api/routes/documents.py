from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse

from db.mongodb import get_database
from db.redis import get_redis
from schemas.document import DocumentCreate, DocumentResponse, DocumentUpdate
from services.document_service import (
    build_document_response,
    create_document,
    update_document,
)


document_router = APIRouter(
    prefix="/documents",
    tags=["Documents"],
)

ALLOWED_DOCUMENT_STATUSES = {
    "queued",
    "processing",
    "enriching",
    "completed",
    "failed",
}


# ============================================================
# CREATE
# ============================================================

@document_router.post(
    "",
    response_model=DocumentResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_document_api(
    payload: DocumentCreate,
    db=Depends(get_database),
    redis_client=Depends(get_redis),
):
    document, created = await create_document(
        db=db,
        redis_client=redis_client,
        user_id=payload.user_id,
        title=payload.title,
        content=payload.content,
        client_doc_ref=payload.client_doc_ref,
    )

    response = build_document_response(document)

    # Same client_doc_ref + same content is an idempotent retry.
    if not created:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=response,
        )

    return response


# ============================================================
# GET BY CLIENT REF
# ============================================================

@document_router.get(
    "/by-ref/{client_doc_ref}",
    response_model=DocumentResponse,
)
async def get_document_by_ref(
    client_doc_ref: str,
    user_id: str = Query(..., min_length=1, max_length=100),
    db=Depends(get_database),
):
    document = await db.documents.find_one(
        {
            "client_doc_ref": client_doc_ref,
            "user_id": user_id,
        }
    )

    if not document:
        raise HTTPException(
            status_code=404,
            detail="Document not found",
        )

    return build_document_response(document)


# ============================================================
# LIST USER DOCUMENTS
# ============================================================

@document_router.get(
    "/users/{user_id}/documents",
)
async def list_user_documents(
    user_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    status_filter: str | None = Query(
        default=None,
        alias="status",
    ),
    db=Depends(get_database),
):
    if status_filter is not None and status_filter not in ALLOWED_DOCUMENT_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Invalid status. Allowed values: "
                + ", ".join(sorted(ALLOWED_DOCUMENT_STATUSES))
            ),
        )

    query = {"user_id": user_id}

    if status_filter:
        query["status"] = status_filter

    skip = (page - 1) * page_size

    cursor = (
        db.documents
        .find(query)
        .sort("created_at", -1)
        .skip(skip)
        .limit(page_size)
    )

    documents = []

    async for document in cursor:
        documents.append(build_document_response(document))

    return {
        "page": page,
        "page_size": page_size,
        "documents": documents,
    }


# ============================================================
# GET SINGLE DOCUMENT
# ============================================================

@document_router.get(
    "/{document_id}",
    response_model=DocumentResponse,
)
async def get_document_api(
    document_id: str,
    user_id: str = Query(..., min_length=1, max_length=100),
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


# ============================================================
# UPDATE
# ============================================================

@document_router.patch(
    "/{document_id}",
    response_model=DocumentResponse,
)
async def update_document_api(
    document_id: str,
    payload: DocumentUpdate,
    db=Depends(get_database),
    redis_client=Depends(get_redis),
):
    document = await update_document(
        db=db,
        redis_client=redis_client,
        document_id=document_id,
        user_id=payload.user_id,
        content=payload.content,
    )

    return build_document_response(document)
