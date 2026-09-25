from bson import ObjectId
from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Query,
    status,
)

from schemas.document import (
    DocumentCreate,
    DocumentResponse,
    DocumentUpdate,
)

from services.document_service import (
    build_document_response,
    create_document,
    update_document,
)

from db.mongodb import get_database
from db.redis import get_redis


document_router = APIRouter(
    prefix="/documents",
    tags=["Documents"],
)


async def get_current_user_id(
    x_user_id: str | None = Header(
        default=None,
        alias="X-User-Id",
    ),
):
    """
    Simulates an authenticated user.

    In production this would come from JWT/session
    authentication rather than a header.
    """

    if not x_user_id:
        raise HTTPException(
            status_code=401,
            detail="Missing X-User-Id header",
        )

    return x_user_id


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
    current_user_id: str = Depends(get_current_user_id),
    db=Depends(get_database),
    redis=Depends(get_redis),
):
    # The submitting user must match the authenticated user.
    if payload.user_id != current_user_id:
        raise HTTPException(
            status_code=403,
            detail="Cannot create a document for another user",
        )

    document = await create_document(
        db=db,
        redis=redis,
        user_id=current_user_id,
        title=payload.title,
        content=payload.content,
        client_doc_ref=payload.client_doc_ref,
    )

    return build_document_response(document)


# ============================================================
# GET BY REF
# ============================================================

@document_router.get(
    "/by-ref/{client_doc_ref}",
    response_model=DocumentResponse,
)
async def get_document_by_ref(
    client_doc_ref: str,
    current_user_id: str = Depends(get_current_user_id),
    db=Depends(get_database),
):
    document = await db.documents.find_one(
        {
            "client_doc_ref": client_doc_ref,
            "user_id": current_user_id,
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
    page: int = Query(
        1,
        ge=1,
    ),
    page_size: int = Query(
        10,
        ge=1,
        le=100,
    ),
    status_filter: str | None = Query(
        default=None,
        alias="status",
    ),
    current_user_id: str = Depends(get_current_user_id),
    db=Depends(get_database),
):
    # Owner-only access.
    if user_id != current_user_id:
        raise HTTPException(
            status_code=404,
            detail="User not found",
        )

    query = {
        "user_id": current_user_id,
    }

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

    documents = await cursor.to_list(
        length=page_size,
    )

    return {
        "page": page,
        "page_size": page_size,
        "documents": [
            build_document_response(document)
            for document in documents
        ],
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
    current_user_id: str = Depends(get_current_user_id),
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
            "user_id": current_user_id,
        }
    )

    if not document:
        raise HTTPException(
            status_code=404,
            detail="Document not found",
        )

    return build_document_response(document)


# ============================================================
# PATCH
# ============================================================

@document_router.patch(
    "/{document_id}",
    response_model=DocumentResponse,
)
async def update_document_api(
    document_id: str,
    data: DocumentUpdate,
    current_user_id: str = Depends(get_current_user_id),
    db=Depends(get_database),
    redis=Depends(get_redis),
):
    document = await update_document(
        db=db,
        redis=redis,
        document_id=document_id,
        user_id=current_user_id,
        content=data.content,
    )

    return build_document_response(document)