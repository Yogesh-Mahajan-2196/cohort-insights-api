from datetime import datetime, timezone

def utc_now():
    return datetime.now(timezone.utc)

def build_document(
        user_id: str,
        title: str,
        content: str,
        content_hash: str,
        client_doc_ref: str | None = None
):
    return {
        "user_id": user_id,
        "title": title,
        "content": content,

        "content_version": 1,
        "content_hash": content_hash,

        "status": "queued",
        "failed_stage": None,

        # Stage 1
        "processing": {
            "status": "queued",
            "summery": None,
            "content_version": None,
        },

        # Stage 2
        "enriching": {
            "status": "queued",
            "tags": [],
            "content_version": None,
        },

        "client_doc_ref": client_doc_ref,

        "created_at": utc_now(),
        "updated_at": utc_now()
    }