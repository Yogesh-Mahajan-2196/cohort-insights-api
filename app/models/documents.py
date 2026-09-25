from datetime import datetime, timezone


def utc_now():
    return datetime.now(timezone.utc)


def build_document(
    user_id: str,
    title: str,
    content: str,
    content_hash: str,
    client_doc_ref: str | None = None,
):
    document = {
        "user_id": user_id,
        "title": title,
        "content": content,

        "content_version": 1,
        "content_hash": content_hash,

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

        "created_at": utc_now(),
        "updated_at": utc_now(),
    }

    if client_doc_ref is not None:
        document["client_doc_ref"] = client_doc_ref

    return document