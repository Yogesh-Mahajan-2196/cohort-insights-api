async def create_indexes(db):

    # User's documents, newest first
    await db.documents.create_index(
        [
            ("user_id", 1),
            ("created_at", -1),
        ]
    )

    # User + status filtering + newest first
    await db.documents.create_index(
        [
            ("user_id", 1),
            ("status", 1),
            ("created_at", -1),
        ]
    )

    # Content-based lookup/cache fallback
    await db.documents.create_index(
        [
            ("content_hash", 1),
        ]
    )

    # Unique client reference when supplied.
    await db.documents.create_index(
        [
            ("client_doc_ref", 1),
        ],
        unique=True,
        partialFilterExpression={
            "client_doc_ref": {
                "$type": "string"
            }
        },
    )