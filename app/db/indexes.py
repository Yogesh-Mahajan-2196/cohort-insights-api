async def create_indexes(db):

    await db.documents.create_index(
        [
            ("user_id", 1),
            ("created_at", -1),
        ]
    )

    await db.documents.create_index(
        [
            ("user_id", 1),
            ("status", 1),
            ("created_at", -1),
        ]
    )

    await db.documents.create_index(
        [
            ("content_hash", 1),
        ]
    )

    await db.documents.create_index(
        [
            ("client_doc_ref", 1),
        ],
        unique=True,
        sparse=True,
    )