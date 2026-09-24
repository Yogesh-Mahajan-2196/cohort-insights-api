STREAM_NAME = "document_processing"

GROUP_NAME = "document_workers"


async def create_consumer_group(redis):
    try:
        await redis.xgroup_create(
            name=STREAM_NAME,
            groupname=GROUP_NAME,
            id="0",
            mkstream=True,
        )
    except Exception as exc:
        # BUSYGROUP means the group already exists.
        if "BUSYGROUP" not in str(exc):
            raise


async def enqueue_document(redis, document_id: str, version: int):
    message_id = await redis.xadd(
        STREAM_NAME,
        {
            "document_id": document_id,
            "content_version": str(version),
        },
    )

    return message_id