STREAM_NAME = "document_processing"
GROUP_NAME = "document_workers"


async def create_consumer_group(
    redis_client,
):
    try:

        await redis_client.xgroup_create(
            name=STREAM_NAME,
            groupname=GROUP_NAME,
            id="0",
            mkstream=True,
        )

        print(
            f"[REDIS] Consumer group created: "
            f"{GROUP_NAME}"
        )

    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            print(
                f"[REDIS] Consumer group already exists: "
                f"{GROUP_NAME}"
            )

        else:
            raise


async def enqueue_document(
    redis_client,
    document_id: str,
    version: int,
):

    message_id = await redis_client.xadd(
        STREAM_NAME,
        {
            "document_id": document_id,
            "content_version": str(version),
        },
    )

    print(
        f"[REDIS] XADD "
        f"stream={STREAM_NAME} "
        f"message={message_id} "
        f"document={document_id} "
        f"version={version}"
    )

    return message_id