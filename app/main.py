from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
from core.config import settings
from db.db_connect import lifespan
from api.routes.documents import document_router
from db.mongodb import get_database
from db.redis import get_redis

app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(document_router)


@app.get("/health")
async def health(
    db=Depends(get_database),
    redis=Depends(get_redis),
):
    mongo_status = "ok"
    redis_status = "ok"

    try:
        await db.command("ping")
    except Exception:
        mongo_status = "error"

    try:
        await redis.ping()
    except Exception:
        redis_status = "error"

    healthy = (
        mongo_status == "ok"
        and redis_status == "ok"
    )

    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "status": (
                "ok"
                if healthy
                else "degraded"
            ),
            "mongodb": mongo_status,
            "redis": redis_status,
        },
    )
