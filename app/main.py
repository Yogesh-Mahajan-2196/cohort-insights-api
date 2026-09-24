from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from core.config import settings
from db.db_connect import lifespan
from api.routes.documents import document_router

app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(document_router)


@app.get("/health")
async def health(request:Request):
    mongo_status = "ok"
    redis_status = "ok"

    try:
        await request.app.state.db.command("ping")
    except Exception:
        mongo_status = "error"

    try:
        await request.app.state.redis.ping()
    except Exception:
        redis_status = "error"

    is_healthy = (
        mongo_status == "ok"
        and
        redis_status == "ok"
    )

    return JSONResponse(
        status_code=200 if is_healthy else 503,
        content= {
            "status": "ok" if is_healthy else "degraded",
            "mongodb": mongo_status,
            "redis": redis_status
        }
    )
