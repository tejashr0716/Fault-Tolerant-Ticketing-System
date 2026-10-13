import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.routers.seats import router as seats_router
from app.routers.payments import router as payments_router
from app.services.cache import init_redis, close_redis, get_redis_client
from app.services.queue import close_rabbitmq

# Configure root logger
logging.basicConfig(
    level=logging.INFO if not settings.DEBUG else logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan event context manager managing async Redis and RabbitMQ connections."""
    logger.info("Initializing Cache and Queue Service resources...")
    await init_redis()
    yield
    logger.info("Shutting down Cache and Queue Service resources...")
    await close_redis()
    close_rabbitmq()


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Fault-Tolerant Movie Ticketing System - Seat Reservation and RabbitMQ Payment Pipeline",
    lifespan=lifespan,
)

# Enable CORS for frontend / microservice cross-origin interaction
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from pathlib import Path
from fastapi.responses import JSONResponse, HTMLResponse

from app.routers.catalog import router as catalog_router
from app.routers.auth import router as auth_router
from app.routers.live import router as live_router

# Register route modules (support both /seats and /api/seats for full backwards compatibility)
app.include_router(seats_router)
app.include_router(seats_router, prefix="/api")
app.include_router(payments_router)
app.include_router(payments_router, prefix="/api")
app.include_router(catalog_router)
app.include_router(auth_router)
app.include_router(live_router)

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"


@app.get("/", include_in_schema=False)
async def root_dashboard():
    """Serves the BookMyShow Bengaluru showcase interactive dashboard."""
    template_path = TEMPLATES_DIR / "index.html"
    if template_path.exists():
        return HTMLResponse(content=template_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>BookMyShow Bengaluru Showcase Ready</h1>")




@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint verifying application and Redis connectivity."""
    redis_healthy = False
    error_message = None
    try:
        client = await get_redis_client()
        pong = await client.ping()
        redis_healthy = bool(pong)
    except Exception as exc:
        error_message = str(exc)
        logger.warning(f"Health check Redis ping failed: {exc}")

    status_code = status.HTTP_200_OK if redis_healthy else status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(
        status_code=status_code,
        content={
            "status": "healthy" if redis_healthy else "degraded",
            "redis_connected": redis_healthy,
            "error": error_message,
            "app_version": settings.APP_VERSION,
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
