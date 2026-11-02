"""Точка входа HTTP API: uvicorn src.main:app --host 0.0.0.0 --port 8000"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import Depends, FastAPI

from src.api.payments import router as payments_router
from src.core.config import get_settings
from src.core.logging import setup_logging
from src.core.security import verify_api_key
from src.db.session import engine

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield
    await engine.dispose()
    logger.info("api.shutdown")


def create_app() -> FastAPI:
    setup_logging()
    settings = get_settings()

    app = FastAPI(
        title="Payment Processing Service",
        version="1.0.0",
        description="Асинхронный сервис процессинга платежей: "
        "создание платежа → RabbitMQ → consumer → webhook.",
        lifespan=lifespan,
    )
    app.include_router(
        payments_router,
        prefix="/api/v1",
        dependencies=[Depends(verify_api_key)],
    )

    @app.get("/health", tags=["health"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    logger.info("api.initialized", app_name=settings.app_name)
    return app


app = create_app()
