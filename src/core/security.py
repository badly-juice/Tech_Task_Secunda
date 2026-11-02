"""Авторизация: статический API-ключ в заголовке X-API-Key."""

import secrets

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from src.core.config import get_settings

# Security-схема для OpenAPI: в Swagger UI появляется кнопка Authorize,
# и введённый ключ автоматически подставляется во все запросы.
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str | None = Security(api_key_header)) -> None:

    settings = get_settings()
    key_is_valid = api_key is not None and secrets.compare_digest(
        api_key.encode("utf-8"), settings.api_key.encode("utf-8")
    )
    if not key_is_valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
