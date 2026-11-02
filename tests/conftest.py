"""Общие фикстуры тестов.

Тесты идут против реального PostgreSQL из docker-compose (порт 5432 на хосте)
в отдельной БД payments_test с настоящими Alembic-миграциями. RabbitMQ не
нужен: API пишет события в outbox-таблицу, а публикация и consumer
проверяются на фейковых функциях/транспортах.

"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path
from typing import AsyncIterator

import asyncpg
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

TEST_DATABASE_URL = "postgresql+asyncpg://payments:payments@localhost:5432/payments_test"
ADMIN_DSN = "postgresql://payments:payments@localhost:5432/postgres"


os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ.setdefault("API_KEY", "change-me-super-secret-key")


@pytest.fixture(scope="session", autouse=True)
def test_database() -> None:

    async def create_database() -> None:
        admin = await asyncpg.connect(ADMIN_DSN)
        try:
            exists = await admin.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = 'payments_test'"
            )
            if not exists:
                await admin.execute("CREATE DATABASE payments_test")
        finally:
            await admin.close()

    asyncio.run(create_database())

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        cwd=PROJECT_ROOT,
        env={**os.environ, "DATABASE_URL": TEST_DATABASE_URL},
    )


@pytest.fixture(autouse=True)
async def clean_database() -> AsyncIterator[None]:
    yield
    from sqlalchemy import text

    from src.db.session import engine

    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))


@pytest.fixture
async def client() -> AsyncIterator["httpx.AsyncClient"]:
    import httpx
    from httpx import ASGITransport

    from src.main import app

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c
