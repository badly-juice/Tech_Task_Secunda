"""Конфигурация приложения на pydantic-settings.

Значения по умолчанию повторяют .env.example; в runtime всё переопределяется
переменными окружения (в Docker они задаются в docker-compose.yml).
"""

from functools import lru_cache
from typing import Annotated

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

PositiveFloat = Annotated[float, Field(gt=0)]
PositiveInt = Annotated[int, Field(gt=0)]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = "payment-service"
    log_level: str = "INFO"
    log_json: bool = False

    # HTTP API
    api_key: str = "change-me-super-secret-key"

    # PostgreSQL
    postgres_host: str = "postgres"
    postgres_port: Annotated[int, Field(ge=1, le=65535)] = 5432
    postgres_user: str = "payments"
    postgres_password: str = "payments"
    postgres_db: str = "payments"
    database_url: str | None = None

    # RabbitMQ
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"

    # Outbox relay
    outbox_poll_interval: PositiveFloat = 1.0
    outbox_batch_size: PositiveInt = 100

    # Эмуляция платёжного шлюза
    processing_time_min: PositiveFloat = 2.0
    processing_time_max: PositiveFloat = 5.0
    processing_success_rate: Annotated[float, Field(ge=0, le=1)] = 0.9

    # Webhook
    webhook_timeout: PositiveFloat = 10.0
    retry_max_attempts: Annotated[int, Field(ge=1)] = 3
    retry_base_delay: PositiveFloat = 1.0
    retry_delay_multiplier: Annotated[float, Field(gt=1)] = 2.0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def async_database_url(self) -> str:

        return self.database_url or URL.create(
            "postgresql+asyncpg",
            username=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        ).render_as_string(hide_password=False)


@lru_cache
def get_settings() -> Settings:
    return Settings()
