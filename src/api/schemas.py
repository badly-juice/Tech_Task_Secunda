"""Pydantic-схемы API (Pydantic v2)"""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, HttpUrl

from src.db.models import Currency, PaymentStatus


class PaymentCreateRequest(BaseModel):

    amount: Annotated[Decimal, Field(gt=0, max_digits=12, decimal_places=2)]
    currency: Currency
    description: Annotated[str | None, Field(max_length=500)] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentCreatedResponse(BaseModel):

    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: Annotated[
        dict[str, Any],
        Field(validation_alias=AliasChoices("metadata_", "metadata")),
    ]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
