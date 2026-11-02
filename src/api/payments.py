"""HTTP-эндпоинты платежей"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Response, status

from src.api.deps import SessionDep
from src.api.schemas import (
    PaymentCreateRequest,
    PaymentCreatedResponse,
    PaymentResponse,
)
from src.services.payments import PaymentService

router = APIRouter(prefix="/payments", tags=["payments"])

IDEMPOTENCY_KEY_MAX_LENGTH = 255


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreatedResponse,
    summary="Создать платёж (асинхронная обработка)",
)
async def create_payment(
    data: PaymentCreateRequest,
    response: Response,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    session: SessionDep,
) -> PaymentCreatedResponse:
    """Принимает платёж в обработку и сразу возвращает 202 Accepted.

    Повторный запрос с тем же Idempotency-Key не создаёт дубликат:
    возвращается существующий платёж со статусом 200 OK.
    """
    key = idempotency_key.strip()
    if not key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Idempotency-Key must not be blank",
        )
    if len(key) > IDEMPOTENCY_KEY_MAX_LENGTH:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Idempotency-Key must not exceed {IDEMPOTENCY_KEY_MAX_LENGTH} characters",
        )

    payment, created = await PaymentService(session).create_payment(data, key)
    if not created:
        response.status_code = status.HTTP_200_OK
    return PaymentCreatedResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    summary="Получить детальную информацию о платеже",
)
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentResponse:
    payment = await PaymentService(session).get_payment(payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentResponse.model_validate(payment)
