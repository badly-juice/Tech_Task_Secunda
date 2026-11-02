"""Бизнес-логика платежей: создание с идемпотентностью и записью в outbox."""

import uuid

import structlog
from sqlalchemy.exc import IntegrityError

from src.api.schemas import PaymentCreateRequest
from src.db.models import Outbox, Payment, PaymentStatus
from src.db.repositories import OutboxRepository, PaymentRepository
from src.db.session import AsyncSession

logger = structlog.get_logger(__name__)

EVENT_PAYMENT_CREATED = "payment.created"


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create_payment(
        self, data: PaymentCreateRequest, idempotency_key: str
    ) -> tuple[Payment, bool]:
        """Создаёт платёж и событие outbox в одной транзакции.

        Возвращает (платёж, created). Если платёж с таким Idempotency-Key уже
        существует — возвращает его (created=False), в том числе при гонке
        параллельных запросов: проигравший IntegrityError от unique-констрейнта
        читает запись победителя.
        """
        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            logger.info(
                "payment.idempotent_replay",
                payment_id=str(existing.id),
                idempotency_key=idempotency_key,
            )
            return existing, False

        payment = Payment(
            id=uuid.uuid4(),
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            idempotency_key=idempotency_key,
            webhook_url=str(data.webhook_url),
        )
        # Минимальный payload: consumer читает актуальное состояние из БД,
        # БД — единственный источник правды.
        event = Outbox(
            id=uuid.uuid4(),
            event_type=EVENT_PAYMENT_CREATED,
            payload={"payment_id": str(payment.id)},
        )
        self._payments.add(payment)
        self._outbox.add(event)

        try:
            await self._session.commit()
        except IntegrityError:
            await self._session.rollback()
            existing = await self._payments.get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            logger.info(
                "payment.idempotent_race_lost",
                payment_id=str(existing.id),
                idempotency_key=idempotency_key,
            )
            return existing, False

        logger.info(
            "payment.created",
            payment_id=str(payment.id),
            amount=str(payment.amount),
            currency=payment.currency.value,
        )
        return payment, True

    async def get_payment(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._payments.get(payment_id)
