"""Репозитории: инкапсуляция запросов к БД."""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.models import Outbox, OutboxStatus, Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return await self._session.scalar(stmt)

    def add(self, payment: Payment) -> None:
        self._session.add(payment)


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event: Outbox) -> None:
        self._session.add(event)

    async def fetch_pending(self, limit: int) -> list[Outbox]:
        """Берёт необработанные события с построчной блокировкой.

        FOR UPDATE SKIP LOCKED: несколько экземпляров relay не захватят одни
        и те же строки — каждый получит свой непересекающийся набор, двойной
        публикации не будет.
        """
        stmt = (
            select(Outbox)
            .where(Outbox.status == OutboxStatus.pending)
            .order_by(Outbox.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self._session.scalars(stmt)).all())
