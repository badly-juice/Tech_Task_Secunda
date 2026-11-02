"""Outbox relay: публикация событий (RabbitMQ замокан фейковой функцией)."""

import uuid

from sqlalchemy import select

from src.api.schemas import PaymentCreateRequest
from src.db.models import Outbox, OutboxStatus
from src.db.session import session_factory
from src.services.outbox import OutboxPublisher
from src.services.payments import PaymentService


def _payload() -> dict:
    return {
        "amount": "10.00",
        "currency": "USD",
        "description": "outbox test",
        "metadata": {},
        "webhook_url": "http://webhook.test/hook",
    }


async def _create_payment(idempotency_key: str) -> uuid.UUID:
    async with session_factory() as session:
        payment, _created = await PaymentService(session).create_payment(
            PaymentCreateRequest(**_payload()), idempotency_key
        )
        return payment.id


async def _outbox_rows() -> list[Outbox]:
    async with session_factory() as session:
        return list((await session.scalars(select(Outbox))).all())


async def test_publish_pending_publishes_and_marks_processed():
    payment_id = await _create_payment("t-outbox-1")
    published: list[dict] = []

    async def fake_publish(message: dict) -> None:
        published.append(message)

    async with session_factory() as session:
        published_count = await OutboxPublisher(session, fake_publish).publish_pending(10)

    assert published_count == 1
    assert published == [
        {"event_type": "payment.created", "payload": {"payment_id": str(payment_id)}}
    ]

    # Событие помечено processed: повторный прогон не публикует дубль
    async with session_factory() as session:
        again = await OutboxPublisher(session, fake_publish).publish_pending(10)
    assert again == 0
    assert len(published) == 1

    rows = await _outbox_rows()
    assert rows[0].status is OutboxStatus.processed


async def test_failed_publish_keeps_event_pending():
    await _create_payment("t-outbox-2")

    async def failing_publish(message: dict) -> None:
        raise RuntimeError("rabbitmq is down")

    async with session_factory() as session:
        published = await OutboxPublisher(session, failing_publish).publish_pending(10)

    assert published == 0
    rows = await _outbox_rows()
    assert rows[0].status is OutboxStatus.pending
