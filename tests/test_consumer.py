"""Consumer и webhook: бизнес-логика без реального RabbitMQ."""

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from src.core.config import get_settings
from src.db.models import Currency, Payment, PaymentStatus
from src.db.session import session_factory
from src.worker.consumer import PaymentProcessor
from src.worker.webhook import WebhookSender


async def _insert_payment(idempotency_key: str) -> uuid.UUID:
    async with session_factory() as session:
        payment = Payment(
            id=uuid.uuid4(),
            amount=Decimal("10.00"),
            currency=Currency.USD,
            metadata_={},
            idempotency_key=idempotency_key,
            webhook_url="http://webhook.test/hook",
        )
        session.add(payment)
        await session.commit()
        return payment.id


def _terminal_payment() -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        metadata_={},
        status=PaymentStatus.succeeded,
        idempotency_key="t-webhook-1",
        webhook_url="http://webhook.test/hook",
        created_at=datetime.now(UTC),
        processed_at=datetime.now(UTC),
    )


async def test_process_runs_gateway_once(monkeypatch):
    payment_id = await _insert_payment("t-consumer-1")
    processor = PaymentProcessor(get_settings())

    async def gateway_succeeded() -> PaymentStatus:
        return PaymentStatus.succeeded

    monkeypatch.setattr(processor, "emulate_gateway", gateway_succeeded)
    payment = await processor.process(payment_id)

    assert payment.status is PaymentStatus.succeeded
    assert payment.processed_at is not None
    processed_at = payment.processed_at

    # Повторная доставка (retry webhook'а) не должна запускать шлюз заново
    async def gateway_must_not_run() -> PaymentStatus:
        raise AssertionError("gateway must not run for terminal payment")

    monkeypatch.setattr(processor, "emulate_gateway", gateway_must_not_run)
    redelivered = await processor.process(payment_id)
    assert redelivered.status is PaymentStatus.succeeded
    assert redelivered.processed_at == processed_at


async def test_webhook_sender_posts_notification(monkeypatch):
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["json"] = json.loads(request.content)
        return httpx.Response(200)

    real_client_factory = httpx.AsyncClient

    def client_with_mock_transport(**kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client_factory(**kwargs)

    monkeypatch.setattr(
        "src.worker.webhook.httpx.AsyncClient", client_with_mock_transport
    )

    payment = _terminal_payment()
    await WebhookSender(timeout=5.0).send(payment)

    assert captured["url"] == "http://webhook.test/hook"
    body = captured["json"]
    assert body["event"] == "payment.processed"
    assert body["payment_id"] == str(payment.id)
    assert body["status"] == "succeeded"
    assert body["amount"] == "10.00"
    assert body["currency"] == "USD"
    assert body["idempotency_key"] == "t-webhook-1"


async def test_webhook_sender_raises_on_error_response(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    real_client_factory = httpx.AsyncClient

    def client_with_mock_transport(**kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client_factory(**kwargs)

    monkeypatch.setattr(
        "src.worker.webhook.httpx.AsyncClient", client_with_mock_transport
    )

    with pytest.raises(httpx.HTTPStatusError):
        await WebhookSender(timeout=5.0).send(_terminal_payment())
