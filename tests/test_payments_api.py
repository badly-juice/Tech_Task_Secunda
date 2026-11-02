"""API-тесты: создание, идемпотентность, получение платежа."""

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select

from src.db.models import Outbox, Payment
from src.db.session import session_factory

API_KEY = "change-me-super-secret-key"
AUTH = {"X-API-Key": API_KEY}


def payment_payload(**overrides) -> dict:
    payload: dict = {
        "amount": "99.90",
        "currency": "RUB",
        "description": "test payment",
        "metadata": {"order_id": "123"},
        "webhook_url": "http://webhook.test/hook",
    }
    payload.update(overrides)
    return payload


async def _payment_rows() -> list[Payment]:
    async with session_factory() as session:
        return list((await session.scalars(select(Payment))).all())


async def _outbox_rows() -> list[Outbox]:
    async with session_factory() as session:
        return list((await session.scalars(select(Outbox))).all())


async def test_create_payment_returns_202_and_persists(client):
    resp = await client.post(
        "/api/v1/payments",
        json=payment_payload(),
        headers={**AUTH, "Idempotency-Key": "t-create-1"},
    )

    assert resp.status_code == 202
    body = resp.json()
    assert uuid.UUID(body["payment_id"])
    assert body["status"] == "pending"

    payments = await _payment_rows()
    assert len(payments) == 1
    payment = payments[0]
    assert payment.amount == Decimal("99.90")
    assert payment.currency.value == "RUB"
    assert payment.status.value == "pending"
    assert payment.idempotency_key == "t-create-1"
    assert payment.metadata_ == {"order_id": "123"}
    assert payment.processed_at is None

    # Платёж и событие outbox записаны в одной транзакции
    outbox = await _outbox_rows()
    assert len(outbox) == 1
    assert outbox[0].event_type == "payment.created"
    assert outbox[0].payload == {"payment_id": body["payment_id"]}
    assert outbox[0].status.value == "pending"


async def test_idempotent_replay_returns_same_payment(client):
    headers = {**AUTH, "Idempotency-Key": "t-idem-1"}
    first = await client.post("/api/v1/payments", json=payment_payload(), headers=headers)
    second = await client.post("/api/v1/payments", json=payment_payload(), headers=headers)

    assert first.status_code == 202
    assert second.status_code == 200
    assert second.json() == first.json()
    assert len(await _payment_rows()) == 1
    # Повторный запрос не создаёт второе событие в outbox
    assert len(await _outbox_rows()) == 1


async def test_create_payment_requires_idempotency_key(client):
    resp = await client.post("/api/v1/payments", json=payment_payload(), headers=AUTH)
    assert resp.status_code == 422


async def test_create_payment_rejects_blank_key(client):
    resp = await client.post(
        "/api/v1/payments",
        json=payment_payload(),
        headers={**AUTH, "Idempotency-Key": "   "},
    )
    assert resp.status_code == 400


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": "0"},
        {"amount": "-5.00"},
        {"amount": "1.999"},
        {"currency": "GBP"},
        {"webhook_url": "not-a-url"},
    ],
)
async def test_create_payment_validates_payload(client, overrides):
    resp = await client.post(
        "/api/v1/payments",
        json=payment_payload(**overrides),
        headers={**AUTH, "Idempotency-Key": "t-invalid"},
    )
    assert resp.status_code == 422
    assert len(await _payment_rows()) == 0


async def test_api_requires_valid_key(client):
    body = payment_payload()
    no_key = await client.post(
        "/api/v1/payments", json=body, headers={"Idempotency-Key": "t-auth-1"}
    )
    wrong_key = await client.post(
        "/api/v1/payments",
        json=body,
        headers={"X-API-Key": "wrong", "Idempotency-Key": "t-auth-2"},
    )
    assert no_key.status_code == 401
    assert wrong_key.status_code == 401


async def test_get_payment_returns_details(client):
    created = await client.post(
        "/api/v1/payments",
        json=payment_payload(),
        headers={**AUTH, "Idempotency-Key": "t-get-1"},
    )
    payment_id = created.json()["payment_id"]

    resp = await client.get(f"/api/v1/payments/{payment_id}", headers=AUTH)

    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == payment_id
    assert data["amount"] == "99.90"
    assert data["currency"] == "RUB"
    assert data["status"] == "pending"
    assert data["description"] == "test payment"
    assert data["metadata"] == {"order_id": "123"}
    assert data["idempotency_key"] == "t-get-1"
    assert data["webhook_url"] == "http://webhook.test/hook"
    assert data["created_at"] is not None
    assert data["processed_at"] is None


async def test_get_unknown_payment_returns_404(client):
    resp = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers=AUTH)
    assert resp.status_code == 404
