"""Отправка webhook-уведомлений о результате обработки платежа."""

import httpx
import structlog

from src.db.models import Payment

logger = structlog.get_logger(__name__)


class WebhookSender:
    def __init__(self, timeout: float) -> None:
        self._timeout = timeout

    async def send(self, payment: Payment) -> None:
        """POST JSON-уведомления на payment.webhook_url.

        Успех — любой 2xx; любой другой статус или сетевая ошибка — исключение:
        сообщение вернётся через retry-очередь, и при повторной доставке
        отправка повторится (шлюз заново не запустится — статус уже
        терминальный).
        """
        payload = {
            "event": "payment.processed",
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "idempotency_key": payment.idempotency_key,
            "created_at": payment.created_at.isoformat(),
            "processed_at": payment.processed_at.isoformat()
            if payment.processed_at
            else None,
        }
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(payment.webhook_url, json=payload)
            response.raise_for_status()
        logger.info(
            "webhook.delivered",
            payment_id=str(payment.id),
            webhook_status_code=response.status_code,
        )
