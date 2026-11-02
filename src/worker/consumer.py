"""Consumer платежей: python -m src.worker.consumer

Единый обработчик очереди payments.new:
  1. эмулирует платёжный шлюз (2-5 сек, 90% успех);
  2. обновляет статус платежа в БД;
  3. отправляет webhook на указанный URL.

Повторные попытки (обработка или webhook): сообщение публикуется в TTL-очередь
с экспоненциальной задержкой, брокер возвращает его в payments.new; после
исчерпания RETRY_MAX_ATTEMPTS — в payments.dlq. Обработчик не бросает
исключений наружу: иначе FastStream сделал бы nack + мгновенный requeue
(горячий цикл), поэтому судьбу сообщения он определяет сам.
"""

import asyncio
import random
import signal
import uuid
from datetime import UTC, datetime
from typing import Any

import structlog
from faststream.rabbit import RabbitBroker, RabbitMessage

from src.core.config import Settings, get_settings
from src.core.logging import setup_logging
from src.db.models import Payment, PaymentStatus
from src.db.repositories import PaymentRepository
from src.db.session import session_factory
from src.worker.broker import (
    RETRY_COUNT_HEADER,
    build_retry_queues,
    create_broker,
    dead_letter_queue,
    payments_exchange,
    payments_new_queue,
)
from src.worker.webhook import WebhookSender

logger = structlog.get_logger(__name__)

ERROR_HEADER = "x-last-error"
ERROR_HEADER_MAX_LENGTH = 500


class PaymentNotFound(Exception):
    """Платёж из события отсутствует в БД — повторы сообщения бессмысленны."""


def _attempt_of(message: RabbitMessage) -> int:
    """Номер текущей попытки: 0 для свежих сообщений, +1 за retry-заголовком."""
    raw = message.headers.get(RETRY_COUNT_HEADER, 0)
    try:
        return int(raw) + 1
    except (TypeError, ValueError):
        return 1


def _failure_headers(attempt: int, error: BaseException) -> dict[str, Any]:
    return {
        RETRY_COUNT_HEADER: attempt,
        ERROR_HEADER: f"{type(error).__name__}: {error}"[:ERROR_HEADER_MAX_LENGTH],
    }


class PaymentProcessor:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def emulate_gateway(self) -> PaymentStatus:
        """Эмуляция внешнего платёжного шлюза (тестовая заглушка по ТЗ)."""
        await asyncio.sleep(
            random.uniform(
                self._settings.processing_time_min, self._settings.processing_time_max
            )
        )
        if random.random() < self._settings.processing_success_rate:
            return PaymentStatus.succeeded
        return PaymentStatus.failed

    async def process(self, payment_id: uuid.UUID) -> Payment:
        """Проводит pending-платёж через «шлюз» и возвращает его состояние.

        Повторная доставка сообщения (retry webhook'а) шлюз не запускает:
        платёж уже в терминальном статусе — возвращается как есть.
        """
        async with session_factory() as session:
            payment = await PaymentRepository(session).get(payment_id)
            if payment is None:
                raise PaymentNotFound(str(payment_id))
            if payment.status is PaymentStatus.pending:
                payment.status = await self.emulate_gateway()
                payment.processed_at = datetime.now(UTC)
                await session.commit()
                logger.info(
                    "payment.processed",
                    payment_id=str(payment.id),
                    status=payment.status.value,
                )
            return payment


settings = get_settings()
processor = PaymentProcessor(settings)
webhook_sender = WebhookSender(settings.webhook_timeout)
retry_queues = build_retry_queues(
    settings.retry_base_delay,
    settings.retry_delay_multiplier,
    settings.retry_max_attempts,
)
broker = create_broker()


async def _retry_or_dead_letter(
    body: dict[str, Any], attempt: int, payment_id: str, error: BaseException
) -> None:
    """Публикует сообщение в TTL-очередь следующей попытки или в DLQ."""
    retry_queue = retry_queues.get(attempt)
    if retry_queue is not None:
        logger.warning(
            "consumer.retry_scheduled",
            payment_id=payment_id,
            attempt=attempt,
            next_attempt=attempt + 1,
            retry_queue=retry_queue.name,
            error=str(error),
        )
        await broker.publish(
            body,
            queue=retry_queue,
            headers=_failure_headers(attempt, error),
        )
        return

    logger.error(
        "consumer.dead_lettered",
        payment_id=payment_id,
        attempt=attempt,
        queue=dead_letter_queue.name,
        error=str(error),
        exc_info=error,
    )
    await broker.publish(
        body,
        queue=dead_letter_queue,
        headers=_failure_headers(attempt, error),
    )


@broker.subscriber(payments_new_queue, exchange=payments_exchange)
async def handle_payment_created(message: RabbitMessage) -> None:
    attempt = _attempt_of(message)

    try:
        body = await message.decode()
        if not isinstance(body, dict):
            raise ValueError(f"Expected JSON object, got {type(body).__name__}")
        payment_id = str(body["payload"]["payment_id"])
        uuid.UUID(payment_id)
    except Exception:
        # Повторы не помогут: тело повреждено. Логируем и ack'аем.
        logger.exception(
            "consumer.unparsable_message",
            attempt=attempt,
            headers=message.headers,
            raw_body=message.body,
        )
        return

    try:
        payment = await processor.process(uuid.UUID(payment_id))
        await webhook_sender.send(payment)
    except PaymentNotFound as exc:
        logger.error("consumer.payment_not_found", payment_id=payment_id, attempt=attempt)
        return
    except Exception as exc:
        await _retry_or_dead_letter(body, attempt, payment_id, exc)
        return

    logger.info(
        "consumer.message_done",
        payment_id=payment_id,
        status=payment.status.value,
        attempt=attempt,
    )


async def run() -> None:
    setup_logging()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows: add_signal_handler недоступен
            pass

    await broker.connect()
    # Retry-очереди и DLQ объявляем до старта консюмера: к моменту первой
    # ошибки топология гарантированно существует.
    for queue in retry_queues.values():
        await broker.declare_queue(queue)
    await broker.declare_queue(dead_letter_queue)

    await broker.start()
    logger.info(
        "consumer.started",
        queue=payments_new_queue.name,
        retry_delays={attempt: q.name for attempt, q in retry_queues.items()},
        dlq=dead_letter_queue.name,
        max_attempts=settings.retry_max_attempts,
    )
    try:
        await stop.wait()
    finally:
        await broker.stop()
        logger.info("consumer.stopped")


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
