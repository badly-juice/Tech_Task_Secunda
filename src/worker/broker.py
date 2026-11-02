"""RabbitMQ: топология и фабрика брокера FastStream.

Топологию объявляют и outbox relay, и consumer — декларации идемпотентны,
порядок старта сервисов не важен: durable-очередь создаётся первым
поднявшимся сервисом и принимает сообщения, даже если consumer ещё не
запущен. Routing key очереди совпадает с её именем (дефолт RabbitQueue).

Retry-механика (см. build_retry_queues): consumer сам решает судьбу сообщения
и не делает nack (иначе RabbitMQ вернул бы сообщение мгновенно — горячий
цикл). Неудавшаяся попытка публикуется в TTL-очередь; когда TTL истекает,
брокер через x-dead-letter-exchange возвращает сообщение в payments.new.
Исчерпавшая попытки — в payments.dlq.
"""

from faststream.rabbit import (
    Channel,
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from src.core.config import get_settings

PAYMENTS_EXCHANGE = "payments"
PAYMENTS_NEW_QUEUE = "payments.new"

DEAD_LETTER_QUEUE = "payments.dlq"
RETRY_QUEUE_PREFIX = "payments.retry"
RETRY_COUNT_HEADER = "x-retry-count"


payments_exchange = RabbitExchange(
    PAYMENTS_EXCHANGE,
    type=ExchangeType.DIRECT,
    durable=True,
)
payments_new_queue = RabbitQueue(
    PAYMENTS_NEW_QUEUE,
    durable=True,
)
dead_letter_queue = RabbitQueue(
    DEAD_LETTER_QUEUE,
    durable=True,
)


def build_retry_queues(
    base_delay: float, multiplier: float, max_attempts: int
) -> dict[int, RabbitQueue]:
    """TTL-очереди задержки между попытками.

    Ключ — номер неудавшейся попытки; TTL очереди = задержка перед следующей
    попыткой = base_delay * multiplier ** (failed_attempt - 1). По истечении
    TTL брокер сам возвращает сообщение в payments.new, consumer не занят
    ожиданием. Имя очереди содержит задержку, например payments.retry.1000ms.
    """
    queues: dict[int, RabbitQueue] = {}
    for failed_attempt in range(1, max_attempts):
        delay_ms = int(base_delay * multiplier ** (failed_attempt - 1) * 1000)
        queues[failed_attempt] = RabbitQueue(
            f"{RETRY_QUEUE_PREFIX}.{delay_ms}ms",
            durable=True,
            arguments={
                "x-message-ttl": delay_ms,
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
                "x-dead-letter-routing-key": PAYMENTS_NEW_QUEUE,
            },
        )
    return queues


def create_broker() -> RabbitBroker:
    # prefetch_count=1: consumer обрабатывает строго одно сообщение за раз
    # («один consumer» по ТЗ); publisher_confirms=True (дефолт) — публикация
    # ждёт подтверждения брокера.
    return RabbitBroker(
        get_settings().rabbitmq_url,
        default_channel=Channel(prefetch_count=1),
    )
