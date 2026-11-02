"""Outbox relay: публикация событий из БД в RabbitMQ.

Слой сервисов не знает о FastStream — функция публикации передаётся снаружи
(в воркере это `publisher.publish` брокера). Публикация и пометка события
processed — разные операции: crash между ними безопасен, сообщение будет
опубликовано повторно (at-least-once).
"""

from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from src.db.models import OutboxStatus
from src.db.repositories import OutboxRepository
from src.db.session import AsyncSession

logger = structlog.get_logger(__name__)

# Сообщение в RabbitMQ: конверт события из outbox.
# dict[str, Any] -> {"event_type": str, "payload": dict}
PublishFn = Callable[[dict[str, Any]], Awaitable[None]]


class OutboxPublisher:
    def __init__(self, session: AsyncSession, publish: PublishFn) -> None:
        self._session = session
        self._publish = publish
        self._outbox = OutboxRepository(session)

    async def publish_pending(self, batch_size: int) -> int:
        """Публикует пачку pending-событий, успешные помечает processed.

        Возвращает число опубликованных. Событие с ошибкой публикации остаётся
        pending и будет взято следующей итерацией. Коммит один на пачку —
        блокировки FOR UPDATE держатся недолго.
        """
        events = await self._outbox.fetch_pending(batch_size)
        published = 0
        for event in events:
            message = {"event_type": event.event_type, "payload": event.payload}
            try:
                await self._publish(message)
            except Exception:
                # mandatory=True: если очередь не существует, публикация упадёт,
                # событие останется pending и не потеряется.
                logger.exception(
                    "outbox.publish_failed",
                    event_id=str(event.id),
                    event_type=event.event_type,
                )
                continue
            event.status = OutboxStatus.processed
            published += 1

        await self._session.commit()
        if events:
            logger.info(
                "outbox.batch_published",
                total=len(events),
                published=published,
                failed=len(events) - published,
            )
        return published
