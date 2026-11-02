"""Outbox relay worker: python -m src.worker.outbox_worker

Раз в OUTBOX_POLL_INTERVAL секунд забирает пачку pending-событий из outbox
и публикует их в RabbitMQ. Полная пачка означает бэклог — следующая итерация
идёт без паузы, пока очередь событий не разгрузится.
"""

import asyncio
import signal

import structlog

from src.core.config import get_settings
from src.core.logging import setup_logging
from src.db.session import session_factory
from src.services.outbox import OutboxPublisher
from src.worker.broker import create_broker, payments_exchange, payments_new_queue

logger = structlog.get_logger(__name__)


async def run() -> None:
    setup_logging()
    settings = get_settings()

    broker = create_broker()
    # Публишер декларирует exchange + очередь + биндинг при входе в контекст:
    # публикация безопасна даже если consumer ещё не поднят.
    publisher = broker.publisher(payments_new_queue, exchange=payments_exchange)

    async with broker:
        logger.info(
            "outbox_worker.started",
            poll_interval=settings.outbox_poll_interval,
            batch_size=settings.outbox_batch_size,
        )
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:  # Windows: add_signal_handler недоступен
                pass

        while not stop.is_set():
            try:
                async with session_factory() as session:
                    published = await OutboxPublisher(
                        session, publisher.publish
                    ).publish_pending(settings.outbox_batch_size)
            except Exception:
                logger.exception("outbox_worker.iteration_failed")
                published = 0

            if published < settings.outbox_batch_size:
                try:
                    await asyncio.wait_for(
                        stop.wait(), timeout=settings.outbox_poll_interval
                    )
                except TimeoutError:
                    pass

        logger.info("outbox_worker.stopped")


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
