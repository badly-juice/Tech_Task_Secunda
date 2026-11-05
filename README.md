# Payment Processing Service

Асинхронный микросервис процессинга платежей (тестовое задание): принимает
запросы на оплату, обрабатывает их через эмуляцию внешнего платёжного шлюза
и уведомляет клиента о результате через webhook.

**Стек:** FastAPI + Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL 18,
RabbitMQ 4 (FastStream), Alembic, Docker Compose, structlog, httpx, pytest.


## Демонстрационный стенд

Развёрнутая версия сервиса доступна по постоянной ссылке, его можно открыть
в браузере и проверить вручную без локального запуска: создать платёж,
запросить его статус, проследить доставку webhook'а.

| Ресурс | Адрес |
|---|---|
| Swagger UI | https://tech-task-secunda.boxd.sh/docs |
| ReDoc | https://tech-task-secunda.boxd.sh/redoc |
| OpenAPI-схема | https://tech-task-secunda.boxd.sh/openapi.json |
| Health check | https://tech-task-secunda.boxd.sh/health |
| RabbitMQ Management | https://rabbitmq.tech-task-secunda.boxd.sh (guest / guest) |
| Демо-приёмник webhook'ов | https://webhooks.tech-task-secunda.boxd.sh |

Все запросы к API, как и при локальном запуске, требуют заголовок
`X-API-Key` (см. раздел «Аутентификация»). Для ручной проверки удобнее всего
использовать Swagger UI: он позволяет выполнять запросы прямо из браузера.


## Запуск

```bash
docker compose --profile demo up -d --build
```

Миграции применяются one-shot контейнером `migrate` до старта остальных
сервисов (`service_completed_successfully`), `alembic upgrade head`
идемпотентен.

| Сервис | Назначение | Порт (host) |
|---|---|---|
| `api` | HTTP API (uvicorn) | 8000 |
| `consumer` | обработка платежей (FastStream) | — |
| `outbox_worker` | публикация событий outbox → RabbitMQ | — |
| `postgres` | БД (healthcheck) | 5432 |
| `rabbitmq` | брокер (healthcheck; management UI guest/guest) | 5672, 15672 |
| `webhook_receiver` | демо-приёмник webhook'ов (профиль `demo`) | 8001 |

- Swagger UI: http://localhost:8000/docs
- RabbitMQ Management: http://localhost:15672 (guest / guest)
- Демо-приёмник webhook'ов: http://localhost:8001

## Аутентификация

Все эндпоинты требуют заголовок **`X-API-Key`** со статическим ключом из
переменной окружения `API_KEY` (по умолчанию `change-me-super-secret-key`).
Проверка оформлена как FastAPI-зависимость на уровне роутера (новый эндпоинт
невозможно добавить без авторизации).

## API

### Создание платежа — 202 Accepted

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me-super-secret-key" \
  -H "Idempotency-Key: example-001" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "99.90",
    "currency": "RUB",
    "description": "Оплата заказа №42",
    "metadata": {"order_id": "42"},
    "webhook_url": "http://webhook_receiver:8000/webhook/ok"
  }'
```

```json
{"payment_id":"8eb93900-...","status":"pending","created_at":"..."}
```

Платёж обрабатывается асинхронно (2–5 сек, 90% успех), результат приходит
webhook'ом на `webhook_url`.

Повторный запрос с тем же `Idempotency-Key` возвращает существующий платёж
(**200** вместо 202) — дубликат не создаётся.

### Получение платежа

```bash
curl -s http://localhost:8000/api/v1/payments/{payment_id} \
  -H "X-API-Key: change-me-super-secret-key"
```

Коды ошибок: неверный/отсутствующий `X-API-Key` → `401`; отсутствующий или
пустой `Idempotency-Key` → `422`/`400`; неизвестный `payment_id` → `404`;
нарушение валидации body → `422`.

## Демонстрация retry и DLQ

Webhook на `/webhook/fail` демо-приёмника всегда отвечает `500`:

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: change-me-super-secret-key" \
  -H "Idempotency-Key: example-dlq" \
  -H "Content-Type: application/json" \
  -d '{"amount": "10.00", "currency": "EUR", "webhook_url": "http://webhook_receiver:8000/webhook/fail"}'
```

В логах consumer'а: две попытки с задержками (1с, 2с — через TTL-очереди),
затем `consumer.dead_lettered`. В `payments.dlq` появится сообщение
с заголовками `x-retry-count` и `x-last-error`; обратно в обработку его можно
вернуть повторной публикацией в `payments.new`. Платёж при этом остаётся
`succeeded` — шлюз провёл оплату, не доставилось только уведомление.

```bash
docker compose logs -f consumer
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

## Архитектура

```
                    ┌───────────────┐   1 транзакция   ┌──────────────┐
  POST /payments ─► │ api (FastAPI) │  ─────────────►  │  PostgreSQL  │
                    └───────────────┘  payments+outbox └──────────────┘
                                                          │        ▲ 
                                                          │        │
                                               SELECT ... FOR UPDATE SKIP LOCKED
                                                          │        │
                                                          ▼        │
                        ┌───────────────┐   publish    ┌───────────────┐
                        │ outbox_worker │ ───────────► │   RabbitMQ    │
                        └───────────────┘              │ payments.new  │
                                                       └───────────────┘
                                                               │
                                                               ▼                                               
                        ┌──────────────┐               ┌───────────────┐
                        │  PostgreSQL  │ ◄──────────── │   consumer    │
                        │ status update│   2.статус    │ (FastStream)  │
                        └──────────────┘               │  1. шлюз 2-5с │
                                                       │  3. webhook ──┼──► webhook_url
                                                       └──────┬────────┘
                                  retry: TTL-очереди ─► DLQ ◄─┘ (после 3 попыток)
```

**Гарантии доставки**

- **Outbox pattern.** Событие `payment.created` пишется в одной транзакции
  с платежом; relay (`outbox_worker`) публикует его с `FOR UPDATE SKIP LOCKED`
  (масштабирование без двойной публикации), `mandatory=True` и publisher
  confirms; `processed` — только после успешной публикации. Семантика —
  at-least-once, отказ брокера не блокирует приём платежей.
- **Идемпотентность API.** Unique-констрейнт на `idempotency_key` — решающий
  слой; гонка параллельных запросов закрывается перехватом `IntegrityError`
  (проигравший возвращает запись победителя).
- **Идемпотентность consumer.** Повторная доставка не перезапускает шлюз:
  терминальный статус → повторяется только webhook.
- **Retry/DLQ.** RabbitMQ не умеет отложенный requeue, поэтому — каноничные
  TTL-очереди (`payments.retry.<задержка>ms`): по истечении TTL брокер через
  `x-dead-letter-exchange` возвращает сообщение в `payments.new`. Попытки
  считаются в заголовке `x-retry-count`; после 3 попыток — в `payments.dlq`
  с диагностикой. Consumer не делает nack (иначе — мгновенный requeue и
  горячий цикл): судьбу сообщения определяет явный код.

**Компромиссы (осознанные для объёма тестового задания):**

1. Webhook-доставка привязана к сообщению: если webhook не доставлен за
   3 попытки, платёж остаётся в статусе `succeeded`/`failed`, а сообщение
   лежит в DLQ до ручного вмешательства. В проде я бы вынес доставку
   webhook в отдельную сущность (`webhook_deliveries`) со своей очередью
   и независимыми ретраями — статус платежа не должен зависеть от
   доступности клиента.
2. Повторный запрос с тем же `Idempotency-Key`, но другим телом возвращает
   существующий платёж (как договорено в ТЗ: 200/202), без сравнения тела.
   Строгий вариант — сверять хеш body и отвечать `409` при расхождении.
3. Битые (нераспарсиваемые) сообщения и сообщения о несуществующих платежах
   не ретраятся и не попадают в DLQ — логируются с уровнем error и ack'аются.
4. HMAC-подпись webhook'а не реализована (ТЗ не требует); в проде добавил бы
   заголовок `X-Payment-Signature` с HMAC-SHA256 от body.

**Технические заметки** (грабли, на которые наступил при реализации):

- `metadata` — зарезервированный атрибут Declarative Base в SQLAlchemy:
  атрибут модели называется `metadata_`, колонка в БД — `metadata`
  (в Pydantic-схеме поле `metadata` читается из ORM через `AliasChoices`).
- `str(URL)` в SQLAlchemy **маскирует пароль** звёздочками: DSN собирается
  через `URL.render_as_string(hide_password=False)`, иначе аутентификация
  ломается с паролем-литерой `***`.
- `RabbitMessage.decode()` в FastStream — async: без `await` consumer молча
  дропал все сообщения.
- `postgres:18` меняет layout томов: монтировать `/var/lib/postgresql`,
  а не `/var/lib/postgresql/data`.
- На Windows `configparser` читает `alembic.ini` в locale-кодировке — файл
  держим ASCII-only.

## Конфигурация

Всё через переменные окружения (pydantic-settings), значения по умолчанию —
в коде, шаблон — [`.env.example`](.env.example), секреты в репозиторий не
попадают. Ключевые переменные:

| Переменная | По умолчанию | Описание |
|---|---|---|
| `API_KEY` | `change-me-super-secret-key` | ключ для `X-API-Key` |
| `DATABASE_URL` | — | готовый DSN; иначе собирается из `POSTGRES_*` |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` | DSN брокера |
| `OUTBOX_POLL_INTERVAL` / `OUTBOX_BATCH_SIZE` | `1.0` / `100` | параметры relay |
| `PROCESSING_TIME_MIN/MAX` | `2.0` / `5.0` | длительность эмуляции шлюза, сек |
| `PROCESSING_SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `WEBHOOK_TIMEOUT` | `10.0` | таймаут webhook, сек |
| `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_DELAY` / `RETRY_DELAY_MULTIPLIER` | `3` / `1.0` / `2.0` | retry-политика |
| `LOG_JSON` / `LOG_LEVEL` | `false` / `INFO` | логирование |

## Тесты

```bash
docker compose up -d postgres          # тесты идут против реального PostgreSQL
venv/Scripts/pip install -r requirements-dev.txt
venv/Scripts/python -m pytest -v       # 17 passed
```

API-тесты — в отдельной БД `payments_test` с настоящими миграциями; брокер в
тестах не участвует (публикация и webhook — на фейках): покрытие — создание,
идемпотентность, валидация, авторизация, outbox relay, consumer, webhook.

## Локальный запуск без Docker (кроме инфраструктуры)

```bash
docker compose up -d postgres rabbitmq
python -m venv venv && venv/Scripts/pip install -r requirements.txt
# .env: POSTGRES_HOST=localhost, RABBITMQ_URL=amqp://guest:guest@localhost:5672/
venv/Scripts/python -m alembic upgrade head
venv/Scripts/python -m uvicorn src.main:app --port 8000   # api
venv/Scripts/python -m src.worker.outbox_worker           # relay
venv/Scripts/python -m src.worker.consumer                # consumer
```

## Структура проекта

```
payment-service/
├── src/
│   ├── api/            # роуты, Pydantic-схемы, зависимости
│   ├── core/           # конфиг (pydantic-settings), логгер (structlog), X-API-Key
│   ├── db/             # модели SQLAlchemy 2.0, сессии, репозитории
│   ├── services/       # бизнес-логика: идемпотентное создание платежа, outbox relay
│   ├── worker/         # FastStream: топология, consumer, webhook, outbox_worker
│   └── main.py         # точка входа HTTP API
├── alembic/            # миграции (async env)
├── tests/              # pytest против реального PostgreSQL
├── tools/              # демо-приёмник webhook'ов
├── Dockerfile          # multi-stage, non-root
├── docker-compose.yml  # postgres, rabbitmq, api, consumer, outbox_worker, migrate
└── requirements*.txt   # пиннутые версии (freeze)
```
