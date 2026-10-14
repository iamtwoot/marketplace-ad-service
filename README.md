# Ad Service

CRUD объявлений для маркетплейса. При создании, обновлении и удалении пишет событие в **outbox-таблицу** в той же транзакции; отдельный воркер (`make outbox`) разгребает outbox и шлёт тонкие события в Kafka. Search Service подписан на эти события и дёргает `/internal/ads/{id}`. Токены JWT валидируются локально общим `JWT_SECRET`.

## Стек

- Python 3.13, FastAPI, SQLAlchemy (async), PostgreSQL
- JWT (HS256) — валидация access-токенов (токены выдаёт Auth Service)
- Kafka (Redpanda локально) — события публикуются через Outbox + relay-воркер (`aiokafka`)
- Alembic — миграции
- uv — управление зависимостями

## Быстрый старт

```bash
# Зависимости
uv sync

# PostgreSQL + Redpanda
docker compose up -d

# Переменные окружения
cp .env.example .env  # или отредактировать .env

# Миграции
make migrate

# Запуск API
make run

# В соседнем терминале — relay, который шлёт события из outbox в Kafka
make outbox
```

Сервер стартует на `http://localhost:8002`.

## Переменные окружения

| Переменная                    | По умолчанию            | Описание                        |
|-------------------------------|-------------------------|---------------------------------|
| `POSTGRES_HOST`               | — (обязательная)        | Хост PostgreSQL                 |
| `POSTGRES_PORT`               | — (обязательная)        | Порт PostgreSQL                 |
| `POSTGRES_DATABASE_NAME`      | — (обязательная)        | Имя базы данных                 |
| `POSTGRES_USERNAME`           | — (обязательная)        | Пользователь PostgreSQL         |
| `POSTGRES_PASSWORD`           | — (обязательная)        | Пароль PostgreSQL               |
| `JWT_SECRET`                  | `change-me`             | Секрет для подписи JWT          |
| `JWT_ALGORITHM`               | `HS256`                 | Алгоритм подписи                |
| `KAFKA_BROKERS`               | `localhost:9092`        | Kafka-брокеры                   |
| `KAFKA_TOPIC_MARKETPLACE_ADS` | `ads`                   | Топик для событий объявлений    |
| `AUTH_SERVICE_URL`            | `http://localhost:8000` | URL Auth Service (имя автора)   |

## API

### Публичные эндпоинты (`/ads`)

| Метод    | Путь         | Описание                          | Auth   |
|----------|--------------|-----------------------------------|--------|
| `GET`    | `/ads`       | Список с пагинацией               | Нет    |
| `GET`    | `/ads/my`    | Мои объявления                    | Bearer |
| `GET`    | `/ads/{id}`  | Одно объявление                   | Нет    |
| `POST`   | `/ads`       | Создать объявление                | Bearer |
| `PUT`    | `/ads/{id}`  | Обновить (только автор)           | Bearer |
| `DELETE` | `/ads/{id}`  | Soft-delete (status=archived)     | Bearer |

### Внутренние эндпоинты (`/internal`)

Не должны быть доступны через Ingress — только для межсервисного общения.

| Метод    | Путь                  | Описание                                          | Auth |
|----------|-----------------------|---------------------------------------------------|------|
| `GET`    | `/internal/ads/{id}`  | Объявление в любом статусе (в т.ч. `archived`)    | Нет  |

## Kafka + Outbox

Publish-after-commit по наивному сценарию (`send_and_wait` после `uow.commit`) теряет события, если процесс падает между коммитом и отправкой. Чтобы этого не было, используется **Outbox-паттерн**:

1. Use case в одной транзакции пишет строку в таблицу `outbox` (`event_type`, `payload`) **и** изменение объявления — либо обе записи закоммитятся, либо ни одна.
2. Отдельный процесс `make outbox` крутит цикл:
   - `SELECT ... FROM outbox WHERE published_at IS NULL ... FOR UPDATE SKIP LOCKED LIMIT N`
   - шлёт каждое сообщение в Kafka
   - проставляет `published_at = now()`
3. `SKIP LOCKED` позволяет спокойно поднять несколько relay-инстансов.

В Kafka уходят тонкие события:

```json
{ "event": "ad.created", "payload": { "ad_id": 7 } }
{ "event": "ad.updated", "payload": { "ad_id": 7 } }
{ "event": "ad.deleted", "payload": { "ad_id": 7 } }
```

Потребитель (Search Service) подтягивает актуальные данные объявления через `GET /internal/ads/{id}`.

## Soft delete

`DELETE /ads/{id}` переводит объявление в статус `archived` — строка остаётся в БД. С точки зрения публичного API объявление пропадает (404 в `GET /ads/{id}`, отсутствует в `/ads` и `/ads/my`), но доступно через `/internal/ads/{id}` — Search Service должен суметь получить финальное состояние для своего индекса.

## Make-команды

| Команда                          | Описание                          |
|----------------------------------|-----------------------------------|
| `make run`                       | Запуск сервера                    |
| `make outbox`                    | Воркер: outbox → Kafka            |
| `make seed`                      | Тестовые данные из `seed.json`    |
| `make check`                     | Линтинг + форматирование (ruff)   |
| `make test`                      | Запуск тестов                     |
| `make lint`                      | `ruff check --fix`                |
| `make format`                    | `ruff format`                     |
| `make migrate`                   | Применить миграции                |
| `make migrate-create name="..."` | Сгенерировать миграцию            |
