# HW3 — Flight Booking: gRPC + Redis

## Что было сделано

В этом домашнем задании я реализовал микросервисную систему бронирования авиабилетов из двух сервисов:

- **Booking Service** — REST API для клиентов
- **Flight Service** — gRPC сервис для работы с рейсами и местами

Архитектурно решение построено так:

```text
Client (REST) → Booking Service → (gRPC) → Flight Service
                      ↓                           ↓
                 PostgreSQL               PostgreSQL + Redis (Sentinel)
```

## ER-диаграмма

```mermaid
erDiagram
    FLIGHTS {
        bigserial   id               PK
        varchar     flight_number
        varchar     airline
        varchar     origin_iata
        varchar     destination_iata
        timestamptz departs_at
        timestamptz arrives_at
        int         total_seats
        int         available_seats
        numeric     ticket_price
        varchar     flight_status
    }

    SEAT_RESERVATIONS {
        bigserial   id          PK
        bigint      flight_id   FK
        uuid        booking_ref
        int         seats_held
        varchar     rsv_status
        timestamptz reserved_at
    }

    BOOKINGS {
        uuid        id              PK
        varchar     customer_id
        bigint      flight_id
        varchar     traveller_name
        varchar     traveller_email
        int         seat_count
        numeric     total_cost
        varchar     booking_status
        timestamptz created_at
    }

    FLIGHTS ||--o{ SEAT_RESERVATIONS : "reserves seats for"
```

ER-диаграмму я вынес в отдельный файл:

```text
hw3-flight-booking/er-diagram.mmd
```

## Структура проекта

```text
hw3-flight-booking/
├── booking-service/
│   ├── db.py
│   ├── grpc_client.py
│   ├── main.py
│   ├── flight.proto
│   ├── Dockerfile
│   ├── requirements.txt
│   └── migrations/V1__init.sql
├── flight-service/
│   ├── db.py
│   ├── main.py
│   ├── flight.proto
│   ├── Dockerfile
│   ├── requirements.txt
│   └── migrations/V1__init.sql
├── proto/
│   └── flight.proto
├── tests/
│   ├── conftest.py
│   ├── test_retry.py
│   └── requirements.txt
├── .gitignore
├── docker-compose.yml
├── er-diagram.mmd
└── README.md
```

## Что именно я реализовал по пунктам задания

## 1. Спроектировал доменную модель и ER-диаграмму

Сначала я выделил основные сущности системы:

- `flights` — рейсы
- `seat_reservations` — резерв мест на рейсе
- `bookings` — итоговые бронирования пользователя

Я разделил данные так, чтобы:
- Flight Service отвечал за рейсы и количество свободных мест
- Booking Service отвечал за пользовательские бронирования

Это позволило не смешивать ответственность сервисов и сделать модель ближе к реальной микросервисной архитектуре.

### Где это реализовано

ER-диаграмма:
```text
hw3-flight-booking/er-diagram.mmd
```

SQL-схема Flight Service:
```text
hw3-flight-booking/flight-service/migrations/V1__init.sql
```

SQL-схема Booking Service:
```text
hw3-flight-booking/booking-service/migrations/V1__init.sql
```

## 2. Поднял два отдельных сервиса и две отдельные базы данных

Я разнёс систему на два сервиса:

### Booking Service
REST API, которое принимает HTTP-запросы от клиента.

### Flight Service
gRPC сервис, который:
- ищет рейсы
- отдаёт рейс по ID
- резервирует места
- освобождает резерв

Также я поднял **две отдельные PostgreSQL базы**:
- `flights_db`
- `bookings_db`

Это сделано через `docker-compose.yml`.

### Где это реализовано

```text
hw3-flight-booking/docker-compose.yml
```

Сами сервисы:
```text
hw3-flight-booking/booking-service/
hw3-flight-booking/flight-service/
```

## 3. Описал gRPC контракт между сервисами

Для связи между сервисами я описал protobuf-контракт.

В контракт я вынес:
- сущность `Flight`
- сущность `SeatReservation`
- запросы и ответы для:
  - `SearchFlights`
  - `GetFlight`
  - `ReserveSeats`
  - `ReleaseReservation`

Это позволило сделать строго типизированное взаимодействие между сервисами.

### Где это реализовано

Основной proto-файл:
```text
hw3-flight-booking/proto/flight.proto
```

Копии proto для сборки контейнеров:
```text
hw3-flight-booking/booking-service/flight.proto
hw3-flight-booking/flight-service/flight.proto
```

## 4. Реализовал REST API в Booking Service

Booking Service я сделал на FastAPI.

Он предоставляет следующие endpoints:

- `GET /flights`
- `GET /flights/{flight_id}`
- `POST /bookings`
- `GET /bookings/{booking_id}`
- `POST /bookings/{booking_id}/cancel`
- `GET /bookings?customer_id=...`

Через эти endpoints клиент взаимодействует только с REST API, а дальше Booking Service уже сам общается с Flight Service через gRPC.

### Что важно

Я также добавил валидацию `booking_id`, чтобы при передаче некорректного UUID сервис возвращал **400 Bad Request**, а не падал с 500 ошибкой.

### Где это реализовано

```text
hw3-flight-booking/booking-service/main.py
```

Ключевое место:
- функция `_parse_uuid()`
- обработчики `/bookings/{booking_id}`
- обработчик `/bookings/{booking_id}/cancel`

## 5. Реализовал логику Flight Service и работу с местами

Flight Service я сделал как gRPC сервер.

Он обрабатывает:

### `SearchFlights`
Поиск рейсов по маршруту и дате.

### `GetFlight`
Получение конкретного рейса по ID.

### `ReserveSeats`
Атомарное резервирование мест.

### `ReleaseReservation`
Освобождение мест при отмене бронирования.

Для бронирования я использовал транзакцию и блокировку строки рейса через:

```sql
SELECT * FROM flights WHERE id = $1 FOR UPDATE
```

Это нужно, чтобы два параллельных запроса не смогли одновременно забрать одни и те же места.

### Где это реализовано

```text
hw3-flight-booking/flight-service/main.py
```

Ключевые места:
- метод `ReserveSeats`
- метод `ReleaseReservation`

## 6. Реализовал идемпотентность резервирования

Один из важных пунктов — сделать резервирование идемпотентным.

Я использовал `booking_ref` как идемпотентный ключ.

Перед созданием нового резерва Flight Service сначала проверяет, есть ли уже запись в `seat_reservations` с таким `booking_ref`. Если запись уже существует, сервис не уменьшает места повторно, а возвращает уже существующую резервацию.

Это защищает систему от повторного выполнения одного и того же запроса.

### Где это реализовано

```text
hw3-flight-booking/flight-service/main.py
```

Ключевое место:
- начало метода `ReserveSeats`
- проверка `existing = await conn.fetchrow(...)`

Также уникальность `booking_ref` поддерживается на уровне БД:

```text
hw3-flight-booking/flight-service/migrations/V1__init.sql
```

## 7. Добавил авторизацию между сервисами через API key

Чтобы запретить прямые вызовы Flight Service без внутреннего ключа, я добавил передачу `x-api-key` в gRPC metadata.

### Как это сделано

#### На стороне клиента
В Booking Service я реализовал `ApiKeyInterceptor`, который автоматически добавляет ключ ко всем gRPC вызовам.

#### На стороне сервера
Во Flight Service я реализовал `AuthInterceptor`, который проверяет `x-api-key` и отклоняет запросы без корректного значения.

### Где это реализовано

Клиентский interceptor:
```text
hw3-flight-booking/booking-service/grpc_client.py
```

Серверный interceptor:
```text
hw3-flight-booking/flight-service/main.py
```

## 8. Добавил retry с exponential backoff

Для временных сбоев сети и недоступности Flight Service я реализовал retry-логику в Booking Service.

Retry выполняется только для ошибок:

- `UNAVAILABLE`
- `DEADLINE_EXCEEDED`

Retry **не выполняется** для бизнес-ошибок:

- `NOT_FOUND`
- `RESOURCE_EXHAUSTED`
- `INVALID_ARGUMENT`

Схема backoff:
- 0.1 сек
- 0.2 сек
- 0.4 сек

### Где это реализовано

```text
hw3-flight-booking/booking-service/grpc_client.py
```

Ключевое место:
- функция `call_with_retry()`

### Тесты

Я отдельно написал тесты на retry:
- повтор при `UNAVAILABLE` и `DEADLINE_EXCEEDED`
- отсутствие retry при бизнес-ошибках
- успешный второй вызов
- проверка exponential backoff

Файлы:
```text
hw3-flight-booking/tests/test_retry.py
hw3-flight-booking/tests/conftest.py
```

## 9. Добавил Redis кеширование

Чтобы снизить нагрузку на БД Flight Service, я реализовал кеширование в Redis.

### Что кешируется
- результат `GetFlight`
- результат `SearchFlights`

### Подход
Я использовал стратегию **Cache-Aside**:
1. Сначала проверяю Redis
2. Если кеша нет — иду в PostgreSQL
3. Записываю результат в Redis с TTL
4. При изменениях инвалидирую связанный кеш

### TTL
```text
CACHE_TTL_SECONDS = 360
```

### Инвалидация
После `ReserveSeats` и `ReleaseReservation` я:
- удаляю кеш конкретного рейса (`flight:{id}`)
- удаляю все поисковые ключи (`search:*`) через SCAN

### Где это реализовано

```text
hw3-flight-booking/flight-service/main.py
```

Ключевые места:
- `get_redis()` / `redis_op()`
- `_evict_search_keys()`
- методы `SearchFlights`, `GetFlight`, `ReserveSeats`, `ReleaseReservation`

## 10. Добавил Redis Sentinel и поддержку failover

После обычного Redis я расширил решение до отказоустойчивой схемы:

- `redis-primary`
- `redis-secondary`
- `redis-sentinel`

Теперь Flight Service работает не с обычным Redis напрямую, а через Sentinel и получает master через имя:

```text
primary
```

### Что я сделал

В `docker-compose.yml` я добавил:
- контейнер primary
- контейнер secondary
- контейнер sentinel

Во Flight Service я добавил режим работы:

- `REDIS_MODE=standalone`
- `REDIS_MODE=sentinel`

Когда включен режим `sentinel`, сервис подключается через:

- `REDIS_SENTINEL_HOST`
- `REDIS_SENTINEL_PORT`
- `REDIS_MASTER_NAME`

### Где это реализовано

Compose:
```text
hw3-flight-booking/docker-compose.yml
```

Подключение через Sentinel:
```text
hw3-flight-booking/flight-service/main.py
```

Ключевые места:
- переменные `REDIS_MODE`, `REDIS_MASTER_NAME`, `REDIS_SENTINEL_HOST`
- ветка `if REDIS_MODE == "sentinel":`
- функция `_reset_redis()` — автопереподключение после failover

## 11. Реализовал Circuit Breaker

Чтобы Booking Service не продолжал бесконечно дёргать недоступный Flight Service, я реализовал **Circuit Breaker**.

Состояния:
- `CLOSED`
- `OPEN`
- `HALF_OPEN`

### Логика
- пока ошибок мало — состояние `CLOSED`
- если в окне накопилось достаточно неуспешных вызовов — `OPEN`
- после таймаута делается пробный запрос — `HALF_OPEN`
- если пробный запрос успешен — возвращаемся в `CLOSED`

### Параметры
Задаются через environment variables:

- `CB_FAILURE_THRESHOLD`
- `CB_RESET_TIMEOUT`
- `CB_WINDOW_SIZE`

### Где это реализовано

```text
hw3-flight-booking/booking-service/grpc_client.py
```

Ключевые места:
- класс `Breaker`
- исключение `CircuitOpenError`
- интеграция в `call_with_retry()`

И обработка в REST API:
```text
hw3-flight-booking/booking-service/main.py
```

## Как запускать проект

Из директории:

```text
hw3-flight-booking
```

Запуск:
```bash
docker compose up --build
```

## Как проверять

### Поиск рейсов

```bash
curl "http://localhost:8080/flights?origin=SVO&destination=LED"
curl "http://localhost:8080/flights?origin=SVO&destination=LED&date=2026-04-01"
```

### Получить рейс

```bash
curl http://localhost:8080/flights/1
```

### Создать бронирование

```bash
curl -X POST "http://localhost:8080/bookings" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id":     "user-42",
    "flight_id":       1,
    "traveller_name":  "Ivan Petrov",
    "traveller_email": "ivan@example.com",
    "seat_count":      2
  }'
```

### Получить бронирование

```bash
curl "http://localhost:8080/bookings/<BOOKING_ID>"
```

### Отменить бронирование

```bash
curl -X POST "http://localhost:8080/bookings/<BOOKING_ID>/cancel"
```

### Список бронирований

```bash
curl "http://localhost:8080/bookings?customer_id=user-42"
```

---

## Проверка retry и circuit breaker

Остановить Flight Service:
```bash
docker compose stop flight-service
```

Сделать несколько запросов:
```bash
curl http://localhost:8080/flights/1
```

Ожидаемое поведение:
- сначала идут retry
- потом circuit breaker открывается
- следующие запросы начинают возвращать `503 Service Unavailable`

После восстановления сервиса:
```bash
docker compose start flight-service
```

Circuit breaker должен перейти:
- `OPEN -> HALF_OPEN -> CLOSED`

## Проверка Redis Sentinel failover

Посмотреть master:
```bash
docker compose exec redis-sentinel redis-cli -p 26379 SENTINEL get-master-addr-by-name primary
```

Остановить primary:
```bash
docker compose stop redis-primary
```

Подождать несколько секунд и снова проверить:
```bash
docker compose exec redis-sentinel redis-cli -p 26379 SENTINEL get-master-addr-by-name primary
```

Ожидаемо Sentinel должен переключить master на secondary.

## Тесты

Запуск тестов:
```bash
cd tests
pip install -r requirements.txt
python -m pytest test_retry.py -v
```

У меня тесты покрывают retry-логику, backoff и все переходы состояний Circuit Breaker.

Гайд

Проверить в терминале:

docker --version

docker compose version

Ожидаемый вывод: Docker version 24.x.x и Docker Compose version v2.x.x. Если команда не найдена — установите Docker Desktop с сайта docker.com.

Терминал откроется автоматически в корневой папке проекта:

# Bash

pwd

# Windows PowerShell

Get-Location

D:\Stohastic Processes\VIdeos\Saiko Maksim HW1 SOA-Reference-Implementation\HW 3\hw3-flight-booking

Если путь неверный — перейти вручную:
  
  Windows PS:    cd C:\путь\до\hw3-flight-booking

Выполнить в терминале одну команду:

docker compose up --build

Что происходит в логах в порядке появления:

4.	flights-db и bookings-db — PostgreSQL 15 поднимается, healthcheck pg_isready проходит.

5.	redis-primary — Redis 7 master стартует с --appendonly yes.

6.	redis-secondary — Redis реплика подключается к primary через --replicaof redis-primary 6379.

7.	redis-sentinel — Sentinel генерирует конфиг на лету в shell-скрипте и начинает мониторить primary.

8.	migrate-flights и migrate-bookings — Flyway применяет V1__init.sql к каждой БД. В логах: "Successfully applied 1 migration".

9.	flight-service — gRPC-сервер стартует, подключается к Redis через Sentinel, пишет "Flight Service listening on 0.0.0.0:50051".

10.	booking-service — FastAPI стартует через Uvicorn, пишет "Booking Service ready on :8080".

Первый запуск: 3–7 минут (Docker скачивает python:3.11-slim, postgres:15, redis:7-alpine, flyway/flyway:9).
Повторные запуски без --build: 15–30 секунд.

Проверить статус контейнеров

Открыть второй терминальный таб (кнопка «+» рядом с вкладкой Terminal):

docker compose ps

Ожидаемый результат: все контейнеры со статусом running или Up. Контейнеры migrate-flights и migrate-bookings показывают Exited (0) — это нормально, они завершаются после применения миграций.

Посмотреть логи конкретного сервиса:

docker compose logs -f flight-service

docker compose logs -f booking-service

Booking Service слушает на порту 8080. Flight Service слушает на порту 50051 (gRPC, недоступен напрямую из браузера).

Поиск рейсов:

# Git Bash

curl "http://localhost:8080/flights?origin=SVO&destination=LED"

# Windows PowerShell

Invoke-RestMethod "http://localhost:8080/flights?origin=SVO&destination=LED"

Ожидаемый ответ: JSON-массив с двумя рейсами SU1001 и SU1002 из seed-данных миграции.

Поиск рейсов с датой:

curl "http://localhost:8080/flights?origin=SVO&destination=LED&date=2026-04-01"

Получить рейс по ID:

curl http://localhost:8080/flights/1

Создать бронирование:

$body = @{
    customer_id = "user-42"
    flight_id = 1
    traveller_name = "Ivan Petrov"
    traveller_email = "ivan@example.com"
    seat_count = 2
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8080/bookings" `
    -Method Post `
    -ContentType "application/json" `
    -Body $body


или

Invoke-RestMethod -Uri "http://localhost:8080/bookings" -Method Post -ContentType "application/json" -Body '{"customer_id":"user-42","flight_id":1,"traveller_name":"Ivan Petrov","traveller_email":"ivan@example.com","seat_count":2}'

(дальше копировать id, например "3f32ecfb-8e26-4406-8c2b-1a53fbf434b3")


curl "http://localhost:8080/bookings/3f32ecfb-8e26-4406-8c2b-1a53fbf434b3"

Invoke-RestMethod -Uri "http://localhost:8080/bookings/3f32ecfb-8e26-4406-8c2b-1a53fbf434b3/cancel" -Method Post


Получить бронирование:

curl "http://localhost:8080/bookings/<UUID>"

Отменить бронирование:

Invoke-RestMethod -Uri "http://localhost:8080/bookings/<UUID>/cancel" -Method Post

Список бронирований пользователя:

curl "http://localhost:8080/bookings?customer_id=user-42"


Проверить кеширование Redis

В логах flight-service (docker compose logs -f flight-service) должны появляться:

CACHE MISS search:SVO:LED:          <- первый запрос, идёт в БД

CACHE SET  search:SVO:LED: TTL=360s <- записано в Redis

CACHE HIT  search:SVO:LED:          <- повторный запрос, из Redis

После создания бронирования в логах появится:

CACHE HIT  flight:1

ReserveSeats: flight=1 seats=2 booking_ref=<UUID>


Проверить Circuit Breaker

11.	Остановить Flight Service:

docker compose stop flight-service

12.	Сделать несколько запросов (каждый уйдёт в retry, потом откроется Circuit Breaker):

curl http://localhost:8080/flights/1

Первые запросы возвращают 502 после трёх попыток (0.1s -> 0.2s -> 0.4s backoff). После накопления ошибок — немедленный 503 Service Unavailable. В логах booking-service:

CircuitBreaker CLOSED -> OPEN

13.	Восстановить сервис:

docker compose start flight-service

Через 15 секунд (CB_RESET_TIMEOUT) Circuit Breaker перейдёт в HALF_OPEN, сделает один пробный запрос, и при успехе — в CLOSED:

CircuitBreaker OPEN -> HALF_OPEN

CircuitBreaker HALF_OPEN -> CLOSED


Проверить Redis Sentinel Failover

14.	Посмотреть текущий master:

docker compose exec redis-sentinel redis-cli -p 26379 SENTINEL get-master-addr-by-name primary

Вывод: IP-адрес и порт текущего Redis master (redis-primary).

15.	Остановить primary:

docker compose stop redis-primary

16.	Подождать 5-10 секунд и снова проверить:

docker compose exec redis-sentinel redis-cli -p 26379 SENTINEL get-master-addr-by-name primary

Теперь в ответе будет адрес redis-secondary — Sentinel переключил master. Flight Service продолжает работать через автопереподключение.


Запустить тесты (без Docker)

Тесты запускаются локально, Docker не нужен:

cd tests

pip install -r requirements.txt

python -m pytest test_retry.py -v

Ожидаемый результат: 14 passed. Тесты мокают protobuf через conftest.py, поэтому работают без запущенного Docker.


Посмотреть данные в БД (PostgreSQL)

Можно подключиться к любой БД напрямую из контейнера:

# Рейсы и резервации (Flight Service):

docker compose exec flights-db psql -U flights_svc -d flights_db -c 'SELECT * FROM flights;'

docker compose exec flights-db psql -U flights_svc -d flights_db -c 'SELECT * FROM seat_reservations;'

# Бронирования (Booking Service):

docker compose exec bookings-db psql -U bookings_svc -d bookings_db -c 'SELECT * FROM bookings;'


Остановить проект

docker compose stop            # остановить без удаления данных

docker compose down            # удалить контейнеры и сети, данные в volumes остаются

docker compose down -v         # полный сброс: удалить всё включая данные БД
 



## Итог

В результате я собрал полноценную микросервисную систему, в которой есть:

- REST API для клиента
- gRPC взаимодействие между сервисами
- две отдельные БД
- транзакционное резервирование мест
- идемпотентность
- внутренняя авторизация по API key
- retry с exponential backoff
- circuit breaker
- кеширование в Redis
- Redis Sentinel с failover и автопереподключением
- тесты на retry-механику и circuit breaker

То есть я не просто поднял два сервиса, а довёл решение до состояния, где оно уже умеет переживать сбои зависимостей и корректно работать при частичных отказах.
