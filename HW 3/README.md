# HW3 — Flight Booking: gRPC + Redis

Микросервисная система бронирования авиабилетов.

```
Client (REST) → Booking Service → (gRPC) → Flight Service
                      ↓                           ↓
                 PostgreSQL               PostgreSQL + Redis (Sentinel)
```

---

## Структура проекта

```
hw3-flight-booking/
├── booking-service/
│   ├── db.py               — asyncpg connection pool
│   ├── grpc_client.py      — gRPC-клиент: ApiKeyInterceptor, Breaker, retry
│   ├── main.py             — FastAPI REST API
│   ├── flight.proto
│   ├── Dockerfile
│   ├── requirements.txt
│   └── migrations/
│       └── V1__init.sql
├── flight-service/
│   ├── db.py               — asyncpg connection pool
│   ├── main.py             — gRPC-сервер + Redis Cache-Aside + AuthInterceptor
│   ├── flight.proto
│   ├── Dockerfile
│   ├── requirements.txt
│   └── migrations/
│       └── V1__init.sql
├── proto/
│   └── flight.proto        — единый источник gRPC-контракта
├── tests/
│   ├── conftest.py
│   ├── test_retry.py
│   └── requirements.txt
├── .gitignore
├── docker-compose.yml
├── er-diagram.mmd
└── README.md
```

---

## Запуск

```bash
docker compose up --build
```

Booking Service доступен на `http://localhost:8080`.

---

## REST API

### Поиск рейсов

```bash
curl "http://localhost:8080/flights?origin=SVO&destination=LED"
curl "http://localhost:8080/flights?origin=SVO&destination=LED&date=2026-04-01"
```

### Получить рейс

```bash
curl "http://localhost:8080/flights/1"
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

### Получить / отменить бронирование

```bash
curl "http://localhost:8080/bookings/<ID>"
curl -X POST "http://localhost:8080/bookings/<ID>/cancel"
```

### Список бронирований

```bash
curl "http://localhost:8080/bookings?customer_id=user-42"
```

---

## Проверка Circuit Breaker

```bash
docker compose stop flight-service   # провоцируем ошибки
curl "http://localhost:8080/flights/1"  # retry → OPEN → 503
docker compose start flight-service  # восстанавливаем
# OPEN → HALF_OPEN → CLOSED
```

---

## Проверка Redis Sentinel Failover

```bash
# Текущий master
docker compose exec redis-sentinel redis-cli -p 26379 SENTINEL get-master-addr-by-name primary

# Убиваем primary
docker compose stop redis-primary

# Sentinel переключает master на secondary (~5-10 сек)
docker compose exec redis-sentinel redis-cli -p 26379 SENTINEL get-master-addr-by-name primary
```

---

## Тесты

```bash
cd tests
pip install -r requirements.txt
python -m pytest test_retry.py -v
```
