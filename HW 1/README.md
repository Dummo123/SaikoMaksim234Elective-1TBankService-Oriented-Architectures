# Marketplace Architecture

## 1. Context

The goal was to design a backend architecture for a Marketplace - a platform where sellers publish product listings and buyers browse, order, and pay for them. The system needs to handle a personalized product feed, a full order lifecycle, payments, and notifications.

## 2. C4 Diagram

![C4 Diagram](C4_Diagram.png)

## 3. Services & Responsibilities

| Domain | Service | What it does |
|--------|---------|--------------|
| Identity | Auth Service | Registration, login, JWT token issuance |
| Catalog | Catalog Service | Product listings, categories, seller management |
| Orders | Order Service | Cart, order placement, order status tracking |
| Payments | Payment Service | Transaction processing, payment gateway integration |
| Recommendations | Recommendation Service | ML-based personalized feed per user |
| Notifications | Notification Service | Email/push alerts triggered by order events |

### Database Strategy

Each service has its own database (Database-per-Service pattern) - no service queries another's DB directly:

- Auth Service => PostgreSQL (credentials, user profiles)
- Catalog Service => MongoDB (flexible product scheme)
- Order Service => PostgreSQL (transactional, ACID guarantees needed)

### Communication

- Synchronous (REST / gRPC) - for user-facing requests where an immediate response is required (i.e. fetching product details, placing an order)
- Asynchronous (Kafka) - for cross-service events where a response isn't needed instantly (i.e. order.paid event => Notification Service sends an email)

## 4. Architecture Decision: Monolith vs Microservices

### Modular Monolith

All domains live in one codebase, separated by modules.

+: Simple local development, no network overhead between modules, easy to debug end-to-end.  
-: One slow module (e.g. the ML recommendation engine) blocks everything else; scaling requires duplicating the whole app even if only one part is under load; any team working on the same repo creates merge conflicts.

### Microservices

Each domain is an independent service with its own process and database.

+:
- Scale only what needs scaling — during a sale, I can run 10 replicas of Catalog Service without touching Auth Service
- Fault isolation — if Notification Service crashes, purchases still go through; events just queue in Kafka
- Tech flexibility — Python for ML-heavy services, Go/Java for throughput-heavy ones

-: Requires Docker/Kubernetes to manage, distributed tracing is harder, eventual consistency must be handled explicitly.

In the end, I went with **Microservices**, because:

1. The Recommendation Service runs ML inference - it has completely different compute needs (CPU/GPU-heavy, slower) compared to the transactional services. Isolating it means a slow recommendation request never delays an order checkout.
2. The Catalog Service is the most read-heavy part of the system (every page load hits it). It needs to be cached and scaled independently - something you can't do cleanly in a monolith.

## 5. Service Implementation

I implemented the **Order Service** in Python (FastAPI) and containerized it with Docker.

It exposes three endpoints:

| Method | Path | Description |
|-----|------|-------------|
| GET | /health | Liveness check |
| POST | /orders | Create a new order |
| GET | /orders/{order_id} | Fetch an order by ID |

### How to Run

1. Open a terminal and navigate to the task directory:
   ```bash
   cd "HW 1"
   ```

2. Build and start the container:
   ```bash
   docker compose up --build
   ```

3. Check the health endpoint in your browser or with curl:
   ```
   http://localhost:8000/health
   ```
   Expected:
   ```json
   {"status": "OK", "service": "Order Service"}
   ```

4. Create a test order:
   ```bash
   curl -X POST http://localhost:8000/orders \
     -H "Content-Type: application/json" \
     -d '{"user_id": "u1", "product_id": "p42", "quantity": 2, "price": 499.99}'
   ```
   Expected:
   ```json
   {
     "order_id": "<uuid>",
     "user_id": "u1",
     "product_id": "p42",
     "quantity": 2,
     "total": 999.98,
     "status": "pending"
   }
   ```

5. Stop the container:
   ```bash
   docker compose down
   ```
