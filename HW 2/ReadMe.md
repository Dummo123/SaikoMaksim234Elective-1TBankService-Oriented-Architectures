# Shop API

## Overview

A marketplace backend service built with a contract-first approach using FastAPI and PostgreSQL.
The OpenAPI specification is the single source of truth — Pydantic models are generated from it
automatically, so the implementation always matches the API contract.

## Project Structure

```
shop_api/
├── docker-compose.yml
├── generate.sh
├── requirements.txt
├── openapi/
│   └── openapi.yaml
├── migrations/
│   ├── V1__init_products.sql
│   ├── V2__init_orders.sql
│   └── V3__init_users.sql
└── src/
    ├── main.py
    ├── config.py
    ├── database.py
    ├── models/
    │   ├── db.py
    │   └── generated.py       <- auto-generated, do not edit
    └── routers/
        ├── auth.py
        ├── products.py
        ├── orders.py
        └── promo_codes.py
```

## Tech Stack

| Component     | Choice                        |
|---------------|-------------------------------|
| Framework     | FastAPI                       |
| Server        | Uvicorn                       |
| Database      | PostgreSQL 16                 |
| ORM           | SQLAlchemy 2 (async)          |
| Driver        | asyncpg                       |
| Migrations    | Flyway                        |
| Auth          | JWT via PyJWT + bcrypt        |
| Model gen     | datamodel-code-generator      |
| Validation    | Pydantic v2                   |

## How to Run

### Prerequisites

- Python 3.11+
- Docker and Docker Compose

### Step 1 — Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Step 2 — Start PostgreSQL

```bash
docker-compose up -d postgres
docker ps
```

### Step 3 — Run Flyway migrations

```bash
docker-compose up flyway
```

### Step 4 — Generate Pydantic models from OpenAPI spec

```bash
datamodel-codegen --input openapi/openapi.yaml --input-file-type openapi --output src/models/generated.py --output-model-type pydantic_v2.BaseModel
```

This produces src/models/generated.py. The file is listed in .gitignore and must be
regenerated on each fresh clone before starting the server.

### Step 5 — Start the server

```bash
pip install bcrypt==4.0.1
uvicorn src.main:app --reload
```

The API is available at http://127.0.0.1:8000
Interactive docs (Swagger UI) are at http://127.0.0.1:8000/docs

## Authentication

The API uses JWT bearer tokens with two token types.

| Token         | Lifetime  | Purpose                          |
|---------------|-----------|----------------------------------|
| access_token  | 15 min    | Authorize API requests           |
| refresh_token | 30 days   | Obtain a new access_token        |

Auth endpoints do not require a token. All other endpoints require a valid access_token
passed as a Bearer header.

## Roles

| Role   | Description                                      |
|--------|--------------------------------------------------|
| USER   | Can browse products and manage their own orders  |
| SELLER | Can create and manage their own products         |
| ADMIN  | Full access to all resources                     |

## Access Control Matrix

| Operation                  | USER        | SELLER      | ADMIN    |
|----------------------------|-------------|-------------|----------|
| GET /products              | all         | all         | all      |
| GET /products/{id}         | any         | any         | any      |
| POST /products             | denied      | own         | any      |
| PUT /products/{id}         | denied      | own         | any      |
| DELETE /products/{id}      | denied      | own         | any      |
| POST /orders               | yes         | denied      | yes      |
| GET /orders/{id}           | own         | denied      | any      |
| PUT /orders/{id}           | own         | denied      | any      |
| POST /orders/{id}/cancel   | own         | denied      | any      |
| POST /promo-codes          | denied      | yes         | yes      |

## Order State Machine

Orders follow a strict state machine. Transitions not listed below are rejected
with INVALID_STATE_TRANSITION.

```
CREATED -> PAYMENT_PENDING -> PAID -> SHIPPED -> COMPLETED
       \-> CANCELED
PAYMENT_PENDING \-> CANCELED
```

## Order Business Rules

- Rate limit: only one CREATE_ORDER or UPDATE_ORDER operation is allowed per minute per user.
  The check is based on the most recent recorded operation of that type in user_operations.
- A user cannot have more than one order in CREATED or PAYMENT_PENDING status at a time.
- Stock is reserved atomically when an order is created. If any item fails validation,
  no stock is deducted and no order is created.
- price_at_order is a snapshot of the product price at creation time. Later price changes
  do not affect existing orders.
- Cancelling an order restores stock for all items and decrements current_uses on the
  applied promo code if one was used.
- Updating an order restores old stock, re-validates and re-reserves new items, then
  recalculates totals. If an applied promo code no longer meets min_order_amount after
  the update, the discount is removed and current_uses is decremented.

## Promo Code Rules

| Discount type  | Calculation                                           |
|----------------|-------------------------------------------------------|
| PERCENTAGE     | total * discount_value / 100, zeroed if over 70%      |
| FIXED_AMOUNT   | min(discount_value, total)                            |

A promo code is rejected if any of the following are true:
- active is false
- current_uses >= max_uses
- current time is outside [valid_from, valid_until]
- order total is below min_order_amount

## Error Codes

| error_code                 | HTTP | When                                              |
|----------------------------|------|---------------------------------------------------|
| VALIDATION_ERROR           | 400  | Request body or query params failed validation    |
| BAD_CREDENTIALS            | 401  | Wrong username or password                        |
| TOKEN_EXPIRED              | 401  | Access token has expired                          |
| TOKEN_INVALID              | 401  | Access token is malformed or missing              |
| REFRESH_TOKEN_INVALID      | 401  | Refresh token is invalid, expired, or wrong type  |
| ACCESS_DENIED              | 403  | Authenticated but insufficient role permissions   |
| ORDER_OWNERSHIP_VIOLATION  | 403  | Order belongs to a different user                 |
| PRODUCT_NOT_FOUND          | 404  | Product does not exist                            |
| ORDER_NOT_FOUND            | 404  | Order does not exist                              |
| USERNAME_TAKEN             | 409  | Username already registered                       |
| PRODUCT_INACTIVE           | 409  | Product is not in ACTIVE status                   |
| ORDER_HAS_ACTIVE           | 409  | User already has an open order                    |
| INSUFFICIENT_STOCK         | 409  | Requested quantity exceeds available stock        |
| INVALID_STATE_TRANSITION   | 409  | Order status transition is not permitted          |
| ORDER_NOT_EDITABLE         | 409  | Order is not in CREATED status                    |
| PROMO_CODE_INVALID         | 422  | Promo code not found, inactive, expired, or used  |
| PROMO_CODE_MIN_AMOUNT      | 422  | Order total is below the promo minimum            |
| ORDER_LIMIT_EXCEEDED       | 429  | Rate limit hit for order creation or update       |

## Request Logging

Every HTTP request produces a JSON log line on stdout with the following fields.

| Field       | Description                                          |
|-------------|------------------------------------------------------|
| request_id  | UUID generated per request                           |
| method      | HTTP method                                          |
| endpoint    | Request path                                         |
| status_code | HTTP response status                                 |
| duration_ms | Time to process the request in milliseconds          |
| user_id     | Subject from JWT if authenticated, otherwise null    |
| timestamp   | ISO 8601 UTC timestamp                               |
| body        | Request body for POST/PUT/DELETE, passwords masked   |

The request_id is also returned in the X-Request-Id response header.

## E2E Test Flow

```bash
# 1. Register a seller and a user
POST /auth/register   { "username": "seller2", "password": "pass", "role": "SELLER" }
POST /auth/register   { "username": "user2",   "password": "pass", "role": "USER"   }

# 2. Login and copy the access_token from each response
POST /auth/login      { "username": "seller2", "password": "pass" }
```
if doesnt work:
```Poweshell
$response = Invoke-RestMethod -Uri "http://127.0.0.1:8000/auth/login" -Method POST -ContentType "application/json" -Body '{"username": "seller2", "password": "pass123"}'
$response.access_token
```
```bash
POST /auth/login      { "username": "user2",   "password": "pass" }
```
if doesnt work:
```Poweshell
$user = Invoke-RestMethod -Uri "http://127.0.0.1:8000/auth/login" -Method POST -ContentType "application/json" -Body '{"username": "user2", "password": "pass123"}'
$user.access_token
```
```bash
# 3. As seller — create a product
POST /products        { "name": "Widget", "price": 9.99, "stock": 100,
                        "category": "tools", "status": "ACTIVE" }
```
(need to save id here)
```bash
# 4. As seller — create a promo code
POST /promo-codes     { "code": "SAVE10", "discount_type": "PERCENTAGE",
                        "discount_value": 10, "min_order_amount": 5,
                        "max_uses": 50, "valid_from": "2026-01-01T00:00:00",
                        "valid_until": "2027-01-01T00:00:00" }

# 5. As user — place an order with the promo
POST /orders          { "items": [{ "product_id": "<id from #3>", "quantity": 2 }],
                        "promo_code": "SAVE10" }
```
(need to save id here)
```bash
# 6. As user — view the order
GET /orders/<id from #5>

# 7. As user — cancel the order
POST /orders/<id from #5>/cancel

# 8. Verify stock restored — GET /products/<id> should show stock back to 100
```
