# Shop API

## About
A marketplace backend built with a contract-first approach. The OpenAPI specification is the single source of truth — Pydantic request/response models are generated from it automatically, so the code always matches the API contract.

## Tech Stack
|Component| Choice                 |Why|
|----|------------------------|---|
|Framework| FastAPI                |Native async, auto-docs, DI|
|Database| PostgreSQL 16          |ACID, rich types|
|ORM| SQLAlchemy 2 async     |Async-native, flexible|
|Migrations| Flyway                 |Versioned, reproducible|
|Auth| JWT(python-jose) + bcrypt|Industry standard|
|Model gen|datamodel-code-generator|Contract-first DTOs|

## Key Design Decisions

Contract-first: openapi/openapi.yaml is written first. Running ./generate.sh produces src/models/generated.py - the only DTO file. Manually writing DTOs is prohibited.

Soft delete: DELETE /products/{id} sets status = ARCHIVED - data is never lost.

**Order business rules**:
- Rate limit: max 3 CREATE_ORDER operations per 60 seconds per user
- At most 1 active order per user at a time
- Stock is reserved atomically; insufficient stock returns INSUFFICIENT_STOCK
- Promo codes support PERCENTAGE and FIXED_AMOUNT discounts with expiry/usage limits
- Order status follows a strict state machine, only CREATED / PAYMENT_PENDING can be canceled

**RBAC matrix**:

| Endpoint | USER | SELLER | ADMIN |
|----------|-|-|-|
|GET /products|+|+|+|
|POST /products|-|+|+|
|PUT/DELETE /products/{id}|-|own|+|
|POST /orders|+|-|-|
|PUT /orders/{id}|own|-|-|
|POST /orders/{id}/cancel|own|-|+|

**Logging**: every HTTP request produces a structured JSON log line with request_id, method, path, status_code, duration_ms, and masked body (passwords are hidden).

## Running Locally

```bash
# 1. Install dependencies
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Start PostgreSQL
docker-compose up -d postgres

# 3. Apply Flyway migrations
docker-compose up flyway

# 4. Generate Pydantic models
chmod +x generate.sh && ./generate.sh

# 5. Start the server
uvicorn src.main:app --reload
```

Open **http://127.0.0.1:8000/docs** for the Swagger UI.

### Typical test flow
1. POST /auth/register - create a USER account
2. POST /auth/login - get access_token
3. Click **Authorize** in Swagger, paste the token
4. POST /products (as SELLER) - create a product
5. POST /orders (as USER) - place an order
