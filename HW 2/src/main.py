import json
import time
import uuid
from datetime import datetime, timezone

import jwt
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from src.routers.auth        import SECRET_KEY, ALGORITHM, router as auth_router
from src.routers.orders      import router as orders_router
from src.routers.products    import router as products_router
from src.routers.promo_codes import router as promo_codes_router

app = FastAPI(title="Shop API", version="1.0.0")


# ── exception handlers ────────────────────────────────────────────────────────

@app.exception_handler(RequestValidationError)
async def _handle_validation_error(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=400,
        content={
            "error_code": "VALIDATION_ERROR",
            "message":    "Request data failed validation",
            "details":    exc.errors(),
        },
    )


# ── helpers ───────────────────────────────────────────────────────────────────

_SENSITIVE = {"password", "password_hash"}


def _mask_body(raw: bytes) -> dict | None:
    if not raw:
        return None
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return {k: ("***" if k in _SENSITIVE else v) for k, v in data.items()}
        return data
    except (json.JSONDecodeError, ValueError):
        return None


def _extract_user_id(request: Request) -> str | None:
    """Try to extract user_id from Bearer token without failing the request."""
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        return None
    try:
        payload = jwt.decode(auth[7:], SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except Exception:
        return None


async def _rebuffer_body(request: Request, body: bytes) -> None:
    async def _receive():
        return {"type": "http.request", "body": body}
    request._receive = _receive


# ── logging middleware ────────────────────────────────────────────────────────

@app.middleware("http")
async def log_requests(request: Request, call_next):
    req_id     = str(uuid.uuid4())
    started_at = time.monotonic()

    body_bytes = b""
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        body_bytes = await request.body()
        await _rebuffer_body(request, body_bytes)

    response   = await call_next(request)
    duration_ms = round((time.monotonic() - started_at) * 1000)

    log_entry = {
        "request_id":  req_id,
        "method":      request.method,
        "endpoint":    request.url.path,
        "status_code": response.status_code,
        "duration_ms": duration_ms,
        "user_id":     _extract_user_id(request),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }

    masked = _mask_body(body_bytes)
    if masked is not None:
        log_entry["body"] = masked

    print(json.dumps(log_entry, default=str))

    response.headers["X-Request-Id"] = req_id
    return response


# ── routes ────────────────────────────────────────────────────────────────────

@app.get("/healthz", tags=["System"])
async def health():
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


app.include_router(auth_router,        prefix="/auth",        tags=["Auth"])
app.include_router(products_router,    prefix="/products",    tags=["Products"])
app.include_router(orders_router,      prefix="/orders",      tags=["Orders"])
app.include_router(promo_codes_router, prefix="/promo-codes", tags=["PromoCodes"])
