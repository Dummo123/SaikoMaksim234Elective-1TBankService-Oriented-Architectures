"""
grpc_client.py — gRPC client layer for Booking Service.

Provides:
  ApiKeyInterceptor  — injects x-api-key header into every outgoing call
  Breaker            — Circuit Breaker (CLOSED / OPEN / HALF_OPEN)
  call_with_retry    — exponential-backoff retry that respects the breaker
"""

import asyncio
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager

import grpc
import flight_pb2_grpc

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

SERVICE_API_KEY = os.environ.get("SERVICE_API_KEY", "dev-key")
FLIGHT_SVC_HOST = os.environ.get("FLIGHT_SVC_HOST", "localhost")
FLIGHT_SVC_PORT = os.environ.get("FLIGHT_SVC_PORT", "50051")

MAX_ATTEMPTS  = 3
INITIAL_DELAY = 0.1  # seconds; doubles each attempt

# gRPC status codes that should trigger a retry
_RETRYABLE = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}

# Business-logic errors — propagate immediately, no retry
_PERMANENT = {
    grpc.StatusCode.NOT_FOUND,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.INVALID_ARGUMENT,
}

# Circuit Breaker configuration (all overridable via env)
_CB_THRESHOLD = int(os.environ.get("CB_FAILURE_THRESHOLD", "5"))
_CB_TIMEOUT   = int(os.environ.get("CB_RESET_TIMEOUT", "15"))
_CB_WINDOW    = int(os.environ.get("CB_WINDOW_SIZE", "10"))


# ------------------------------------------------------------------
# Custom exception
# ------------------------------------------------------------------

class CircuitOpenError(Exception):
    """Raised when calls are blocked by an open circuit breaker."""


# ------------------------------------------------------------------
# Circuit Breaker
# ------------------------------------------------------------------

class Breaker:
    """
    Sliding-window Circuit Breaker.

    States:
      CLOSED    — normal operation; failures are counted
      OPEN      — all calls fail immediately without touching Flight Service
      HALF_OPEN — one probe call is allowed to check recovery
    """

    def __init__(self, threshold: int, timeout: int, window: int) -> None:
        self.threshold  = threshold
        self.timeout    = timeout
        self.state      = "CLOSED"
        self._window: deque[bool] = deque(maxlen=window)
        self._opened_at = 0.0

    def before_call(self) -> None:
        if self.state == "OPEN":
            elapsed = time.monotonic() - self._opened_at
            if elapsed >= self.timeout:
                self._transition("HALF_OPEN")
            else:
                raise CircuitOpenError(
                    f"Flight Service is unavailable (circuit OPEN, "
                    f"retry in {self.timeout - elapsed:.0f}s)"
                )

    def on_success(self) -> None:
        self._window.append(True)
        if self.state != "CLOSED":
            self._transition("CLOSED")

    def on_failure(self) -> None:
        self._window.append(False)
        if self.state == "HALF_OPEN":
            self._open()
            return
        failures = sum(1 for ok in self._window if not ok)
        if len(self._window) == self._window.maxlen and failures >= self.threshold:
            if self.state == "CLOSED":
                self._open()

    def _open(self) -> None:
        self._opened_at = time.monotonic()
        self._transition("OPEN")

    def _transition(self, new_state: str) -> None:
        if self.state != new_state:
            logger.warning("CircuitBreaker %s -> %s", self.state, new_state)
            self.state = new_state


# Module-level breaker used by all gRPC calls
_breaker = Breaker(_CB_THRESHOLD, _CB_TIMEOUT, _CB_WINDOW)


# ------------------------------------------------------------------
# Auth interceptor
# ------------------------------------------------------------------

class ApiKeyInterceptor(
    grpc.aio.UnaryUnaryClientInterceptor,
    grpc.aio.UnaryStreamClientInterceptor,
):
    """Attaches the internal service API key to outgoing gRPC metadata."""

    def _inject(self, details):
        meta = list(details.metadata or [])
        meta.append(("x-api-key", SERVICE_API_KEY))
        return details._replace(metadata=meta)

    async def intercept_unary_unary(self, continuation, call_details, request):
        return await continuation(self._inject(call_details), request)

    async def intercept_unary_stream(self, continuation, call_details, request):
        return await continuation(self._inject(call_details), request)


# ------------------------------------------------------------------
# Channel factory
# ------------------------------------------------------------------

@asynccontextmanager
async def flight_channel():
    channel = grpc.aio.insecure_channel(
        f"{FLIGHT_SVC_HOST}:{FLIGHT_SVC_PORT}",
        interceptors=[ApiKeyInterceptor()],
    )
    try:
        yield channel
    finally:
        await channel.close()


def get_stub(channel) -> flight_pb2_grpc.FlightServiceStub:
    return flight_pb2_grpc.FlightServiceStub(channel)


# ------------------------------------------------------------------
# Retry + circuit-breaker wrapper
# ------------------------------------------------------------------

async def call_with_retry(fn, *args, **kwargs):
    """
    Run an async gRPC callable with exponential-backoff retry
    protected by the module circuit breaker.

    - Retries only UNAVAILABLE and DEADLINE_EXCEEDED (up to MAX_ATTEMPTS).
    - Propagates NOT_FOUND / RESOURCE_EXHAUSTED / INVALID_ARGUMENT immediately.
    - Raises CircuitOpenError when the breaker is OPEN.
    """
    _breaker.before_call()

    last_exc: grpc.RpcError | None = None

    for attempt in range(MAX_ATTEMPTS):
        try:
            result = await fn(*args, **kwargs)
            _breaker.on_success()
            return result

        except grpc.RpcError as exc:
            code = exc.code()

            if code in _PERMANENT:
                # Not a connectivity issue — don't penalise the breaker
                _breaker.on_success()
                raise

            if code not in _RETRYABLE:
                _breaker.on_failure()
                raise

            last_exc = exc

            if attempt < MAX_ATTEMPTS - 1:
                delay = INITIAL_DELAY * (2 ** attempt)
                logger.warning(
                    "gRPC %s on attempt %d/%d — retrying in %.2fs",
                    code.name, attempt + 1, MAX_ATTEMPTS, delay,
                )
                await asyncio.sleep(delay)

    _breaker.on_failure()
    logger.error("gRPC call failed after %d attempts", MAX_ATTEMPTS)
    raise last_exc
