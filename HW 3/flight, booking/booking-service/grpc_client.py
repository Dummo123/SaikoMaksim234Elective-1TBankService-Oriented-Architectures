import asyncio
import logging
import os
import time
from collections import deque

import grpc
import flight_pb2_grpc

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("INTERNAL_API_KEY", "secret-key")

RETRY_CODES = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}
NO_RETRY_CODES = {
    grpc.StatusCode.NOT_FOUND,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.INVALID_ARGUMENT,
}

MAX_RETRIES = 3
BACKOFF_BASE = 0.1

CB_FAILURE_THRESHOLD = int(os.environ.get("CB_FAILURE_THRESHOLD", 5))
CB_RESET_TIMEOUT = int(os.environ.get("CB_RESET_TIMEOUT", 15))
CB_WINDOW_SIZE = int(os.environ.get("CB_WINDOW_SIZE", 10))


class CircuitBreakerOpenError(Exception):
    pass


class CircuitBreaker:
    def __init__(self, failure_threshold: int, reset_timeout: int, window_size: int):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.window_size = window_size
        self.state = "CLOSED"
        self.last_failure_time = 0.0
        self._events = deque(maxlen=window_size)

    def _failure_count(self) -> int:
        return sum(1 for ok in self._events if not ok)

    def before_call(self):
        now = time.time()

        if self.state == "OPEN":
            if now - self.last_failure_time >= self.reset_timeout:
                self.state = "HALF_OPEN"
                logger.warning("Circuit breaker -> HALF_OPEN")
                return
            raise CircuitBreakerOpenError("Circuit breaker is OPEN")

    def on_success(self):
        self._events.append(True)
        if self.state in {"OPEN", "HALF_OPEN"}:
            logger.warning("Circuit breaker -> CLOSED")
        self.state = "CLOSED"

    def on_failure(self):
        self._events.append(False)
        self.last_failure_time = time.time()

        if self.state == "HALF_OPEN":
            self.state = "OPEN"
            logger.warning("Circuit breaker -> OPEN")
            return

        if len(self._events) >= self.window_size and self._failure_count() >= self.failure_threshold:
            if self.state != "OPEN":
                self.state = "OPEN"
                logger.warning(
                    f"Circuit breaker -> OPEN (failures={self._failure_count()}/{len(self._events)})"
                )


circuit_breaker = CircuitBreaker(
    failure_threshold=CB_FAILURE_THRESHOLD,
    reset_timeout=CB_RESET_TIMEOUT,
    window_size=CB_WINDOW_SIZE,
)


class ApiKeyInterceptor(
    grpc.aio.UnaryUnaryClientInterceptor,
    grpc.aio.UnaryStreamClientInterceptor,
):
    async def intercept_unary_unary(self, continuation, client_call_details, request):
        client_call_details = client_call_details._replace(
            metadata=[*(client_call_details.metadata or []), ("x-api-key", API_KEY)]
        )
        return await continuation(client_call_details, request)

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        client_call_details = client_call_details._replace(
            metadata=[*(client_call_details.metadata or []), ("x-api-key", API_KEY)]
        )
        return await continuation(client_call_details, request)


def get_channel():
    host = os.environ.get("FLIGHT_SERVICE_HOST", "localhost")
    port = os.environ.get("FLIGHT_SERVICE_PORT", "50051")
    return grpc.aio.insecure_channel(
        f"{host}:{port}",
        interceptors=[ApiKeyInterceptor()],
    )


def get_stub(channel):
    return flight_pb2_grpc.FlightServiceStub(channel)


async def grpc_call_with_retry(fn, *args, **kwargs):
    circuit_breaker.before_call()

    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            result = await fn(*args, **kwargs)
            circuit_breaker.on_success()
            return result
        except grpc.RpcError as e:
            code = e.code()

            if code in NO_RETRY_CODES:
                circuit_breaker.on_success()
                raise

            if code not in RETRY_CODES:
                circuit_breaker.on_failure()
                raise

            last_error = e

            if attempt == MAX_RETRIES - 1:
                break

            wait = BACKOFF_BASE * (2 ** attempt)
            logger.warning(
                f"gRPC call failed with {code.name}, "
                f"attempt {attempt + 1}/{MAX_RETRIES}, "
                f"retrying in {wait:.2f}s..."
            )
            await asyncio.sleep(wait)

    circuit_breaker.on_failure()
    logger.error(f"gRPC call failed after {MAX_RETRIES} attempts")
    raise last_error
