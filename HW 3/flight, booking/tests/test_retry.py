"""
Tests for retry and circuit breaker logic in grpc_client.py.

Run from the tests/ directory:
    pip install -r requirements.txt
    python -m pytest test_retry.py -v
"""

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

# Resolve booking-service package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "booking-service"))

from grpc_client import (  # noqa: E402
    INITIAL_DELAY,
    MAX_ATTEMPTS,
    Breaker,
    CircuitOpenError,
    _breaker,
    call_with_retry,
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

class FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode):
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return f"fake error: {self._code.name}"


def rpc_err(code: grpc.StatusCode) -> FakeRpcError:
    return FakeRpcError(code)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_breaker():
    """Reset module-level breaker state before every test."""
    _breaker.state      = "CLOSED"
    _breaker._opened_at = 0.0
    _breaker._window.clear()
    yield


# ------------------------------------------------------------------
# Retry behaviour
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retry_on_unavailable():
    fn = AsyncMock(side_effect=rpc_err(grpc.StatusCode.UNAVAILABLE))

    with patch("grpc_client.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(grpc.RpcError):
            await call_with_retry(fn)

    assert fn.call_count == MAX_ATTEMPTS


@pytest.mark.asyncio
async def test_retry_on_deadline_exceeded():
    fn = AsyncMock(side_effect=rpc_err(grpc.StatusCode.DEADLINE_EXCEEDED))

    with patch("grpc_client.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(grpc.RpcError):
            await call_with_retry(fn)

    assert fn.call_count == MAX_ATTEMPTS


@pytest.mark.asyncio
async def test_no_retry_on_not_found():
    fn = AsyncMock(side_effect=rpc_err(grpc.StatusCode.NOT_FOUND))

    with pytest.raises(grpc.RpcError):
        await call_with_retry(fn)

    assert fn.call_count == 1


@pytest.mark.asyncio
async def test_no_retry_on_resource_exhausted():
    fn = AsyncMock(side_effect=rpc_err(grpc.StatusCode.RESOURCE_EXHAUSTED))

    with pytest.raises(grpc.RpcError):
        await call_with_retry(fn)

    assert fn.call_count == 1


@pytest.mark.asyncio
async def test_no_retry_on_invalid_argument():
    fn = AsyncMock(side_effect=rpc_err(grpc.StatusCode.INVALID_ARGUMENT))

    with pytest.raises(grpc.RpcError):
        await call_with_retry(fn)

    assert fn.call_count == 1


@pytest.mark.asyncio
async def test_success_on_second_attempt():
    ok = MagicMock()
    fn = AsyncMock(side_effect=[rpc_err(grpc.StatusCode.UNAVAILABLE), ok])

    with patch("grpc_client.asyncio.sleep", new_callable=AsyncMock):
        result = await call_with_retry(fn)

    assert result is ok
    assert fn.call_count == 2


@pytest.mark.asyncio
async def test_exponential_backoff():
    fn = AsyncMock(side_effect=rpc_err(grpc.StatusCode.UNAVAILABLE))
    delays: list[float] = []

    async def fake_sleep(t: float):
        delays.append(t)

    with patch("grpc_client.asyncio.sleep", side_effect=fake_sleep):
        with pytest.raises(grpc.RpcError):
            await call_with_retry(fn)

    assert len(delays) == MAX_ATTEMPTS - 1
    assert delays[0] == pytest.approx(INITIAL_DELAY)
    assert delays[1] == pytest.approx(INITIAL_DELAY * 2)


@pytest.mark.asyncio
async def test_success_no_retry_needed():
    expected = MagicMock()
    fn = AsyncMock(return_value=expected)

    result = await call_with_retry(fn)

    assert result is expected
    assert fn.call_count == 1


# ------------------------------------------------------------------
# Circuit Breaker unit tests
# ------------------------------------------------------------------

def test_breaker_opens_after_threshold():
    b = Breaker(threshold=3, timeout=30, window=5)
    for _ in range(5):
        b.on_failure()
    assert b.state == "OPEN"


def test_breaker_stays_closed_below_threshold():
    b = Breaker(threshold=4, timeout=30, window=5)
    for _ in range(3):
        b.on_failure()
    assert b.state == "CLOSED"


def test_breaker_transitions_to_half_open_after_timeout():
    b = Breaker(threshold=1, timeout=0, window=2)
    b.on_failure()
    b.on_failure()
    # timeout=0 → should immediately allow HALF_OPEN probe
    b.before_call()
    assert b.state == "HALF_OPEN"


def test_breaker_closes_after_successful_probe():
    b = Breaker(threshold=1, timeout=0, window=2)
    b.on_failure(); b.on_failure()
    b.before_call()  # → HALF_OPEN
    b.on_success()
    assert b.state == "CLOSED"


def test_breaker_reopens_on_failed_probe():
    b = Breaker(threshold=1, timeout=0, window=2)
    b.on_failure(); b.on_failure()
    b.before_call()  # → HALF_OPEN
    b.on_failure()
    assert b.state == "OPEN"


def test_breaker_raises_when_open():
    b = Breaker(threshold=1, timeout=9999, window=2)
    b.on_failure(); b.on_failure()
    assert b.state == "OPEN"
    with pytest.raises(CircuitOpenError):
        b.before_call()
