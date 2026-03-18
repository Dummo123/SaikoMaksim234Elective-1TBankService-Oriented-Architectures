import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "booking-service"))

from grpc_client import (  # noqa: E402
    BACKOFF_BASE,
    MAX_RETRIES,
    circuit_breaker,
    grpc_call_with_retry,
)


class FakeRpcError(grpc.RpcError):
    def __init__(self, code):
        super().__init__()
        self._code = code

    def code(self):
        return self._code

    def details(self):
        return f"error: {self._code.name}"


def make_rpc_error(code):
    return FakeRpcError(code)


@pytest.fixture(autouse=True)
def reset_cb():
    circuit_breaker.state = "CLOSED"
    circuit_breaker.last_failure_time = 0.0
    circuit_breaker._events.clear()


@pytest.mark.asyncio
async def test_retry_on_unavailable():
    fn = AsyncMock(side_effect=make_rpc_error(grpc.StatusCode.UNAVAILABLE))

    with patch("grpc_client.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(grpc.RpcError):
            await grpc_call_with_retry(fn)

    assert fn.call_count == MAX_RETRIES


@pytest.mark.asyncio
async def test_no_retry_on_not_found():
    fn = AsyncMock(side_effect=make_rpc_error(grpc.StatusCode.NOT_FOUND))

    with pytest.raises(grpc.RpcError):
        await grpc_call_with_retry(fn)

    assert fn.call_count == 1


@pytest.mark.asyncio
async def test_no_retry_on_resource_exhausted():
    fn = AsyncMock(side_effect=make_rpc_error(grpc.StatusCode.RESOURCE_EXHAUSTED))

    with pytest.raises(grpc.RpcError):
        await grpc_call_with_retry(fn)

    assert fn.call_count == 1


@pytest.mark.asyncio
async def test_success_on_second_attempt():
    result = MagicMock()
    fn = AsyncMock(
        side_effect=[
            make_rpc_error(grpc.StatusCode.UNAVAILABLE),
            result,
        ]
    )

    with patch("grpc_client.asyncio.sleep", new_callable=AsyncMock):
        response = await grpc_call_with_retry(fn)

    assert response == result
    assert fn.call_count == 2


@pytest.mark.asyncio
async def test_exponential_backoff():
    fn = AsyncMock(side_effect=make_rpc_error(grpc.StatusCode.UNAVAILABLE))
    sleep_calls = []

    async def fake_sleep(t):
        sleep_calls.append(t)

    with patch("grpc_client.asyncio.sleep", side_effect=fake_sleep):
        with pytest.raises(grpc.RpcError):
            await grpc_call_with_retry(fn)

    assert sleep_calls[0] == pytest.approx(BACKOFF_BASE)
    assert sleep_calls[1] == pytest.approx(BACKOFF_BASE * 2)


@pytest.mark.asyncio
async def test_success_no_retry():
    result = MagicMock()
    fn = AsyncMock(return_value=result)

    response = await grpc_call_with_retry(fn)

    assert response == result
    assert fn.call_count == 1
