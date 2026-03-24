"""
Flight Service — gRPC server.

Implements: SearchFlights, GetFlight, ReserveSeats, ReleaseReservation.

Key features:
  - AuthInterceptor        : validates x-api-key in gRPC metadata
  - Cache-Aside with Redis : Sentinel or standalone, auto-reconnect on failover
  - SELECT FOR UPDATE      : prevents race conditions on seat reservation
  - Idempotency            : duplicate ReserveSeats with same booking_ref is a no-op
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import grpc
import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel
from redis.exceptions import ConnectionError as RedisConnectionError
from google.protobuf.timestamp_pb2 import Timestamp

import flight_pb2
import flight_pb2_grpc
from db import get_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [flight-svc] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------

GRPC_PORT       = int(os.environ.get("GRPC_PORT", "50051"))
SERVICE_API_KEY = os.environ.get("SERVICE_API_KEY", "dev-key")

REDIS_MODE           = os.environ.get("REDIS_MODE", "standalone")
REDIS_HOST           = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT           = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_MASTER_NAME    = os.environ.get("REDIS_MASTER_NAME", "primary")
REDIS_SENTINEL_HOST  = os.environ.get("REDIS_SENTINEL_HOST", "redis-sentinel")
REDIS_SENTINEL_PORT  = int(os.environ.get("REDIS_SENTINEL_PORT", "26379"))
CACHE_TTL            = int(os.environ.get("CACHE_TTL_SECONDS", "360"))

# ------------------------------------------------------------------
# Redis connection (with failover recovery)
# ------------------------------------------------------------------

_redis_client: aioredis.Redis | None = None
_sentinel_obj: Sentinel | None = None


async def _reset_redis() -> None:
    global _redis_client, _sentinel_obj
    if _redis_client is not None:
        try:
            await _redis_client.aclose()
        except Exception:
            pass
    _redis_client = None
    _sentinel_obj = None


async def _connect_redis() -> aioredis.Redis:
    global _redis_client, _sentinel_obj

    if REDIS_MODE == "sentinel":
        logger.info(
            "Connecting via Redis Sentinel %s:%s master=%s",
            REDIS_SENTINEL_HOST, REDIS_SENTINEL_PORT, REDIS_MASTER_NAME,
        )
        _sentinel_obj = Sentinel(
            [(REDIS_SENTINEL_HOST, REDIS_SENTINEL_PORT)],
            socket_timeout=1.0,
            decode_responses=True,
        )
        client = _sentinel_obj.master_for(
            REDIS_MASTER_NAME,
            redis_class=aioredis.Redis,
            decode_responses=True,
            socket_timeout=1.0,
        )
    else:
        logger.info("Connecting to standalone Redis %s:%s", REDIS_HOST, REDIS_PORT)
        client = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_timeout=1.0,
        )

    await client.ping()
    _redis_client = client
    logger.info("Redis connection established (mode=%s)", REDIS_MODE)
    return _redis_client


async def get_redis() -> aioredis.Redis:
    global _redis_client
    if _redis_client is None:
        return await _connect_redis()
    return _redis_client


async def redis_op(op: str, *args):
    """Execute a Redis command with one automatic reconnect on connection loss."""
    try:
        client = await get_redis()
        return await getattr(client, op)(*args)
    except RedisConnectionError:
        logger.warning("Redis connection lost — reconnecting...")
        await _reset_redis()
        client = await get_redis()
        return await getattr(client, op)(*args)


async def _evict_search_keys() -> None:
    """Delete all search:* keys using SCAN to avoid blocking."""
    try:
        client = await get_redis()
        cursor = 0
        while True:
            cursor, keys = await client.scan(cursor, match="search:*", count=100)
            if keys:
                await client.delete(*keys)
            if cursor == 0:
                break
    except Exception as exc:
        logger.warning("Failed to evict search keys: %s", exc)


# ------------------------------------------------------------------
# Proto helpers
# ------------------------------------------------------------------

_FLIGHT_STATUS = {
    "SCHEDULED": flight_pb2.SCHEDULED,
    "DEPARTED":  flight_pb2.DEPARTED,
    "CANCELLED": flight_pb2.CANCELLED,
    "COMPLETED": flight_pb2.COMPLETED,
}

_RSV_STATUS = {
    "ACTIVE":   flight_pb2.ACTIVE,
    "RELEASED": flight_pb2.RELEASED,
    "EXPIRED":  flight_pb2.EXPIRED,
}


def _dt_to_ts(dt: datetime) -> Timestamp:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


def _row_to_flight(row) -> flight_pb2.Flight:
    return flight_pb2.Flight(
        id=row["id"],
        flight_number=row["flight_number"],
        airline=row["airline"],
        origin=row["origin_iata"],
        destination=row["destination_iata"],
        departure_time=_dt_to_ts(row["departs_at"]),
        arrival_time=_dt_to_ts(row["arrives_at"]),
        total_seats=row["total_seats"],
        available_seats=row["available_seats"],
        price=float(row["ticket_price"]),
        status=_FLIGHT_STATUS.get(row["flight_status"], flight_pb2.FLIGHT_STATUS_UNSPECIFIED),
    )


def _flight_to_cache(f: flight_pb2.Flight) -> dict:
    return {
        "id":              f.id,
        "flight_number":   f.flight_number,
        "airline":         f.airline,
        "origin":          f.origin,
        "destination":     f.destination,
        "dep_nanos":       f.departure_time.ToNanoseconds(),
        "arr_nanos":       f.arrival_time.ToNanoseconds(),
        "total_seats":     f.total_seats,
        "available_seats": f.available_seats,
        "price":           f.price,
        "status":          f.status,
    }


def _cache_to_flight(d: dict) -> flight_pb2.Flight:
    dep = Timestamp(); dep.FromNanoseconds(d["dep_nanos"])
    arr = Timestamp(); arr.FromNanoseconds(d["arr_nanos"])
    return flight_pb2.Flight(
        id=d["id"],
        flight_number=d["flight_number"],
        airline=d["airline"],
        origin=d["origin"],
        destination=d["destination"],
        departure_time=dep,
        arrival_time=arr,
        total_seats=d["total_seats"],
        available_seats=d["available_seats"],
        price=d["price"],
        status=d["status"],
    )


def _row_to_reservation(row) -> flight_pb2.SeatReservation:
    return flight_pb2.SeatReservation(
        id=row["id"],
        flight_id=row["flight_id"],
        booking_id=str(row["booking_ref"]),
        seat_count=row["seats_held"],
        status=_RSV_STATUS.get(row["rsv_status"], flight_pb2.RESERVATION_STATUS_UNSPECIFIED),
        reserved_at=_dt_to_ts(row["reserved_at"]),
    )


# ------------------------------------------------------------------
# Auth interceptor
# ------------------------------------------------------------------

class AuthInterceptor(grpc.aio.ServerInterceptor):
    async def intercept_service(self, continuation, handler_call_details):
        meta = dict(handler_call_details.invocation_metadata)
        if meta.get("x-api-key", "") != SERVICE_API_KEY:
            async def _deny(req, ctx):
                await ctx.abort(
                    grpc.StatusCode.UNAUTHENTICATED,
                    "Missing or invalid API key",
                )
            return grpc.unary_unary_rpc_method_handler(_deny)
        return await continuation(handler_call_details)


# ------------------------------------------------------------------
# Servicer
# ------------------------------------------------------------------

class FlightServicer(flight_pb2_grpc.FlightServiceServicer):

    # ── SearchFlights ───────────────────────────────────────────────
    async def SearchFlights(self, request, context):
        cache_key = f"search:{request.origin}:{request.destination}:{request.date}"

        raw = await redis_op("get", cache_key)
        if raw:
            logger.info("CACHE HIT  %s", cache_key)
            data = json.loads(raw)
            return flight_pb2.SearchFlightsResponse(
                flights=[_cache_to_flight(d) for d in data]
            )

        logger.info("CACHE MISS %s", cache_key)
        pool = await get_pool()

        sql = """
            SELECT * FROM flights
            WHERE  origin_iata      = $1
              AND  destination_iata = $2
              AND  flight_status    = 'SCHEDULED'
        """
        params = [request.origin, request.destination]

        from datetime import datetime

        if request.date:
            date_obj = datetime.strptime(request.date, "%Y-%m-%d").date()
            sql += " AND DATE(departs_at AT TIME ZONE 'UTC') = $3"
            params.append(date_obj)

        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        flights = [_row_to_flight(r) for r in rows]
        payload = json.dumps([_flight_to_cache(f) for f in flights])
        await redis_op("setex", cache_key, CACHE_TTL, payload)
        logger.info("CACHE SET  %s TTL=%ds", cache_key, CACHE_TTL)

        return flight_pb2.SearchFlightsResponse(flights=flights)

    # ── GetFlight ───────────────────────────────────────────────────
    async def GetFlight(self, request, context):
        cache_key = f"flight:{request.flight_id}"

        raw = await redis_op("get", cache_key)
        if raw:
            logger.info("CACHE HIT  %s", cache_key)
            return flight_pb2.GetFlightResponse(
                flight=_cache_to_flight(json.loads(raw))
            )

        logger.info("CACHE MISS %s", cache_key)
        pool = await get_pool()

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM flights WHERE id = $1", request.flight_id
            )

        if row is None:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Flight {request.flight_id} not found",
            )
            return

        flight = _row_to_flight(row)
        await redis_op("setex", cache_key, CACHE_TTL, json.dumps(_flight_to_cache(flight)))
        logger.info("CACHE SET  %s TTL=%ds", cache_key, CACHE_TTL)

        return flight_pb2.GetFlightResponse(flight=flight)

    # ── ReserveSeats ────────────────────────────────────────────────
    async def ReserveSeats(self, request, context):
        pool = await get_pool()

        async with pool.acquire() as conn:
            async with conn.transaction():
                # Idempotency guard — same booking_ref returns existing reservation
                existing = await conn.fetchrow(
                    """
                    SELECT * FROM seat_reservations
                    WHERE  booking_ref = $1 AND rsv_status = 'ACTIVE'
                    """,
                    request.booking_id,
                )
                if existing:
                    logger.info("Idempotent hit booking_ref=%s", request.booking_id)
                    return flight_pb2.ReserveSeatsResponse(
                        reservation=_row_to_reservation(existing)
                    )

                # Lock flight row to prevent concurrent over-booking
                flight_row = await conn.fetchrow(
                    "SELECT * FROM flights WHERE id = $1 FOR UPDATE",
                    request.flight_id,
                )
                if flight_row is None:
                    await context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
                    return

                if flight_row["available_seats"] < request.seat_count:
                    await context.abort(
                        grpc.StatusCode.RESOURCE_EXHAUSTED,
                        f"Only {flight_row['available_seats']} seat(s) available, "
                        f"requested {request.seat_count}",
                    )
                    return

                await conn.execute(
                    "UPDATE flights SET available_seats = available_seats - $1 WHERE id = $2",
                    request.seat_count,
                    request.flight_id,
                )
                rsv_row = await conn.fetchrow(
                    """
                    INSERT INTO seat_reservations (flight_id, booking_ref, seats_held)
                    VALUES ($1, $2, $3)
                    RETURNING *
                    """,
                    request.flight_id,
                    request.booking_id,
                    request.seat_count,
                )

        # Evict stale cache entries
        await redis_op("delete", f"flight:{request.flight_id}")
        await _evict_search_keys()
        logger.info(
            "ReserveSeats: flight=%s seats=%s booking_ref=%s",
            request.flight_id, request.seat_count, request.booking_id,
        )

        return flight_pb2.ReserveSeatsResponse(
            reservation=_row_to_reservation(rsv_row)
        )

    # ── ReleaseReservation ──────────────────────────────────────────
    async def ReleaseReservation(self, request, context):
        pool = await get_pool()

        async with pool.acquire() as conn:
            async with conn.transaction():
                rsv_row = await conn.fetchrow(
                    """
                    SELECT * FROM seat_reservations
                    WHERE  booking_ref = $1 AND rsv_status = 'ACTIVE'
                    FOR UPDATE
                    """,
                    request.booking_id,
                )
                if rsv_row is None:
                    await context.abort(
                        grpc.StatusCode.NOT_FOUND,
                        f"No active reservation for booking_ref={request.booking_id}",
                    )
                    return

                await conn.execute(
                    "UPDATE flights SET available_seats = available_seats + $1 WHERE id = $2",
                    rsv_row["seats_held"],
                    rsv_row["flight_id"],
                )
                await conn.execute(
                    "UPDATE seat_reservations SET rsv_status = 'RELEASED' WHERE id = $1",
                    rsv_row["id"],
                )

        await redis_op("delete", f"flight:{rsv_row['flight_id']}")
        await _evict_search_keys()
        logger.info("ReleaseReservation: booking_ref=%s", request.booking_id)

        return flight_pb2.ReleaseReservationResponse(released=True)


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

async def serve() -> None:
    await get_redis()

    server = grpc.aio.server(interceptors=[AuthInterceptor()])
    flight_pb2_grpc.add_FlightServiceServicer_to_server(FlightServicer(), server)

    addr = f"0.0.0.0:{GRPC_PORT}"
    server.add_insecure_port(addr)

    logger.info("Flight Service listening on %s", addr)
    await server.start()

    try:
        await server.wait_for_termination()
    finally:
        await server.stop(grace=5)


if __name__ == "__main__":
    asyncio.run(serve())
