import asyncio
import json
import logging
import os
from datetime import timezone

import grpc
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from google.protobuf.timestamp_pb2 import Timestamp

import flight_pb2
import flight_pb2_grpc
from db import get_pool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_KEY = os.environ.get("INTERNAL_API_KEY", "secret-key")

REDIS_MODE = os.environ.get("REDIS_MODE", "standalone")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

REDIS_MASTER_NAME = os.environ.get("REDIS_MASTER_NAME", "mymaster")
REDIS_SENTINEL_HOST = os.environ.get("REDIS_SENTINEL_HOST", "redis-sentinel")
REDIS_SENTINEL_PORT = int(os.environ.get("REDIS_SENTINEL_PORT", 26379))

CACHE_TTL = 600

_redis = None
_sentinel = None


async def reset_redis():
    global _redis, _sentinel

    if _redis is not None:
        try:
            await _redis.close()
        except Exception:
            pass

    _redis = None
    _sentinel = None


async def get_redis():
    global _redis, _sentinel

    if _redis is not None:
        return _redis

    if REDIS_MODE == "sentinel":
        logger.info(
            f"Connecting to Redis Sentinel: {REDIS_SENTINEL_HOST}:{REDIS_SENTINEL_PORT}, master={REDIS_MASTER_NAME}"
        )
        _sentinel = aioredis.sentinel.Sentinel(
            [(REDIS_SENTINEL_HOST, REDIS_SENTINEL_PORT)],
            decode_responses=True,
            socket_timeout=1,
        )
        _redis = _sentinel.master_for(
            REDIS_MASTER_NAME,
            decode_responses=True,
            socket_timeout=1,
        )
        await _redis.ping()
        logger.info("Connected to Redis via Sentinel")
        return _redis

    logger.info(f"Connecting to standalone Redis: {REDIS_HOST}:{REDIS_PORT}")
    _redis = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_timeout=1,
    )
    await _redis.ping()
    logger.info("Connected to standalone Redis")
    return _redis


async def redis_call(method_name, *args):
    try:
        redis = await get_redis()
        method = getattr(redis, method_name)
        return await method(*args)
    except RedisConnectionError:
        logger.warning("Redis connection lost, reconnecting through configured backend")
        await reset_redis()
        redis = await get_redis()
        method = getattr(redis, method_name)
        return await method(*args)


class AuthInterceptor(grpc.aio.ServerInterceptor):
    async def intercept_service(self, continuation, handler_call_details):
        metadata = dict(handler_call_details.invocation_metadata)
        if metadata.get("x-api-key", "") != API_KEY:
            async def abort(request, context):
                await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or missing API key")
            return grpc.unary_unary_rpc_method_handler(abort)
        return await continuation(handler_call_details)


def row_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "flight_number": row["flight_number"],
        "airline": row["airline"],
        "origin_code": row["origin_code"],
        "destination_code": row["destination_code"],
        "departure_time": row["departure_time"].isoformat(),
        "arrival_time": row["arrival_time"].isoformat(),
        "total_seats": row["total_seats"],
        "available_seats": row["available_seats"],
        "price": float(row["price"]),
        "status": row["status"],
    }


def dict_to_flight(d: dict) -> flight_pb2.Flight:
    from datetime import datetime

    dep = Timestamp()
    dep.FromDatetime(datetime.fromisoformat(d["departure_time"]).replace(tzinfo=timezone.utc))

    arr = Timestamp()
    arr.FromDatetime(datetime.fromisoformat(d["arrival_time"]).replace(tzinfo=timezone.utc))

    return flight_pb2.Flight(
        id=d["id"],
        flight_number=d["flight_number"],
        airline=d["airline"],
        origin=d["origin_code"],
        destination=d["destination_code"],
        departure_time=dep,
        arrival_time=arr,
        total_seats=d["total_seats"],
        available_seats=d["available_seats"],
        price=d["price"],
        status=flight_pb2.FlightStatus.Value(d["status"]),
    )


class FlightServiceServicer(flight_pb2_grpc.FlightServiceServicer):
    async def SearchFlights(self, request, context):
        cache_key = f"search:{request.origin}:{request.destination}:{request.date}"

        cached = await redis_call("get", cache_key)
        if cached:
            logger.info(f"CACHE HIT: {cache_key}")
            flights_data = json.loads(cached)
            return flight_pb2.SearchFlightsResponse(
                flights=[dict_to_flight(f) for f in flights_data]
            )

        logger.info(f"CACHE MISS: {cache_key}")
        pool = await get_pool()

        query = """
            SELECT * FROM flights
            WHERE origin_code = $1 AND destination_code = $2 AND status = 'SCHEDULED'
        """
        args = [request.origin, request.destination]

        if request.date:
            query += " AND DATE(departure_time) = $3"
            args.append(request.date)

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *args)

        flights_data = [row_to_dict(r) for r in rows]
        await redis_call("setex", cache_key, CACHE_TTL, json.dumps(flights_data))
        logger.info(f"CACHE SET: {cache_key} TTL={CACHE_TTL}s")

        return flight_pb2.SearchFlightsResponse(
            flights=[dict_to_flight(f) for f in flights_data]
        )

    async def GetFlight(self, request, context):
        cache_key = f"flight:{request.flight_id}"

        cached = await redis_call("get", cache_key)
        if cached:
            logger.info(f"CACHE HIT: {cache_key}")
            return flight_pb2.GetFlightResponse(flight=dict_to_flight(json.loads(cached)))

        logger.info(f"CACHE MISS: {cache_key}")
        pool = await get_pool()

        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM flights WHERE id = $1", request.flight_id)

        if not row:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Flight {request.flight_id} not found")

        flight_data = row_to_dict(row)
        await redis_call("setex", cache_key, CACHE_TTL, json.dumps(flight_data))
        logger.info(f"CACHE SET: {cache_key} TTL={CACHE_TTL}s")

        return flight_pb2.GetFlightResponse(flight=dict_to_flight(flight_data))

    async def ReserveSeats(self, request, context):
        pool = await get_pool()

        async with pool.acquire() as conn:
            async with conn.transaction():
                existing = await conn.fetchrow(
                    "SELECT * FROM seat_reservations WHERE booking_id = $1",
                    request.booking_id,
                )
                if existing:
                    logger.info(f"ReserveSeats idempotent hit: booking={request.booking_id}")
                    created_at = Timestamp()
                    created_at.FromDatetime(existing["created_at"].replace(tzinfo=timezone.utc))
                    reservation = flight_pb2.SeatReservation(
                        id=existing["id"],
                        flight_id=existing["flight_id"],
                        booking_id=str(existing["booking_id"]),
                        seat_count=existing["seat_count"],
                        status=flight_pb2.ReservationStatus.Value(existing["status"]),
                        created_at=created_at,
                    )
                    return flight_pb2.ReserveSeatsResponse(reservation=reservation)

                row = await conn.fetchrow(
                    "SELECT * FROM flights WHERE id = $1 FOR UPDATE",
                    request.flight_id,
                )
                if not row:
                    await context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")

                if row["available_seats"] < request.seat_count:
                    await context.abort(
                        grpc.StatusCode.RESOURCE_EXHAUSTED,
                        f"Not enough seats: available={row['available_seats']}, requested={request.seat_count}",
                    )

                await conn.execute(
                    "UPDATE flights SET available_seats = available_seats - $1 WHERE id = $2",
                    request.seat_count,
                    request.flight_id,
                )

                res_row = await conn.fetchrow(
                    """
                    INSERT INTO seat_reservations (flight_id, booking_id, seat_count, status)
                    VALUES ($1, $2, $3, 'ACTIVE')
                    RETURNING *
                    """,
                    request.flight_id,
                    request.booking_id,
                    request.seat_count,
                )

        await redis_call("delete", f"flight:{request.flight_id}")
        redis = await get_redis()
        async for key in redis.scan_iter("search:*"):
            await redis_call("delete", key)
        logger.info(f"CACHE INVALIDATED: flight:{request.flight_id} + search:*")

        created_at = Timestamp()
        created_at.FromDatetime(res_row["created_at"].replace(tzinfo=timezone.utc))
        reservation = flight_pb2.SeatReservation(
            id=res_row["id"],
            flight_id=res_row["flight_id"],
            booking_id=str(res_row["booking_id"]),
            seat_count=res_row["seat_count"],
            status=flight_pb2.ReservationStatus.ACTIVE,
            created_at=created_at,
        )
        logger.info(f"ReserveSeats: flight={request.flight_id} seats={request.seat_count}")
        return flight_pb2.ReserveSeatsResponse(reservation=reservation)

    async def ReleaseReservation(self, request, context):
        pool = await get_pool()

        async with pool.acquire() as conn:
            async with conn.transaction():
                res = await conn.fetchrow(
                    "SELECT * FROM seat_reservations WHERE booking_id = $1 AND status = 'ACTIVE'",
                    request.booking_id,
                )
                if not res:
                    await context.abort(
                        grpc.StatusCode.NOT_FOUND,
                        f"Active reservation for booking {request.booking_id} not found",
                    )

                await conn.execute(
                    "UPDATE flights SET available_seats = available_seats + $1 WHERE id = $2",
                    res["seat_count"],
                    res["flight_id"],
                )
                await conn.execute(
                    "UPDATE seat_reservations SET status = 'RELEASED' WHERE id = $1",
                    res["id"],
                )

        await redis_call("delete", f"flight:{res['flight_id']}")
        redis = await get_redis()
        async for key in redis.scan_iter("search:*"):
            await redis_call("delete", key)
        logger.info(f"CACHE INVALIDATED: flight:{res['flight_id']} + search:*")
        logger.info(f"ReleaseReservation: booking={request.booking_id}")

        return flight_pb2.ReleaseReservationResponse(success=True)


async def serve():
    await get_redis()

    server = grpc.aio.server(interceptors=[AuthInterceptor()])
    flight_pb2_grpc.add_FlightServiceServicer_to_server(FlightServiceServicer(), server)

    port = os.environ.get("GRPC_PORT", "50051")
    server.add_insecure_port(f"[::]:{port}")

    logger.info(f"Flight Service starting on port {port}")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
