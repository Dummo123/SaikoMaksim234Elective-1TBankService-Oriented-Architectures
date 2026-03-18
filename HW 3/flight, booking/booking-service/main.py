import logging
import uuid
from contextlib import asynccontextmanager
from typing import Optional

import grpc
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import flight_pb2
from db import get_pool
from grpc_client import (
    CircuitBreakerOpenError,
    get_channel,
    get_stub,
    grpc_call_with_retry,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CreateBookingRequest(BaseModel):
    user_id: str
    flight_id: int
    passenger_name: str
    passenger_email: str
    seat_count: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_pool()
    logger.info("Booking Service started")
    yield


app = FastAPI(title="Booking Service", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def parse_booking_uuid(booking_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(booking_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid booking_id")


def flight_to_dict(f: flight_pb2.Flight) -> dict:
    return {
        "id": f.id,
        "flight_number": f.flight_number,
        "airline": f.airline,
        "origin": f.origin,
        "destination": f.destination,
        "departure_time": f.departure_time.ToDatetime().isoformat(),
        "arrival_time": f.arrival_time.ToDatetime().isoformat(),
        "total_seats": f.total_seats,
        "available_seats": f.available_seats,
        "price": f.price,
        "status": flight_pb2.FlightStatus.Name(f.status),
    }


@app.get("/flights")
async def search_flights(origin: str, destination: str, date: Optional[str] = None):
    async with get_channel() as channel:
        stub = get_stub(channel)
        try:
            response = await grpc_call_with_retry(
                stub.SearchFlights,
                flight_pb2.SearchFlightsRequest(
                    origin=origin,
                    destination=destination,
                    date=date or "",
                ),
            )
        except CircuitBreakerOpenError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except grpc.RpcError as e:
            raise HTTPException(status_code=502, detail=str(e.details()))

    return {"flights": [flight_to_dict(f) for f in response.flights]}


@app.get("/flights/{flight_id}")
async def get_flight(flight_id: int):
    async with get_channel() as channel:
        stub = get_stub(channel)
        try:
            response = await grpc_call_with_retry(
                stub.GetFlight,
                flight_pb2.GetFlightRequest(flight_id=flight_id),
            )
        except CircuitBreakerOpenError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise HTTPException(status_code=404, detail="Flight not found")
            raise HTTPException(status_code=502, detail=str(e.details()))

    return flight_to_dict(response.flight)


@app.post("/bookings", status_code=201)
async def create_booking(req: CreateBookingRequest):
    booking_id = str(uuid.uuid4())

    async with get_channel() as channel:
        stub = get_stub(channel)

        try:
            flight_resp = await grpc_call_with_retry(
                stub.GetFlight,
                flight_pb2.GetFlightRequest(flight_id=req.flight_id),
            )
        except CircuitBreakerOpenError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                raise HTTPException(status_code=404, detail="Flight not found")
            raise HTTPException(status_code=502, detail=str(e.details()))

        flight = flight_resp.flight
        total_price = req.seat_count * flight.price

        try:
            await grpc_call_with_retry(
                stub.ReserveSeats,
                flight_pb2.ReserveSeatsRequest(
                    flight_id=req.flight_id,
                    seat_count=req.seat_count,
                    booking_id=booking_id,
                ),
            )
        except CircuitBreakerOpenError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                raise HTTPException(status_code=409, detail="Not enough seats available")
            raise HTTPException(status_code=502, detail=str(e.details()))

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO bookings
                (id, user_id, flight_id, passenger_name, passenger_email, seat_count, total_price, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'CONFIRMED')
            RETURNING *
            """,
            uuid.UUID(booking_id),
            req.user_id,
            req.flight_id,
            req.passenger_name,
            req.passenger_email,
            req.seat_count,
            total_price,
        )

    logger.info(f"Booking created: id={booking_id}")
    return {
        "id": str(row["id"]),
        "user_id": row["user_id"],
        "flight_id": row["flight_id"],
        "passenger_name": row["passenger_name"],
        "passenger_email": row["passenger_email"],
        "seat_count": row["seat_count"],
        "total_price": float(row["total_price"]),
        "status": row["status"],
        "created_at": row["created_at"].isoformat(),
    }


@app.get("/bookings/{booking_id}")
async def get_booking(booking_id: str):
    booking_uuid = parse_booking_uuid(booking_id)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM bookings WHERE id = $1", booking_uuid)

    if not row:
        raise HTTPException(status_code=404, detail="Booking not found")

    return {
        "id": str(row["id"]),
        "user_id": row["user_id"],
        "flight_id": row["flight_id"],
        "passenger_name": row["passenger_name"],
        "passenger_email": row["passenger_email"],
        "seat_count": row["seat_count"],
        "total_price": float(row["total_price"]),
        "status": row["status"],
        "created_at": row["created_at"].isoformat(),
    }


@app.post("/bookings/{booking_id}/cancel")
async def cancel_booking(booking_id: str):
    booking_uuid = parse_booking_uuid(booking_id)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM bookings WHERE id = $1", booking_uuid)

        if not row:
            raise HTTPException(status_code=404, detail="Booking not found")

        if row["status"] != "CONFIRMED":
            raise HTTPException(status_code=409, detail="Booking is not CONFIRMED")

        async with get_channel() as channel:
            stub = get_stub(channel)
            try:
                await grpc_call_with_retry(
                    stub.ReleaseReservation,
                    flight_pb2.ReleaseReservationRequest(booking_id=booking_id),
                )
            except CircuitBreakerOpenError as e:
                raise HTTPException(status_code=503, detail=str(e))
            except grpc.RpcError as e:
                raise HTTPException(status_code=502, detail=str(e.details()))

        await conn.execute(
            "UPDATE bookings SET status = 'CANCELLED' WHERE id = $1",
            booking_uuid,
        )

    logger.info(f"Booking cancelled: id={booking_id}")
    return {"id": booking_id, "status": "CANCELLED"}


@app.get("/bookings")
async def list_bookings(user_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM bookings WHERE user_id = $1 ORDER BY created_at DESC",
            user_id,
        )

    return {
        "bookings": [
            {
                "id": str(r["id"]),
                "flight_id": r["flight_id"],
                "passenger_name": r["passenger_name"],
                "seat_count": r["seat_count"],
                "total_price": float(r["total_price"]),
                "status": r["status"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in rows
        ]
    }
