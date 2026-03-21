"""
Booking Service — FastAPI REST API.

Endpoints:
  GET  /flights                       — search flights (proxy to Flight Service)
  GET  /flights/{flight_id}           — single flight details
  POST /bookings                      — create booking
  GET  /bookings/{booking_id}         — get booking by ID
  POST /bookings/{booking_id}/cancel  — cancel booking
  GET  /bookings?customer_id=...      — list bookings for a customer
"""

import logging
import uuid
from contextlib import asynccontextmanager
from typing import Optional

import grpc
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

import flight_pb2
from db import get_pool
from grpc_client import (
    CircuitOpenError,
    call_with_retry,
    flight_channel,
    get_stub,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [booking-svc] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Lifespan
# ------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_pool()
    logger.info("Booking Service ready on :8080")
    yield


app = FastAPI(title="Booking Service", version="1.0.0", lifespan=lifespan)


# ------------------------------------------------------------------
# Request schema
# ------------------------------------------------------------------

class BookingRequest(BaseModel):
    customer_id:     str
    flight_id:       int
    traveller_name:  str
    traveller_email: str
    seat_count:      int


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _parse_uuid(raw: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid UUID: {raw!r}")


def _flight_to_dict(f: flight_pb2.Flight) -> dict:
    return {
        "id":              f.id,
        "flight_number":   f.flight_number,
        "airline":         f.airline,
        "origin":          f.origin,
        "destination":     f.destination,
        "departure_time":  f.departure_time.ToDatetime().isoformat(),
        "arrival_time":    f.arrival_time.ToDatetime().isoformat(),
        "total_seats":     f.total_seats,
        "available_seats": f.available_seats,
        "price":           f.price,
        "status":          flight_pb2.FlightStatus.Name(f.status),
    }


def _booking_row(row) -> dict:
    return {
        "id":              str(row["id"]),
        "customer_id":     row["customer_id"],
        "flight_id":       row["flight_id"],
        "traveller_name":  row["traveller_name"],
        "traveller_email": row["traveller_email"],
        "seat_count":      row["seat_count"],
        "total_cost":      float(row["total_cost"]),
        "status":          row["booking_status"],
        "created_at":      row["created_at"].isoformat(),
    }


def _handle_grpc_error(exc: grpc.RpcError) -> None:
    code = exc.code()
    if code == grpc.StatusCode.NOT_FOUND:
        raise HTTPException(status_code=404, detail=exc.details())
    if code == grpc.StatusCode.RESOURCE_EXHAUSTED:
        raise HTTPException(status_code=409, detail=exc.details())
    raise HTTPException(status_code=502, detail=exc.details())


# ------------------------------------------------------------------
# Flight proxy endpoints
# ------------------------------------------------------------------

@app.get("/flights")
async def search_flights(
    origin:      str,
    destination: str,
    date:        Optional[str] = Query(default=None),
):
    async with flight_channel() as ch:
        stub = get_stub(ch)
        try:
            resp = await call_with_retry(
                stub.SearchFlights,
                flight_pb2.SearchFlightsRequest(
                    origin=origin,
                    destination=destination,
                    date=date or "",
                ),
            )
        except CircuitOpenError as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        except grpc.RpcError as exc:
            _handle_grpc_error(exc)

    return {"flights": [_flight_to_dict(f) for f in resp.flights]}


@app.get("/flights/{flight_id}")
async def get_flight(flight_id: int):
    async with flight_channel() as ch:
        stub = get_stub(ch)
        try:
            resp = await call_with_retry(
                stub.GetFlight,
                flight_pb2.GetFlightRequest(flight_id=flight_id),
            )
        except CircuitOpenError as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        except grpc.RpcError as exc:
            _handle_grpc_error(exc)

    return _flight_to_dict(resp.flight)


# ------------------------------------------------------------------
# Booking endpoints
# ------------------------------------------------------------------

@app.post("/bookings", status_code=201)
async def create_booking(body: BookingRequest):
    booking_id = str(uuid.uuid4())

    async with flight_channel() as ch:
        stub = get_stub(ch)

        # Step 1 — verify flight exists and capture current price
        try:
            flight_resp = await call_with_retry(
                stub.GetFlight,
                flight_pb2.GetFlightRequest(flight_id=body.flight_id),
            )
        except CircuitOpenError as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        except grpc.RpcError as exc:
            _handle_grpc_error(exc)

        total_cost = body.seat_count * flight_resp.flight.price

        # Step 2 — atomically reserve seats on Flight Service
        try:
            await call_with_retry(
                stub.ReserveSeats,
                flight_pb2.ReserveSeatsRequest(
                    flight_id=body.flight_id,
                    seat_count=body.seat_count,
                    booking_id=booking_id,
                ),
            )
        except CircuitOpenError as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        except grpc.RpcError as exc:
            _handle_grpc_error(exc)

    # Step 3 — persist booking only after successful reservation
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO bookings
                (id, customer_id, flight_id, traveller_name, traveller_email,
                 seat_count, total_cost)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING *
            """,
            uuid.UUID(booking_id),
            body.customer_id,
            body.flight_id,
            body.traveller_name,
            body.traveller_email,
            body.seat_count,
            total_cost,
        )

    logger.info(
        "Booking created id=%s flight=%s seats=%s cost=%.2f",
        booking_id, body.flight_id, body.seat_count, total_cost,
    )
    return _booking_row(row)


@app.get("/bookings/{booking_id}")
async def get_booking(booking_id: str):
    uid = _parse_uuid(booking_id)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM bookings WHERE id = $1", uid)

    if row is None:
        raise HTTPException(status_code=404, detail="Booking not found")

    return _booking_row(row)


@app.post("/bookings/{booking_id}/cancel")
async def cancel_booking(booking_id: str):
    uid = _parse_uuid(booking_id)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM bookings WHERE id = $1", uid)

        if row is None:
            raise HTTPException(status_code=404, detail="Booking not found")

        if row["booking_status"] != "CONFIRMED":
            raise HTTPException(
                status_code=409,
                detail=f"Cannot cancel booking with status '{row['booking_status']}'",
            )

        async with flight_channel() as ch:
            stub = get_stub(ch)
            try:
                await call_with_retry(
                    stub.ReleaseReservation,
                    flight_pb2.ReleaseReservationRequest(booking_id=booking_id),
                )
            except CircuitOpenError as exc:
                raise HTTPException(status_code=503, detail=str(exc))
            except grpc.RpcError as exc:
                _handle_grpc_error(exc)

        await conn.execute(
            "UPDATE bookings SET booking_status = 'CANCELLED' WHERE id = $1",
            uid,
        )

    logger.info("Booking cancelled id=%s", booking_id)
    return {"id": booking_id, "status": "CANCELLED"}


@app.get("/bookings")
async def list_bookings(customer_id: str = Query(...)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM bookings WHERE customer_id = $1 ORDER BY created_at DESC",
            customer_id,
        )

    return {"bookings": [_booking_row(r) for r in rows]}
