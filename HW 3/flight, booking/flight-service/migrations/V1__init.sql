-- Flight Service schema

CREATE TYPE flight_status AS ENUM ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED');
CREATE TYPE rsv_status    AS ENUM ('ACTIVE', 'RELEASED', 'EXPIRED');

CREATE TABLE flights (
    id               BIGSERIAL      PRIMARY KEY,
    flight_number    VARCHAR(10)    NOT NULL,
    airline          VARCHAR(100)   NOT NULL,
    origin_iata      CHAR(3)        NOT NULL,
    destination_iata CHAR(3)        NOT NULL,
    departs_at       TIMESTAMPTZ    NOT NULL,
    arrives_at       TIMESTAMPTZ    NOT NULL,
    total_seats      INT            NOT NULL CHECK (total_seats > 0),
    available_seats  INT            NOT NULL CHECK (available_seats >= 0),
    ticket_price     NUMERIC(10, 2) NOT NULL CHECK (ticket_price > 0),
    flight_status    flight_status  NOT NULL DEFAULT 'SCHEDULED',
    CONSTRAINT chk_seats_within_total CHECK (available_seats <= total_seats)
);

CREATE UNIQUE INDEX uq_flight_number_date
    ON flights (flight_number, DATE(departs_at AT TIME ZONE 'UTC'));

CREATE TABLE seat_reservations (
    id          BIGSERIAL   PRIMARY KEY,
    flight_id   BIGINT      NOT NULL REFERENCES flights(id),
    booking_ref UUID        NOT NULL UNIQUE,
    seats_held  INT         NOT NULL CHECK (seats_held > 0),
    rsv_status  rsv_status  NOT NULL DEFAULT 'ACTIVE',
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rsv_booking_ref ON seat_reservations(booking_ref);
CREATE INDEX idx_rsv_flight_id   ON seat_reservations(flight_id);

-- Seed flights for testing
INSERT INTO flights
    (flight_number, airline, origin_iata, destination_iata,
     departs_at, arrives_at, total_seats, available_seats, ticket_price)
VALUES
    ('SU1001', 'Aeroflot',    'SVO', 'LED',
     '2026-04-01 10:00:00+03', '2026-04-01 11:20:00+03', 100, 100, 4500.00),
    ('SU1002', 'Aeroflot',    'SVO', 'LED',
     '2026-04-01 18:00:00+03', '2026-04-01 19:20:00+03', 80,  80,  3900.00),
    ('DP401',  'Pobeda',      'VKO', 'AER',
     '2026-04-02 09:00:00+03', '2026-04-02 11:30:00+03', 180, 180, 2100.00),
    ('U6 301', 'Ural Airlines','SVO', 'AER',
     '2026-04-05 07:30:00+03', '2026-04-05 10:10:00+03', 160, 45,  6200.00);
