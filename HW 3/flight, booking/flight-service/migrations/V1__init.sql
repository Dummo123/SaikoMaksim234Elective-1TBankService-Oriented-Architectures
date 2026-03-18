CREATE TYPE flight_status AS ENUM ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED');
CREATE TYPE reservation_status AS ENUM ('ACTIVE', 'RELEASED', 'EXPIRED');

CREATE TABLE flights (
    id               BIGSERIAL PRIMARY KEY,
    flight_number    VARCHAR(10)     NOT NULL,
    airline          VARCHAR(100)    NOT NULL,
    origin_code      CHAR(3)         NOT NULL,
    destination_code CHAR(3)         NOT NULL,
    departure_time   TIMESTAMPTZ     NOT NULL,
    arrival_time     TIMESTAMPTZ     NOT NULL,
    total_seats      INT             NOT NULL CHECK (total_seats > 0),
    available_seats  INT             NOT NULL CHECK (available_seats >= 0),
    price            NUMERIC(10, 2)  NOT NULL CHECK (price > 0),
    status           flight_status   NOT NULL DEFAULT 'SCHEDULED',
    CONSTRAINT chk_available_lte_total CHECK (available_seats <= total_seats)
);

-- уникальность номера рейса + дата вылета через индекс
CREATE UNIQUE INDEX uq_flight_number_date ON flights (flight_number, DATE(departure_time AT TIME ZONE 'UTC'));

CREATE TABLE seat_reservations (
    id          BIGSERIAL           PRIMARY KEY,
    flight_id   BIGINT              NOT NULL REFERENCES flights(id),
    booking_id  UUID                NOT NULL UNIQUE,
    seat_count  INT                 NOT NULL CHECK (seat_count > 0),
    status      reservation_status  NOT NULL DEFAULT 'ACTIVE',
    created_at  TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_reservations_booking_id ON seat_reservations(booking_id);

INSERT INTO flights (flight_number, airline, origin_code, destination_code,
                     departure_time, arrival_time, total_seats, available_seats, price)
VALUES
    ('SU1234', 'Aeroflot', 'SVO', 'LED', '2026-04-01 10:00:00+03', '2026-04-01 11:20:00+03', 100, 100, 4500.00),
    ('SU5678', 'Aeroflot', 'SVO', 'LED', '2026-04-01 18:00:00+03', '2026-04-01 19:20:00+03', 80,  80,  3900.00),
    ('DP401',  'Pobeda',   'VKO', 'AER', '2026-04-02 09:00:00+03', '2026-04-02 11:30:00+03', 180, 180, 2100.00);
