-- Booking Service schema

CREATE TYPE booking_status AS ENUM ('CONFIRMED', 'CANCELLED');

CREATE TABLE bookings (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id     VARCHAR(100)    NOT NULL,
    flight_id       BIGINT          NOT NULL,
    traveller_name  VARCHAR(255)    NOT NULL,
    traveller_email VARCHAR(255)    NOT NULL,
    seat_count      INT             NOT NULL CHECK (seat_count > 0),
    total_cost      NUMERIC(12, 2)  NOT NULL CHECK (total_cost > 0),
    booking_status  booking_status  NOT NULL DEFAULT 'CONFIRMED',
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_bookings_customer ON bookings(customer_id);
CREATE INDEX idx_bookings_flight   ON bookings(flight_id);
