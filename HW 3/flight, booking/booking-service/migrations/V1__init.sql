CREATE TYPE booking_status AS ENUM ('CONFIRMED', 'CANCELLED');

CREATE TABLE bookings (
    id               UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          VARCHAR(100)    NOT NULL,
    flight_id        BIGINT          NOT NULL,
    passenger_name   VARCHAR(200)    NOT NULL,
    passenger_email  VARCHAR(200)    NOT NULL,
    seat_count       INT             NOT NULL CHECK (seat_count > 0),
    total_price      NUMERIC(10, 2)  NOT NULL CHECK (total_price > 0),
    status           booking_status  NOT NULL DEFAULT 'CONFIRMED',
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- индекс для быстрого поиска бронирований по пользователю
CREATE INDEX idx_bookings_user_id ON bookings(user_id);
