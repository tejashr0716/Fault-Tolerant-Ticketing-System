-- ============================================================================
-- PRODUCTION-READY POSTGRESQL DATABASE SCHEMA
-- HIGH-CONCURRENCY MOVIE TICKET BOOKING SYSTEM (BENGALURU FOCUS)
-- ============================================================================
-- Design Highlights:
-- 1. Double-booking prevention via partial unique indexes at the database level.
-- 2. ACID-compliant wallet system with CHECK constraints (balance >= 0),
--    optimistic locking version columns, and comprehensive audit trail logging.
-- 3. Idempotency guarantees for booking and wallet transaction operations.
-- 4. Bengaluru-specialized theater registry with Geo/Pincode/Metadata support.
-- 5. Atomic procedures for seat reservation, expiration sweeps, and payment debits.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. EXTENSIONS & SETUP
-- ----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Drop existing tables/types in reverse dependency order for clean migrations
DROP VIEW IF EXISTS view_show_seat_availability CASCADE;
DROP VIEW IF EXISTS view_active_user_bookings CASCADE;

DROP TABLE IF EXISTS wallet_transactions CASCADE;
DROP TABLE IF EXISTS wallets CASCADE;
DROP TABLE IF EXISTS show_seats CASCADE;
DROP TABLE IF EXISTS bookings CASCADE;
DROP TABLE IF EXISTS shows CASCADE;
DROP TABLE IF EXISTS seats CASCADE;
DROP TABLE IF EXISTS screens CASCADE;
DROP TABLE IF EXISTS movies CASCADE;
DROP TABLE IF EXISTS theaters CASCADE;
DROP TABLE IF EXISTS users CASCADE;

DROP FUNCTION IF EXISTS update_updated_at_column CASCADE;
DROP FUNCTION IF EXISTS hold_show_seats CASCADE;
DROP FUNCTION IF EXISTS debit_wallet_for_booking CASCADE;
DROP FUNCTION IF EXISTS release_expired_seat_holds CASCADE;

-- ----------------------------------------------------------------------------
-- 1. SHARED UTILITY TRIGGER FOR UPDATED_AT TIMESTAMPS
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------------
-- 2. USERS TABLE
-- ----------------------------------------------------------------------------
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(150) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(20) NOT NULL UNIQUE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_user_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
    CONSTRAINT chk_user_phone_length CHECK (LENGTH(TRIM(phone)) >= 10)
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_phone ON users(phone);

CREATE TRIGGER trg_users_updated_at
BEFORE UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE users IS 'Registered end-users and customers booking movie tickets';
COMMENT ON COLUMN users.phone IS 'Indian phone number with country/area code (e.g., +919876543210)';

-- ----------------------------------------------------------------------------
-- 3. THEATERS TABLE (FOCUSED ONLY ON BENGALURU)
-- ----------------------------------------------------------------------------
CREATE TABLE theaters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(200) NOT NULL,
    brand VARCHAR(100) NOT NULL, -- PVR, INOX, Cinepolis, etc.
    location VARCHAR(255) NOT NULL, -- Mall, Street or Landmark
    area VARCHAR(100) NOT NULL, -- Koramangala, Rajajinagar, Bannerghatta Road, etc.
    city VARCHAR(50) NOT NULL DEFAULT 'Bengaluru',
    pincode VARCHAR(10) NOT NULL,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    total_screens INTEGER NOT NULL DEFAULT 1 CHECK (total_screens > 0),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb, -- amenities, parking, food court, accessibility
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_theaters_city_bengaluru CHECK (city = 'Bengaluru'),
    CONSTRAINT chk_theaters_pincode_blr CHECK (pincode ~ '^560[0-9]{3}$')
);

CREATE INDEX idx_theaters_area ON theaters(area);
CREATE INDEX idx_theaters_brand ON theaters(brand);
CREATE INDEX idx_theaters_pincode ON theaters(pincode);
CREATE INDEX idx_theaters_metadata_gin ON theaters USING GIN(metadata);

CREATE TRIGGER trg_theaters_updated_at
BEFORE UPDATE ON theaters
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE theaters IS 'Multiplexes and single screens strictly localized in Bengaluru';
COMMENT ON COLUMN theaters.metadata IS 'JSONB holding amenities, parking levels, gourmet options, and wheelchair access';

-- ----------------------------------------------------------------------------
-- 4. SCREENS TABLE
-- ----------------------------------------------------------------------------
CREATE TABLE screens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    theater_id UUID NOT NULL REFERENCES theaters(id) ON DELETE CASCADE,
    screen_number INTEGER NOT NULL CHECK (screen_number > 0),
    name VARCHAR(100) NOT NULL, -- e.g. "Audi 1 - IMAX with Laser", "Audi 4 - 4DX"
    screen_type VARCHAR(50) NOT NULL DEFAULT 'STANDARD' 
        CHECK (screen_type IN ('STANDARD', 'IMAX', '4DX', 'ICE', 'DOLBY_ATMOS', 'GOLD_CLASS', 'LUXE', 'P_XL')),
    sound_system VARCHAR(100) NOT NULL DEFAULT 'Dolby 7.1',
    total_capacity INTEGER NOT NULL CHECK (total_capacity > 0),
    seating_layout JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_screens_theater_screen_number UNIQUE (theater_id, screen_number)
);

CREATE INDEX idx_screens_theater_id ON screens(theater_id);
CREATE INDEX idx_screens_type ON screens(screen_type);

CREATE TRIGGER trg_screens_updated_at
BEFORE UPDATE ON screens
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE screens IS 'Individual auditoriums/screens within a Bengaluru theater';

-- ----------------------------------------------------------------------------
-- 5. MOVIES TABLE
-- ----------------------------------------------------------------------------
CREATE TABLE movies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(255) NOT NULL,
    original_language VARCHAR(50) NOT NULL,
    languages_available TEXT[] NOT NULL DEFAULT ARRAY['Kannada']::TEXT[],
    genres TEXT[] NOT NULL,
    duration_minutes INTEGER NOT NULL CHECK (duration_minutes > 0),
    certification VARCHAR(10) NOT NULL CHECK (certification IN ('U', 'UA', 'UA 13+', 'UA 16+', 'A', 'S')),
    release_date DATE NOT NULL,
    synopsis TEXT,
    poster_url VARCHAR(500),
    status VARCHAR(20) NOT NULL DEFAULT 'NOW_SHOWING' CHECK (status IN ('UPCOMING', 'NOW_SHOWING', 'ENDED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_movies_status ON movies(status);
CREATE INDEX idx_movies_title ON movies(title);
CREATE INDEX idx_movies_genres_gin ON movies USING GIN(genres);

CREATE TRIGGER trg_movies_updated_at
BEFORE UPDATE ON movies
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE movies IS 'Movie catalog details, censor rating, and supported languages';

-- ----------------------------------------------------------------------------
-- 6. SEATS TABLE (PHYSICAL SEAT INVENTORY PER SCREEN)
-- ----------------------------------------------------------------------------
CREATE TABLE seats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    screen_id UUID NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
    seat_number VARCHAR(10) NOT NULL, -- e.g., 'A1', 'K12'
    row_label VARCHAR(5) NOT NULL,    -- 'A', 'B', 'K'
    column_number INTEGER NOT NULL CHECK (column_number > 0),
    tier VARCHAR(30) NOT NULL DEFAULT 'CLASSIC' CHECK (tier IN ('CLASSIC', 'PRIME', 'RECLINER', 'GOLD')),
    pricing_multiplier NUMERIC(4,2) NOT NULL DEFAULT 1.00 CHECK (pricing_multiplier >= 1.00),
    is_accessible BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_seats_screen_seat_number UNIQUE (screen_id, seat_number)
);

CREATE INDEX idx_seats_screen_tier ON seats(screen_id, tier);
CREATE INDEX idx_seats_screen_id ON seats(screen_id);

COMMENT ON TABLE seats IS 'Physical layout of seats for each auditorium';

-- ----------------------------------------------------------------------------
-- 7. SHOWS TABLE
-- ----------------------------------------------------------------------------
CREATE TABLE shows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    screen_id UUID NOT NULL REFERENCES screens(id) ON DELETE CASCADE,
    movie_id UUID NOT NULL REFERENCES movies(id) ON DELETE RESTRICT,
    audio_language VARCHAR(50) NOT NULL,
    subtitle_language VARCHAR(50),
    experience_format VARCHAR(50) NOT NULL DEFAULT '2D' 
        CHECK (experience_format IN ('2D', '3D', 'IMAX 2D', 'IMAX 3D', '4DX 3D', 'ICE 3D')),
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    base_price NUMERIC(10,2) NOT NULL CHECK (base_price > 0),
    status VARCHAR(20) NOT NULL DEFAULT 'SCHEDULED' 
        CHECK (status IN ('SCHEDULED', 'BOOKINGS_OPEN', 'HOUSEFULL', 'STARTED', 'COMPLETED', 'CANCELLED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_shows_time_window CHECK (end_time > start_time)
);

CREATE INDEX idx_shows_screen_start_time ON shows(screen_id, start_time);
CREATE INDEX idx_shows_movie_start_time ON shows(movie_id, start_time);
CREATE INDEX idx_shows_status ON shows(status);

CREATE TRIGGER trg_shows_updated_at
BEFORE UPDATE ON shows
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE shows IS 'Scheduled screening of a movie on a screen at a specific time';

-- ----------------------------------------------------------------------------
-- 8. BOOKINGS TABLE
-- ----------------------------------------------------------------------------
CREATE TABLE bookings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    booking_reference VARCHAR(32) NOT NULL UNIQUE, -- Human-readable ref, e.g. 'BLR-202610-8X92F'
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    show_id UUID NOT NULL REFERENCES shows(id) ON DELETE RESTRICT,
    seat_count INTEGER NOT NULL CHECK (seat_count > 0 AND seat_count <= 10),
    ticket_amount NUMERIC(12,2) NOT NULL CHECK (ticket_amount >= 0.00),
    convenience_fee NUMERIC(10,2) NOT NULL DEFAULT 0.00 CHECK (convenience_fee >= 0.00),
    tax_amount NUMERIC(10,2) NOT NULL DEFAULT 0.00 CHECK (tax_amount >= 0.00),
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0.00),
    status VARCHAR(30) NOT NULL DEFAULT 'INITIATED' 
        CHECK (status IN ('INITIATED', 'PENDING_PAYMENT', 'CONFIRMED', 'CANCELLED', 'EXPIRED', 'FAILED')),
    idempotency_key VARCHAR(128) UNIQUE, -- Prevents duplicate payment/order attempts
    payment_method VARCHAR(30) CHECK (payment_method IN ('WALLET', 'UPI', 'CREDIT_CARD', 'DEBIT_CARD', 'NET_BANKING')),
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    confirmed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_bookings_user_id ON bookings(user_id);
CREATE INDEX idx_bookings_show_id ON bookings(show_id);
CREATE INDEX idx_bookings_status ON bookings(status);
CREATE INDEX idx_bookings_created_at ON bookings(created_at DESC);
CREATE INDEX idx_bookings_idempotency_key ON bookings(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TRIGGER trg_bookings_updated_at
BEFORE UPDATE ON bookings
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE bookings IS 'Master booking record tying the customer, show, and transaction';

-- ----------------------------------------------------------------------------
-- 9. SHOW_SEATS / RESERVATIONS TABLE (HIGH CONCURRENCY SEAT LOCKING)
-- ----------------------------------------------------------------------------
-- Double-Booking Prevention Architecture:
-- 1. A seat begins as AVAILABLE (or unassigned).
-- 2. When a user selects a seat, it transitions to HELD with hold_expires_at (e.g. +10 mins).
-- 3. On successful payment, it transitions to BOOKED with booking_id.
-- 4. If hold expires or order is cancelled, status becomes CANCELLED or reverts to AVAILABLE.
-- 5. CRITICAL: Partial Unique Indexes ensure that at most ONE active hold or booking exists
--    for any (show_id, seat_number) where status != 'CANCELLED'.
-- ----------------------------------------------------------------------------
CREATE TABLE show_seats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    show_id UUID NOT NULL REFERENCES shows(id) ON DELETE CASCADE,
    seat_id UUID NOT NULL REFERENCES seats(id) ON DELETE RESTRICT,
    seat_number VARCHAR(10) NOT NULL,
    booking_id UUID REFERENCES bookings(id) ON DELETE SET NULL,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    price NUMERIC(10,2) NOT NULL CHECK (price >= 0.00),
    status VARCHAR(20) NOT NULL DEFAULT 'AVAILABLE' 
        CHECK (status IN ('AVAILABLE', 'HELD', 'BOOKED', 'CANCELLED')),
    held_at TIMESTAMPTZ,
    hold_expires_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 1, -- Optimistic locking counter
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_show_seats_hold_expiry CHECK (
        (status = 'HELD' AND hold_expires_at IS NOT NULL) OR 
        (status != 'HELD')
    ),
    CONSTRAINT chk_show_seats_booked_requires_booking_id CHECK (
        (status = 'BOOKED' AND booking_id IS NOT NULL) OR
        (status != 'BOOKED')
    )
);

-- PARTIAL UNIQUE INDEXES: Strictly prevent double-booking at the database engine level!
CREATE UNIQUE INDEX uq_show_seats_active_prevent_double_booking 
    ON show_seats (show_id, seat_number) 
    WHERE (status != 'CANCELLED');

CREATE UNIQUE INDEX uq_show_seats_seat_id_active 
    ON show_seats (show_id, seat_id) 
    WHERE (status != 'CANCELLED');

-- High-performance query indexes
CREATE INDEX idx_show_seats_show_status ON show_seats(show_id, status);
CREATE INDEX idx_show_seats_booking_id ON show_seats(booking_id) WHERE booking_id IS NOT NULL;
CREATE INDEX idx_show_seats_user_id ON show_seats(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_show_seats_held_expiry ON show_seats(hold_expires_at) WHERE (status = 'HELD');

CREATE TRIGGER trg_show_seats_updated_at
BEFORE UPDATE ON show_seats
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE show_seats IS 'Dynamic per-show seat allocation with ACID double-booking prevention';
COMMENT ON INDEX uq_show_seats_active_prevent_double_booking IS 'Prevents concurrent holds and bookings on the same seat for any show';

-- ----------------------------------------------------------------------------
-- 10. WALLETS TABLE (ACID-COMPLIANT DIGITAL WALLET)
-- ----------------------------------------------------------------------------
CREATE TABLE wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    balance NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR' CHECK (currency = 'INR'),
    version BIGINT NOT NULL DEFAULT 1, -- For optimistic concurrency locking
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- STRICT CHECK CONSTRAINT: Guarantees zero negative balances under any concurrent race condition
    CONSTRAINT chk_wallet_balance_positive CHECK (balance >= 0.00)
);

CREATE INDEX idx_wallets_user_id ON wallets(user_id);

CREATE TRIGGER trg_wallets_updated_at
BEFORE UPDATE ON wallets
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMENT ON TABLE wallets IS 'Prepaid digital wallet for instantaneous, high-concurrency ticket checkouts';
COMMENT ON CONSTRAINT chk_wallet_balance_positive ON wallets IS 'Database-enforced rule preventing wallet overdraft or negative balances';

-- ----------------------------------------------------------------------------
-- 11. WALLET_TRANSACTIONS TABLE (ACID AUDIT LOG)
-- ----------------------------------------------------------------------------
CREATE TABLE wallet_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    transaction_type VARCHAR(30) NOT NULL 
        CHECK (transaction_type IN ('TOPUP', 'BOOKING_PAYMENT', 'BOOKING_REFUND', 'PROMOTIONAL_CREDIT', 'ADJUSTMENT')),
    amount NUMERIC(12,2) NOT NULL CHECK (amount != 0.00), -- Positive for Credit, Negative for Debit
    balance_before NUMERIC(12,2) NOT NULL CHECK (balance_before >= 0.00),
    balance_after NUMERIC(12,2) NOT NULL CHECK (balance_after >= 0.00),
    booking_id UUID REFERENCES bookings(id) ON DELETE SET NULL,
    idempotency_key VARCHAR(128) UNIQUE, -- Guarantees exact-once processing of financial operations
    reference_number VARCHAR(64) NOT NULL, -- External PG transaction or booking reference
    status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS' CHECK (status IN ('SUCCESS', 'FAILED', 'REVERSED')),
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_wallet_tx_balance_consistency CHECK (balance_after = balance_before + amount)
);

CREATE INDEX idx_wallet_tx_wallet_id ON wallet_transactions(wallet_id);
CREATE INDEX idx_wallet_tx_user_id ON wallet_transactions(user_id);
CREATE INDEX idx_wallet_tx_booking_id ON wallet_transactions(booking_id) WHERE booking_id IS NOT NULL;
CREATE INDEX idx_wallet_tx_created_at ON wallet_transactions(created_at DESC);
CREATE INDEX idx_wallet_tx_idempotency ON wallet_transactions(idempotency_key) WHERE idempotency_key IS NOT NULL;

COMMENT ON TABLE wallet_transactions IS 'Immutable financial audit log capturing every balance mutation with pre/post snapshots';

-- ----------------------------------------------------------------------------
-- 12. STORED PROCEDURES & ATOMIC CONCURRENCY FUNCTIONS
-- ----------------------------------------------------------------------------

-- Procedure 1: Atomically Hold Seats for 10 Minutes with Pessimistic Row Locking
CREATE OR REPLACE FUNCTION hold_show_seats(
    p_user_id UUID,
    p_show_id UUID,
    p_seat_numbers VARCHAR[],
    p_hold_duration_seconds INTEGER DEFAULT 600
)
RETURNS TABLE (
    seat_id UUID,
    seat_number VARCHAR,
    price NUMERIC,
    hold_expires_at TIMESTAMPTZ
) AS $$
DECLARE
    v_seat_count INTEGER;
    v_available_count INTEGER;
    v_expiry TIMESTAMPTZ;
BEGIN
    v_seat_count := ARRAY_LENGTH(p_seat_numbers, 1);
    IF v_seat_count IS NULL OR v_seat_count <= 0 THEN
        RAISE EXCEPTION 'No seats provided for hold';
    END IF;

    v_expiry := CURRENT_TIMESTAMP + (p_hold_duration_seconds || ' seconds')::INTERVAL;

    -- Clean up any expired holds for this show first to release seats
    UPDATE show_seats
    SET status = 'AVAILABLE',
        held_at = NULL,
        hold_expires_at = NULL,
        user_id = NULL,
        version = version + 1
    WHERE show_id = p_show_id
      AND status = 'HELD'
      AND show_seats.hold_expires_at < CURRENT_TIMESTAMP;

    -- Pessimistic Lock on requested seats for update
    -- Verifies they are AVAILABLE before transitioning to HELD
    RETURN QUERY
    WITH locked_available_seats AS (
        SELECT ss.id, ss.seat_id, ss.seat_number, ss.price
        FROM show_seats ss
        WHERE ss.show_id = p_show_id
          AND ss.seat_number = ANY(p_seat_numbers)
          AND ss.status = 'AVAILABLE'
        FOR UPDATE
    ),
    updated_seats AS (
        UPDATE show_seats ss
        SET status = 'HELD',
            user_id = p_user_id,
            held_at = CURRENT_TIMESTAMP,
            hold_expires_at = v_expiry,
            version = ss.version + 1
        FROM locked_available_seats las
        WHERE ss.id = las.id
        RETURNING ss.seat_id, ss.seat_number, ss.price, ss.hold_expires_at
    )
    SELECT us.seat_id, us.seat_number, us.price, us.hold_expires_at
    FROM updated_seats us;

    GET DIAGNOSTICS v_available_count = ROW_COUNT;
    IF v_available_count <> v_seat_count THEN
        RAISE EXCEPTION 'Concurrency conflict: One or more selected seats are already held or booked (requested: %, secured: %)', 
            v_seat_count, v_available_count;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Procedure 2: Atomic Wallet Debit & Audit Log Write for Instant Bookings
CREATE OR REPLACE FUNCTION debit_wallet_for_booking(
    p_user_id UUID,
    p_booking_id UUID,
    p_debit_amount NUMERIC,
    p_idempotency_key VARCHAR,
    p_reference_number VARCHAR,
    p_description TEXT DEFAULT 'Movie ticket booking payment'
)
RETURNS UUID AS $$
DECLARE
    v_wallet_id UUID;
    v_current_balance NUMERIC;
    v_new_balance NUMERIC;
    v_tx_id UUID;
BEGIN
    IF p_debit_amount <= 0 THEN
        RAISE EXCEPTION 'Debit amount must be strictly greater than zero. Received: %', p_debit_amount;
    END IF;

    -- Check idempotency: If already executed, return existing transaction ID
    SELECT id INTO v_tx_id
    FROM wallet_transactions
    WHERE idempotency_key = p_idempotency_key;

    IF v_tx_id IS NOT NULL THEN
        RETURN v_tx_id;
    END IF;

    -- Lock the wallet row with SELECT ... FOR UPDATE to serialize concurrent debits
    SELECT id, balance INTO v_wallet_id, v_current_balance
    FROM wallets
    WHERE user_id = p_user_id
    FOR UPDATE;

    IF v_wallet_id IS NULL THEN
        RAISE EXCEPTION 'Wallet not found for user: %', p_user_id;
    END IF;

    IF v_current_balance < p_debit_amount THEN
        RAISE EXCEPTION 'Insufficient wallet balance. Available: %, Required: %', v_current_balance, p_debit_amount;
    END IF;

    v_new_balance := v_current_balance - p_debit_amount;

    -- Update wallet balance and increment version
    UPDATE wallets
    SET balance = v_new_balance,
        version = version + 1
    WHERE id = v_wallet_id;

    -- Record atomic audit transaction log
    INSERT INTO wallet_transactions (
        wallet_id,
        user_id,
        transaction_type,
        amount,
        balance_before,
        balance_after,
        booking_id,
        idempotency_key,
        reference_number,
        status,
        description
    ) VALUES (
        v_wallet_id,
        p_user_id,
        'BOOKING_PAYMENT',
        -p_debit_amount,
        v_current_balance,
        v_new_balance,
        p_booking_id,
        p_idempotency_key,
        p_reference_number,
        'SUCCESS',
        p_description
    ) RETURNING id INTO v_tx_id;

    RETURN v_tx_id;
END;
$$ LANGUAGE plpgsql;

-- Procedure 3: Background Worker Sweeper to Expire Stale Holds
CREATE OR REPLACE FUNCTION release_expired_seat_holds()
RETURNS INTEGER AS $$
DECLARE
    v_released_count INTEGER;
BEGIN
    WITH released AS (
        UPDATE show_seats
        SET status = 'AVAILABLE',
            user_id = NULL,
            held_at = NULL,
            hold_expires_at = NULL,
            version = version + 1
        WHERE status = 'HELD'
          AND hold_expires_at < CURRENT_TIMESTAMP
        RETURNING id
    )
    SELECT COUNT(*) INTO v_released_count FROM released;

    RETURN v_released_count;
END;
$$ LANGUAGE plpgsql;

-- ----------------------------------------------------------------------------
-- 13. CONVENIENCE ANALYTICS & MONITORING VIEWS
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW view_show_seat_availability AS
SELECT 
    s.id AS show_id,
    m.title AS movie_title,
    t.name AS theater_name,
    t.area AS theater_area,
    sc.name AS screen_name,
    s.start_time,
    s.base_price,
    COUNT(ss.id) AS total_seats,
    COUNT(CASE WHEN ss.status = 'AVAILABLE' OR (ss.status = 'HELD' AND ss.hold_expires_at < CURRENT_TIMESTAMP) THEN 1 END) AS available_seats,
    COUNT(CASE WHEN ss.status = 'HELD' AND ss.hold_expires_at >= CURRENT_TIMESTAMP THEN 1 END) AS actively_held_seats,
    COUNT(CASE WHEN ss.status = 'BOOKED' THEN 1 END) AS booked_seats
FROM shows s
JOIN movies m ON s.movie_id = m.id
JOIN screens sc ON s.screen_id = sc.id
JOIN theaters t ON sc.theater_id = t.id
LEFT JOIN show_seats ss ON s.id = ss.show_id
GROUP BY s.id, m.title, t.name, t.area, sc.name, s.start_time, s.base_price;

-- ============================================================================
-- 14. SEED DATA (BENGALURU THEATERS, SCREENS, MOVIES, SHOWS, USERS & WALLETS)
-- ============================================================================

-- A. USERS SEED
INSERT INTO users (id, name, email, phone) VALUES
('a0000001-0000-0000-0000-000000000001', 'Rahul Dravid', 'rahul.dravid@bengaluru.in', '+919845011111'),
('a0000001-0000-0000-0000-000000000002', 'Deepika Padukone', 'deepika.p@bengaluru.in', '+919845022222'),
('a0000001-0000-0000-0000-000000000003', 'Rishab Shetty', 'rishab.shetty@kantara.in', '+919845033333'),
('a0000001-0000-0000-0000-000000000004', 'Ananya Sharma', 'ananya.sharma@techblr.com', '+919845044444');

-- B. WALLETS SEED (ACID PRE-FUNDED BALANCES)
INSERT INTO wallets (id, user_id, balance, currency, version) VALUES
('b0000001-0000-0000-0000-000000000001', 'a0000001-0000-0000-0000-000000000001', 5000.00, 'INR', 1),
('b0000001-0000-0000-0000-000000000002', 'a0000001-0000-0000-0000-000000000002', 12500.00, 'INR', 1),
('b0000001-0000-0000-0000-000000000003', 'a0000001-0000-0000-0000-000000000003', 7500.00, 'INR', 1),
('b0000001-0000-0000-0000-000000000004', 'a0000001-0000-0000-0000-000000000004', 3200.00, 'INR', 1);

-- INITIAL WALLET TOPUP AUDIT LOGS
INSERT INTO wallet_transactions (
    wallet_id, user_id, transaction_type, amount, balance_before, balance_after, idempotency_key, reference_number, description
) VALUES
('b0000001-0000-0000-0000-000000000001', 'a0000001-0000-0000-0000-000000000001', 'TOPUP', 5000.00, 0.00, 5000.00, 'TOPUP-INIT-001', 'PG-UPI-BLR-001', 'Initial UPI Wallet Topup'),
('b0000001-0000-0000-0000-000000000002', 'a0000001-0000-0000-0000-000000000002', 'TOPUP', 12500.00, 0.00, 12500.00, 'TOPUP-INIT-002', 'PG-CC-BLR-002', 'Initial Credit Card Wallet Topup'),
('b0000001-0000-0000-0000-000000000003', 'a0000001-0000-0000-0000-000000000003', 'TOPUP', 7500.00, 0.00, 7500.00, 'TOPUP-INIT-003', 'PG-UPI-BLR-003', 'Initial UPI Wallet Topup'),
('b0000001-0000-0000-0000-000000000004', 'a0000001-0000-0000-0000-000000000004', 'TOPUP', 3200.00, 0.00, 3200.00, 'TOPUP-INIT-004', 'PG-NET-BLR-004', 'Initial NetBanking Topup');

-- C. BENGALURU THEATERS SEED
INSERT INTO theaters (id, name, brand, location, area, city, pincode, latitude, longitude, total_screens, metadata) VALUES
(
    'c0000001-0000-0000-0000-000000000001',
    'PVR Cinemas, The Forum Mall, Koramangala',
    'PVR',
    'Hosur Main Road, Chikku Lakshmaiah Layout, Koramangala',
    'Koramangala',
    'Bengaluru',
    '560095',
    12.935242,
    77.611520,
    11,
    '{
        "mall": "The Forum Mall Koramangala",
        "parking": "Multi-level underground parking with EV charging",
        "amenities": ["Dolby Atmos", "4DX", "Gold Class", "Food Court Access", "Wheelchair Accessible"],
        "metro_station": "South End Circle / Dairy Circle (Upcoming Pink Line)",
        "fnb": ["Chef Gourmet Burgers", "Nachos & Popcorn Bar", "Costa Coffee"]
    }'::jsonb
),
(
    'c0000001-0000-0000-0000-000000000002',
    'INOX Megaplex, Forum South Bengaluru',
    'INOX',
    'Konanakunte Cross, Kanakapura Road',
    'Konanakunte / South Bengaluru',
    'Bengaluru',
    '560062',
    12.889310,
    77.558320,
    11,
    '{
        "mall": "Forum South Bengaluru Mall",
        "parking": "Covered 4-level mall parking with valet",
        "amenities": ["IMAX with Laser", "MX4D", "ScreenX", "Insignia Luxe Lounge", "Dolby Atmos"],
        "metro_station": "Konanakunte Cross (Green Line)",
        "fnb": ["Insignia Gourmet Dining", "Live Waffle Counter", "Starbucks"]
    }'::jsonb
),
(
    'c0000001-0000-0000-0000-000000000003',
    'Cinepolis, Orion Mall, Rajajinagar',
    'Cinepolis',
    'Dr. Rajkumar Road, Brigade Gateway Complex, Rajajinagar',
    'Rajajinagar',
    'Bengaluru',
    '560055',
    13.011210,
    77.555040,
    11,
    '{
        "mall": "Orion Mall at Brigade Gateway",
        "parking": "Brigade Gateway Multi-Level Car Parking",
        "amenities": ["VIP Club Class", "RealD 3D", "Dolby Atmos 7.1", "Coffee Tree Gourmet"],
        "metro_station": "Sandal Soap Factory (Green Line)",
        "fnb": ["Coffee Tree Cafe", "Mexican Nachos", "Caramel Popcorn"]
    }'::jsonb
),
(
    'c0000001-0000-0000-0000-000000000004',
    'PVR Superplex, Vega City Mall, Bannerghatta Road',
    'PVR',
    'Dollars Colony, Stage 2, Bannerghatta Main Road, BTM Layout',
    'Bannerghatta Road',
    'Bengaluru',
    '560076',
    12.908120,
    77.599740,
    12,
    '{
        "mall": "Vega City Mall",
        "parking": "Basement Parking B1, B2, B3 with reserved handicap bays",
        "amenities": ["IMAX 4K Laser", "Playhouse for Kids", "4DX", "P[XL] Large Screen", "Gold Class"],
        "metro_station": "Jayadeva Hospital Interchange (Yellow / Pink Line)",
        "fnb": ["PVR Cafe Royale", "Artisan Gelato", "Freshly Brewed Beverage Bar"]
    }'::jsonb
);

-- D. SCREENS SEED (FEATURED SCREENS ACROSS BENGALURU THEATERS)
INSERT INTO screens (id, theater_id, screen_number, name, screen_type, sound_system, total_capacity) VALUES
-- PVR Koramangala Screens
('d0000001-0000-0000-0000-000000000001', 'c0000001-0000-0000-0000-000000000001', 1, 'Screen 1 - Gold Class Dolby Atmos', 'GOLD_CLASS', 'Dolby Atmos 64-Channel', 48),
('d0000001-0000-0000-0000-000000000002', 'c0000001-0000-0000-0000-000000000001', 2, 'Screen 2 - 4DX Dynamic Experience', '4DX', 'JBL Surround 7.1', 84),
('d0000001-0000-0000-0000-000000000003', 'c0000001-0000-0000-0000-000000000001', 3, 'Screen 3 - Prime Standard', 'STANDARD', 'Dolby 7.1', 150),

-- INOX Forum South Bengaluru Screens
('d0000001-0000-0000-0000-000000000004', 'c0000001-0000-0000-0000-000000000002', 1, 'Megaplex 1 - IMAX with Laser 4K', 'IMAX', 'IMAX 12-Channel High Precision', 320),
('d0000001-0000-0000-0000-000000000005', 'c0000001-0000-0000-0000-000000000002', 2, 'Megaplex 2 - Insignia Luxe', 'LUXE', 'Dolby Atmos', 60),

-- Cinepolis Orion Mall Screens
('d0000001-0000-0000-0000-000000000006', 'c0000001-0000-0000-0000-000000000003', 1, 'Screen 1 - VIP Club Class', 'GOLD_CLASS', 'Dolby Atmos', 54),
('d0000001-0000-0000-0000-000000000007', 'c0000001-0000-0000-0000-000000000003', 2, 'Screen 2 - Cinepolis Macro XE', 'STANDARD', 'Dolby 7.1', 210),

-- PVR Vega City Screens
('d0000001-0000-0000-0000-000000000008', 'c0000001-0000-0000-0000-000000000004', 1, 'Screen 1 - IMAX Commercial Laser', 'IMAX', 'IMAX 12-Channel Sound', 290),
('d0000001-0000-0000-0000-000000000009', 'c0000001-0000-0000-0000-000000000004', 2, 'Screen 2 - P[XL] Giant Screen', 'P_XL', 'Dolby Atmos 128-Object', 240);

-- E. MOVIES SEED
INSERT INTO movies (id, title, original_language, languages_available, genres, duration_minutes, certification, release_date, synopsis, status) VALUES
(
    'e0000001-0000-0000-0000-000000000001',
    'Kantara: Chapter 1',
    'Kannada',
    ARRAY['Kannada', 'Hindi', 'Telugu', 'Tamil', 'Malayalam']::TEXT[],
    ARRAY['Action', 'Mythology', 'Drama', 'Thriller']::TEXT[],
    168,
    'UA 16+',
    '2026-10-02',
    'The primordial origin myth and sacred guardian lore of the Kadamba dynasty era in coastal Karnataka.',
    'NOW_SHOWING'
),
(
    'e0000001-0000-0000-0000-000000000002',
    'Kalki 2898 AD - Part 2',
    'Telugu',
    ARRAY['Telugu', 'Kannada', 'Hindi', 'Tamil']::TEXT[],
    ARRAY['Sci-Fi', 'Action', 'Epic']::TEXT[],
    180,
    'UA',
    '2026-08-15',
    'The climactic conflict in Kasi and the desert expanse of Shambhala as the prophesied avatar rises.',
    'NOW_SHOWING'
),
(
    'e0000001-0000-0000-0000-000000000003',
    'Interstellar: 12th Anniversary IMAX Re-Release',
    'English',
    ARRAY['English']::TEXT[],
    ARRAY['Sci-Fi', 'Adventure', 'Drama']::TEXT[],
    169,
    'UA',
    '2026-09-20',
    'Exclusive Bengaluru IMAX 70mm & Laser re-release exploring Gargantua and temporal relativity.',
    'NOW_SHOWING'
);

-- F. SHOWS SEED
INSERT INTO shows (id, screen_id, movie_id, audio_language, subtitle_language, experience_format, start_time, end_time, base_price, status) VALUES
-- Show 1: Kantara Chapter 1 at INOX Forum South (IMAX Laser)
(
    'f0000001-0000-0000-0000-000000000001',
    'd0000001-0000-0000-0000-000000000004',
    'e0000001-0000-0000-0000-000000000001',
    'Kannada',
    'English',
    'IMAX 2D',
    CURRENT_TIMESTAMP + INTERVAL '2 hours',
    CURRENT_TIMESTAMP + INTERVAL '5 hours',
    450.00,
    'BOOKINGS_OPEN'
),
-- Show 2: Interstellar IMAX at PVR Vega City
(
    'f0000001-0000-0000-0000-000000000002',
    'd0000001-0000-0000-0000-000000000008',
    'e0000001-0000-0000-0000-000000000003',
    'English',
    'English',
    'IMAX 2D',
    CURRENT_TIMESTAMP + INTERVAL '4 hours',
    CURRENT_TIMESTAMP + INTERVAL '7 hours',
    550.00,
    'BOOKINGS_OPEN'
),
-- Show 3: Kantara Chapter 1 at PVR Koramangala (Gold Class)
(
    'f0000001-0000-0000-0000-000000000003',
    'd0000001-0000-0000-0000-000000000001',
    'e0000001-0000-0000-0000-000000000001',
    'Kannada',
    'English',
    '2D',
    CURRENT_TIMESTAMP + INTERVAL '6 hours',
    CURRENT_TIMESTAMP + INTERVAL '9 hours',
    750.00,
    'BOOKINGS_OPEN'
);

-- G. SAMPLE SEAT INVENTORY SEED (Screen 1 Gold Class PVR Koramangala)
INSERT INTO seats (id, screen_id, seat_number, row_label, column_number, tier, pricing_multiplier) VALUES
('10000001-0000-0000-0000-000000000001', 'd0000001-0000-0000-0000-000000000001', 'A1', 'A', 1, 'RECLINER', 1.00),
('10000001-0000-0000-0000-000000000002', 'd0000001-0000-0000-0000-000000000002', 'A2', 'A', 2, 'RECLINER', 1.00),
('10000001-0000-0000-0000-000000000003', 'd0000001-0000-0000-0000-000000000003', 'A3', 'A', 3, 'RECLINER', 1.00),
('10000001-0000-0000-0000-000000000004', 'd0000001-0000-0000-0000-000000000004', 'A4', 'A', 4, 'RECLINER', 1.00),
('10000001-0000-0000-0000-000000000005', 'd0000001-0000-0000-0000-000000000005', 'B1', 'B', 1, 'RECLINER', 1.00),
('10000001-0000-0000-0000-000000000006', 'd0000001-0000-0000-0000-000000000006', 'B2', 'B', 2, 'RECLINER', 1.00),
('10000001-0000-0000-0000-000000000007', 'd0000001-0000-0000-0000-000000000007', 'B3', 'B', 3, 'RECLINER', 1.00),
('10000001-0000-0000-0000-000000000008', 'd0000001-0000-0000-0000-000000000008', 'B4', 'B', 4, 'RECLINER', 1.00);

-- H. SHOW SEATS ALLOCATION (SHOW 3 - PVR Koramangala)
-- Demonstrating AVAILABLE, HELD (active hold lock), and BOOKED states
INSERT INTO show_seats (id, show_id, seat_id, seat_number, price, status, held_at, hold_expires_at, user_id, version) VALUES
-- Seat A1: Confirmed Booked
(
    '20000001-0000-0000-0000-000000000001',
    'f0000001-0000-0000-0000-000000000003',
    '10000001-0000-0000-0000-000000000001',
    'A1',
    750.00,
    'BOOKED',
    CURRENT_TIMESTAMP - INTERVAL '25 minutes',
    NULL,
    'a0000001-0000-0000-0000-000000000001',
    2
),
-- Seat A2: Confirmed Booked
(
    '20000001-0000-0000-0000-000000000002',
    'f0000001-0000-0000-0000-000000000003',
    '10000001-0000-0000-0000-000000000002',
    'A2',
    750.00,
    'BOOKED',
    CURRENT_TIMESTAMP - INTERVAL '25 minutes',
    NULL,
    'a0000001-0000-0000-0000-000000000001',
    2
),
-- Seat A3: Active Transient Hold (Rahul Dravid checking out right now)
(
    '20000001-0000-0000-0000-000000000003',
    'f0000001-0000-0000-0000-000000000003',
    '10000001-0000-0000-0000-000000000003',
    'A3',
    750.00,
    'HELD',
    CURRENT_TIMESTAMP - INTERVAL '2 minutes',
    CURRENT_TIMESTAMP + INTERVAL '8 minutes',
    'a0000001-0000-0000-0000-000000000002',
    1
),
-- Seats A4, B1, B2, B3, B4: Available for immediate high-concurrency booking
('20000001-0000-0000-0000-000000000004', 'f0000001-0000-0000-0000-000000000003', '10000001-0000-0000-0000-000000000004', 'A4', 750.00, 'AVAILABLE', NULL, NULL, NULL, 1),
('20000001-0000-0000-0000-000000000005', 'f0000001-0000-0000-0000-000000000003', '10000001-0000-0000-0000-000000000005', 'B1', 750.00, 'AVAILABLE', NULL, NULL, NULL, 1),
('20000001-0000-0000-0000-000000000006', 'f0000001-0000-0000-0000-000000000003', '10000001-0000-0000-0000-000000000006', 'B2', 750.00, 'AVAILABLE', NULL, NULL, NULL, 1),
('20000001-0000-0000-0000-000000000007', 'f0000001-0000-0000-0000-000000000003', '10000001-0000-0000-0000-000000000007', 'B3', 750.00, 'AVAILABLE', NULL, NULL, NULL, 1),
('20000001-0000-0000-0000-000000000008', 'f0000001-0000-0000-0000-000000000003', '10000001-0000-0000-0000-000000000008', 'B4', 750.00, 'AVAILABLE', NULL, NULL, NULL, 1);

-- I. SAMPLE COMPLETED BOOKING RECORD
INSERT INTO bookings (
    id, booking_reference, user_id, show_id, seat_count, ticket_amount, convenience_fee, tax_amount, total_amount,
    status, idempotency_key, payment_method, reserved_at, confirmed_at
) VALUES (
    '30000001-0000-0000-0000-000000000001',
    'BLR-202610-KOR-991A',
    'a0000001-0000-0000-0000-000000000001',
    'f0000001-0000-0000-0000-000000000003',
    2,
    1500.00,
    60.00,
    10.80,
    1570.80,
    'CONFIRMED',
    'IDEMP-BOOK-BLR-001',
    'WALLET',
    CURRENT_TIMESTAMP - INTERVAL '25 minutes',
    CURRENT_TIMESTAMP - INTERVAL '23 minutes'
);

-- Link confirmed show_seats to this booking
UPDATE show_seats 
SET booking_id = '30000001-0000-0000-0000-000000000001'
WHERE id IN ('20000001-0000-0000-0000-000000000001', '20000001-0000-0000-0000-000000000002');

-- Record wallet debit audit log for the booking
INSERT INTO wallet_transactions (
    wallet_id, user_id, transaction_type, amount, balance_before, balance_after,
    booking_id, idempotency_key, reference_number, description
) VALUES (
    'b0000001-0000-0000-0000-000000000001',
    'a0000001-0000-0000-0000-000000000001',
    'BOOKING_PAYMENT',
    -1570.80,
    5000.00,
    3429.20,
    '30000001-0000-0000-0000-000000000001',
    'IDEMP-PAY-BLR-001',
    'BLR-202610-KOR-991A',
    'Debit for Kantara: Chapter 1 tickets (A1, A2) at PVR Koramangala'
);

-- Update wallet balance reflection
UPDATE wallets
SET balance = 3429.20,
    version = version + 1
WHERE id = 'b0000001-0000-0000-0000-000000000001';

-- ============================================================================
-- END OF SCHEMA SCRIPT
-- ============================================================================
