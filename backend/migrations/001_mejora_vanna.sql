-- Migración 001: integridad de datos y conteo real de asientos.
--
-- Aplicar UNA sola vez sobre una base existente, en una transacción:
--   psql "$URL" -v ON_ERROR_STOP=1 --single-transaction -f 001_mejora_vanna.sql
-- Si ya se aplicó, aborta sin cambiar nada.
--
-- Cambios:
--   * seats_available pasa a significar "asientos que QUEDAN": se descuentan
--     los asientos de solicitudes ya aceptadas en viajes activos.
--   * Constraints CHECK de cantidades, índice único de solicitudes vigentes.
--   * Triggers de updated_at y de sincronización de lat/lng.
--   * foto_url y fcm_token pasan a TEXT (las URLs de Firebase Storage pueden
--     superar 255 caracteres).

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     VARCHAR(50) PRIMARY KEY,
    applied_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM schema_migrations WHERE version = '001_mejora_vanna') THEN
        RAISE EXCEPTION 'La migración 001_mejora_vanna ya fue aplicada';
    END IF;
END $$;

-- 1. Columnas más amplias
ALTER TABLE users ALTER COLUMN foto_url TYPE TEXT;
ALTER TABLE users ALTER COLUMN fcm_token TYPE TEXT;

-- Columnas de auditoría que usan los triggers de updated_at. El backend viejo
-- nunca las usó, así que una base antigua puede no tenerlas.
ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE trips ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE trips ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE trip_requests ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE trip_requests ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();

-- 2. Limpieza de datos que violarían los nuevos constraints
UPDATE trips SET price = 0 WHERE price < 0;
UPDATE trips SET seats_available = 0 WHERE seats_available < 0;
UPDATE trip_requests
SET status = 'cancelado', seats_requested = 1
WHERE seats_requested IS NULL OR seats_requested <= 0;

-- Solicitudes vigentes duplicadas: se conserva una (aceptada primero, luego la
-- más antigua) y el resto se cancela.
WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY trip_id, passenger_id
               ORDER BY (status = 'aceptado') DESC, id ASC
           ) AS rn
    FROM trip_requests
    WHERE status IN ('pendiente', 'aceptado')
)
UPDATE trip_requests tr
SET status = 'cancelado'
FROM ranked
WHERE tr.id = ranked.id AND ranked.rn > 1;

-- 3. Asientos restantes = asientos publicados - asientos ya aceptados
UPDATE trips t
SET seats_available = GREATEST(t.seats_available - acc.total, 0)
FROM (
    SELECT trip_id, SUM(seats_requested) AS total
    FROM trip_requests
    WHERE status = 'aceptado'
    GROUP BY trip_id
) acc
WHERE acc.trip_id = t.id AND t.status = 'activo';

-- 4. Constraints e índices
ALTER TABLE trips ADD CONSTRAINT chk_trip_price CHECK (price >= 0);
ALTER TABLE trips ADD CONSTRAINT chk_trip_seats CHECK (seats_available >= 0);
ALTER TABLE trip_requests ADD CONSTRAINT chk_request_seats CHECK (seats_requested > 0);

CREATE UNIQUE INDEX IF NOT EXISTS uq_trip_requests_active
    ON trip_requests(trip_id, passenger_id)
    WHERE status IN ('pendiente', 'aceptado');
CREATE INDEX IF NOT EXISTS idx_trips_status_departure ON trips(status, departure_time);
CREATE INDEX IF NOT EXISTS idx_trips_driver_status ON trips(driver_id, status);
CREATE INDEX IF NOT EXISTS idx_trips_route_geom ON trips USING GIST(route_geom);
CREATE INDEX IF NOT EXISTS idx_trip_requests_trip_status ON trip_requests(trip_id, status);
CREATE INDEX IF NOT EXISTS idx_trip_requests_passenger_status ON trip_requests(passenger_id, status);
CREATE INDEX IF NOT EXISTS idx_users_fcm_token ON users(fcm_token);

-- 5. Triggers
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_trip_coords() RETURNS trigger AS $$
BEGIN
    NEW.origin_lat := ST_Y(NEW.origin_geom);
    NEW.origin_lng := ST_X(NEW.origin_geom);
    NEW.dest_lat   := ST_Y(NEW.dest_geom);
    NEW.dest_lng   := ST_X(NEW.dest_geom);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_users_updated_at ON users;
CREATE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_trips_updated_at ON trips;
CREATE TRIGGER trg_trips_updated_at
    BEFORE UPDATE ON trips
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_trip_requests_updated_at ON trip_requests;
CREATE TRIGGER trg_trip_requests_updated_at
    BEFORE UPDATE ON trip_requests
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_trips_sync_coords ON trips;
CREATE TRIGGER trg_trips_sync_coords
    BEFORE INSERT OR UPDATE OF origin_geom, dest_geom ON trips
    FOR EACH ROW EXECUTE FUNCTION sync_trip_coords();

-- Rellena lat/lng de viajes existentes (dispara sync_trip_coords)
UPDATE trips SET origin_geom = origin_geom
WHERE origin_geom IS NOT NULL OR dest_geom IS NOT NULL;

INSERT INTO schema_migrations (version) VALUES ('001_mejora_vanna');
