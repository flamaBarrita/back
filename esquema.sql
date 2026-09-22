-- Esquema de base de datos para back_blabla
-- Crea desde cero las tablas del proyecto en su versión más reciente.
-- Para una base de datos existente, aplicar en su lugar las migraciones de
-- backend/migrations/ (ver backend/migrations/README.md).
-- Nota: PostGIS debe estar disponible en la base de datos.

CREATE EXTENSION IF NOT EXISTS postgis;

-- Control de migraciones aplicadas
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     VARCHAR(50) PRIMARY KEY,
    applied_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Mantiene updated_at al día en cualquier UPDATE
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Mantiene origin_lat/lng y dest_lat/lng sincronizados con las geometrías
CREATE OR REPLACE FUNCTION sync_trip_coords() RETURNS trigger AS $$
BEGIN
    NEW.origin_lat := ST_Y(NEW.origin_geom);
    NEW.origin_lng := ST_X(NEW.origin_geom);
    NEW.dest_lat   := ST_Y(NEW.dest_geom);
    NEW.dest_lng   := ST_X(NEW.dest_geom);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Tabla de usuarios (conductores y pasajeros)
CREATE TABLE IF NOT EXISTS users (
    id          VARCHAR(255) PRIMARY KEY,
    name        VARCHAR(100),
    status      VARCHAR(50),
    biography   TEXT,
    preferences TEXT,
    vehicles    TEXT,
    fcm_token   TEXT,
    foto_url    TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_fcm_token ON users(fcm_token);

DROP TRIGGER IF EXISTS trg_users_updated_at ON users;
CREATE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Tabla de viajes
-- departure_time guarda la hora local de APP_TIMEZONE (America/Mexico_City).
-- seats_available son los asientos que QUEDAN libres: se descuentan al aceptar
-- una solicitud y se devuelven cuando un pasajero cancela.
CREATE TABLE IF NOT EXISTS trips (
    id              SERIAL PRIMARY KEY,
    driver_id       VARCHAR(255) NOT NULL REFERENCES users(id),
    origin_name     TEXT,
    origin_lat      DOUBLE PRECISION,
    origin_lng      DOUBLE PRECISION,
    dest_name       TEXT,
    dest_lat        DOUBLE PRECISION,
    dest_lng        DOUBLE PRECISION,
    distance_text   VARCHAR(50),
    duration_text   VARCHAR(50),
    origin_geom     GEOMETRY(Point, 4326),
    dest_geom       GEOMETRY(Point, 4326),
    departure_time  TIMESTAMP WITHOUT TIME ZONE,
    price           NUMERIC,
    seats_available INTEGER,
    status          VARCHAR(20) NOT NULL DEFAULT 'activo',
    route_geom      GEOMETRY(LineString, 4326),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_trip_status CHECK (status IN ('activo', 'cancelado', 'completado')),
    CONSTRAINT chk_trip_price CHECK (price >= 0),
    CONSTRAINT chk_trip_seats CHECK (seats_available >= 0)
);

CREATE INDEX IF NOT EXISTS idx_trips_driver_status ON trips(driver_id, status);
CREATE INDEX IF NOT EXISTS idx_trips_status_departure ON trips(status, departure_time);
CREATE INDEX IF NOT EXISTS idx_trips_origin_gix ON trips USING GIST(origin_geom);
CREATE INDEX IF NOT EXISTS idx_trips_dest_gix ON trips USING GIST(dest_geom);
CREATE INDEX IF NOT EXISTS idx_trips_route_geom ON trips USING GIST(route_geom);

DROP TRIGGER IF EXISTS trg_trips_updated_at ON trips;
CREATE TRIGGER trg_trips_updated_at
    BEFORE UPDATE ON trips
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_trips_sync_coords ON trips;
CREATE TRIGGER trg_trips_sync_coords
    BEFORE INSERT OR UPDATE OF origin_geom, dest_geom ON trips
    FOR EACH ROW EXECUTE FUNCTION sync_trip_coords();

-- Tabla de solicitudes de viaje
CREATE TABLE IF NOT EXISTS trip_requests (
    id               SERIAL PRIMARY KEY,
    trip_id          INTEGER NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    passenger_id     VARCHAR(255) NOT NULL REFERENCES users(id),
    seats_requested  INTEGER NOT NULL,
    status           VARCHAR(20) NOT NULL DEFAULT 'pendiente',
    created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_request_status CHECK (
        status IN ('pendiente', 'aceptado', 'rechazado', 'cancelado', 'cancelled_by_passen')
    ),
    CONSTRAINT chk_request_seats CHECK (seats_requested > 0)
);

CREATE INDEX IF NOT EXISTS idx_trip_requests_trip_status ON trip_requests(trip_id, status);
CREATE INDEX IF NOT EXISTS idx_trip_requests_passenger_status ON trip_requests(passenger_id, status);

-- Un pasajero solo puede tener una solicitud vigente por viaje
CREATE UNIQUE INDEX IF NOT EXISTS uq_trip_requests_active
    ON trip_requests(trip_id, passenger_id)
    WHERE status IN ('pendiente', 'aceptado');

DROP TRIGGER IF EXISTS trg_trip_requests_updated_at ON trip_requests;
CREATE TRIGGER trg_trip_requests_updated_at
    BEFORE UPDATE ON trip_requests
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Un esquema nuevo ya incluye todas las migraciones
INSERT INTO schema_migrations (version) VALUES ('001_mejora_vanna')
ON CONFLICT (version) DO NOTHING;
