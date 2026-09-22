"""Repositorio de acceso a datos para viajes.

``departure_time`` se guarda como hora local naive de ``APP_TIMEZONE``; por eso
las consultas reciben ``tz`` y comparan contra ``NOW() AT TIME ZONE tz``.

Un viaje se considera vigente para el conductor mientras
``status = 'activo'`` y no haya pasado ``departure_time + grace_minutes``. Para
la búsqueda de pasajeros deja de ser visible a la hora de salida.
"""

from datetime import datetime

import asyncpg

from app.schemas.trip import TripCreate


async def lock_driver(conn: asyncpg.Connection, driver_id: str) -> None:
    """Serializa la publicación de viajes de un mismo conductor.

    Debe llamarse dentro de una transacción; el lock se libera al terminarla.
    """
    await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1));", driver_id)


async def has_active_trip(
    conn: asyncpg.Connection,
    driver_id: str,
    tz: str,
    grace_minutes: int,
) -> bool:
    """Verifica si un conductor tiene un viaje vigente."""
    query = """
        SELECT 1 FROM trips
        WHERE driver_id = $1
          AND status = 'activo'
          AND departure_time + make_interval(mins => $3) > (NOW() AT TIME ZONE $2);
    """
    return await conn.fetchval(query, driver_id, tz, grace_minutes) is not None


async def create_trip(
    conn: asyncpg.Connection,
    driver_id: str,
    trip: TripCreate,
    departure_time: datetime,
    route_wkt: str,
) -> int:
    """Crea un nuevo viaje y devuelve su ID.

    La distancia se calcula aquí a partir de la ruta (no se confía en la del
    cliente). Las columnas lat/lng se llenan por trigger desde las geometrías.
    """
    query = """
        WITH route AS (SELECT ST_GeomFromText($12, 4326) AS geom)
        INSERT INTO trips (
            driver_id, origin_name, dest_name, duration_text,
            departure_time, price, seats_available, status,
            origin_geom, dest_geom, route_geom, distance_text
        )
        SELECT
            $1, $2, $3, $4, $5, $6, $7, 'activo',
            ST_SetSRID(ST_MakePoint($8, $9), 4326),
            ST_SetSRID(ST_MakePoint($10, $11), 4326),
            route.geom,
            ROUND((ST_Length(route.geom::geography) / 1000)::numeric, 1)::text || ' km'
        FROM route
        RETURNING id;
    """
    return await conn.fetchval(
        query,
        driver_id,
        trip.origin_name,
        trip.dest_name,
        trip.duration_text,
        departure_time,
        trip.price,
        trip.seats_available,
        trip.origin_lng,
        trip.origin_lat,
        trip.dest_lng,
        trip.dest_lat,
        route_wkt,
    )


async def get_active_trip_with_passengers(
    conn: asyncpg.Connection,
    driver_id: str,
    tz: str,
    grace_minutes: int,
) -> asyncpg.Record | None:
    """Obtiene el viaje vigente de un conductor junto con sus pasajeros aceptados.

    Returns:
        Una fila con el viaje y un campo ``passengers`` en formato JSONB.
    """
    query = """
        SELECT
            t.*,
            COALESCE(
                JSONB_AGG(
                    JSONB_BUILD_OBJECT(
                        'id', u.id,
                        'name', u.name,
                        'foto_url', u.foto_url
                    )
                ) FILTER (WHERE u.id IS NOT NULL),
                '[]'::jsonb
            ) AS passengers
        FROM trips t
        LEFT JOIN trip_requests tr
            ON tr.trip_id = t.id AND tr.status = 'aceptado'
        LEFT JOIN users u ON u.id = tr.passenger_id
        WHERE t.driver_id = $1
          AND t.status = 'activo'
          AND t.departure_time + make_interval(mins => $3) > (NOW() AT TIME ZONE $2)
        GROUP BY t.id
        ORDER BY t.id DESC
        LIMIT 1;
    """
    return await conn.fetchrow(query, driver_id, tz, grace_minutes)


async def get_trip_by_id(
    conn: asyncpg.Connection,
    trip_id: int,
    for_update: bool = False,
) -> asyncpg.Record | None:
    """Obtiene un viaje por su ID.

    Con ``for_update=True`` bloquea la fila hasta el fin de la transacción.
    """
    query = """
        SELECT id, driver_id, status, departure_time, seats_available
        FROM trips
        WHERE id = $1
    """
    if for_update:
        query += " FOR UPDATE"
    return await conn.fetchrow(query, trip_id)


async def cancel_trip(conn: asyncpg.Connection, trip_id: int) -> bool:
    """Marca un viaje activo como cancelado (soft delete).

    Returns:
        True si el viaje estaba activo y se canceló.
    """
    result = await conn.execute(
        "UPDATE trips SET status = 'cancelado' WHERE id = $1 AND status = 'activo';",
        trip_id,
    )
    return result != "UPDATE 0"


async def reserve_seats(conn: asyncpg.Connection, trip_id: int, seats: int) -> bool:
    """Descuenta asientos de forma atómica.

    Returns:
        True si había asientos suficientes y se descontaron.
    """
    query = """
        UPDATE trips
        SET seats_available = seats_available - $2
        WHERE id = $1 AND status = 'activo' AND seats_available >= $2;
    """
    result = await conn.execute(query, trip_id, seats)
    return result != "UPDATE 0"


async def release_seats(conn: asyncpg.Connection, trip_id: int, seats: int) -> None:
    """Devuelve asientos a un viaje (p. ej. cuando un pasajero cancela)."""
    await conn.execute(
        "UPDATE trips SET seats_available = seats_available + $2 WHERE id = $1;",
        trip_id,
        seats,
    )


async def expire_trips(conn: asyncpg.Connection, tz: str, grace_minutes: int) -> int:
    """Marca como completados los viajes activos cuya ventana ya terminó.

    Returns:
        Número de viajes completados.
    """
    query = """
        UPDATE trips
        SET status = 'completado'
        WHERE status = 'activo'
          AND departure_time + make_interval(mins => $2) <= (NOW() AT TIME ZONE $1);
    """
    result = await conn.execute(query, tz, grace_minutes)
    return int(result.split()[-1])


async def search_trips(
    conn: asyncpg.Connection,
    olat: float,
    olng: float,
    dlat: float,
    dlng: float,
    passenger_id: str,
    tz: str,
    radius_m: int,
) -> list[asyncpg.Record]:
    """Busca viajes que pasen cerca del origen y luego del destino del pasajero.

    Se crean buffers de ``radius_m`` alrededor del origen y destino del
    pasajero y se buscan rutas que intersecten ambos. Además, sobre la ruta el
    punto más cercano al origen debe ir antes que el más cercano al destino,
    para descartar viajes en sentido contrario. Se excluyen viajes que ya
    salieron, sin asientos o publicados por el propio pasajero.
    """
    query = """
        WITH pasajero AS (
            SELECT
                ST_SetSRID(ST_MakePoint($1, $2), 4326) AS punto_origen,
                ST_SetSRID(ST_MakePoint($3, $4), 4326) AS punto_destino
        ),
        pasajero_circulos AS (
            SELECT
                punto_origen,
                punto_destino,
                ST_Buffer(punto_origen::geography, $7)::geometry AS circulo_origen,
                ST_Buffer(punto_destino::geography, $7)::geometry AS circulo_destino
            FROM pasajero
        )
        SELECT
            t.id, t.origin_name, t.dest_name, t.departure_time, t.price,
            t.seats_available, t.distance_text, t.duration_text,
            u.id AS driver_id, u.name AS driver_name, u.biography,
            u.vehicles, u.preferences, u.foto_url AS driver_foto_url
        FROM trips t
        JOIN users u ON t.driver_id = u.id
        CROSS JOIN pasajero_circulos c
        WHERE t.status = 'activo'
          AND t.departure_time > (NOW() AT TIME ZONE $6)
          AND t.seats_available > 0
          AND t.driver_id <> $5
          AND ST_Intersects(t.route_geom, c.circulo_origen)
          AND ST_Intersects(t.route_geom, c.circulo_destino)
          AND ST_LineLocatePoint(t.route_geom, c.punto_origen)
              < ST_LineLocatePoint(t.route_geom, c.punto_destino)
        ORDER BY t.departure_time ASC;
    """
    return await conn.fetch(
        query, olng, olat, dlng, dlat, passenger_id, tz, float(radius_m)
    )


async def get_approved_trips_for_passenger(
    conn: asyncpg.Connection,
    passenger_id: str,
) -> list[asyncpg.Record]:
    """Obtiene los viajes aprobados para un pasajero."""
    query = """
        SELECT
            t.*,
            tr.status AS request_status,
            u.name AS driver_name,
            u.biography AS driver_biography,
            u.vehicles AS driver_vehicles,
            u.preferences AS driver_preferences,
            u.foto_url AS driver_foto_url
        FROM trips t
        INNER JOIN trip_requests tr ON t.id = tr.trip_id
        INNER JOIN users u ON t.driver_id = u.id
        WHERE tr.passenger_id = $1
          AND tr.status = 'aceptado'
          AND t.status != 'cancelado'
        ORDER BY t.departure_time ASC;
    """
    return await conn.fetch(query, passenger_id)
