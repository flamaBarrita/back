"""Repositorio de acceso a datos para solicitudes de viaje.

Las funciones que modifican asientos deben llamarse dentro de una transacción
que ya haya bloqueado el viaje con ``trip_repo.get_trip_by_id(for_update=True)``.
Bloquear siempre primero el viaje y después la solicitud evita deadlocks.
"""

import asyncpg


async def get_pending_requests(
    conn: asyncpg.Connection,
    trip_id: int,
) -> list[asyncpg.Record]:
    """Obtiene las solicitudes pendientes de un viaje con datos del pasajero."""
    query = """
        SELECT
            tr.id,
            tr.trip_id,
            tr.passenger_id,
            u.name AS passenger_name,
            u.foto_url AS passenger_photo,
            tr.seats_requested,
            tr.status
        FROM trip_requests tr
        JOIN users u ON tr.passenger_id = u.id
        WHERE tr.trip_id = $1 AND tr.status = 'pendiente'
        ORDER BY tr.id ASC;
    """
    return await conn.fetch(query, trip_id)


async def create_request(
    conn: asyncpg.Connection,
    trip_id: int,
    passenger_id: str,
    seats_requested: int,
) -> int:
    """Crea una nueva solicitud de viaje y devuelve su ID."""
    query = """
        INSERT INTO trip_requests (trip_id, passenger_id, seats_requested, status)
        VALUES ($1, $2, $3, 'pendiente')
        RETURNING id;
    """
    return await conn.fetchval(query, trip_id, passenger_id, seats_requested)


async def get_request_trip_id(conn: asyncpg.Connection, request_id: int) -> int | None:
    """Obtiene el ID del viaje al que pertenece una solicitud (sin bloquear)."""
    return await conn.fetchval(
        "SELECT trip_id FROM trip_requests WHERE id = $1;", request_id
    )


async def get_request_for_update(
    conn: asyncpg.Connection,
    request_id: int,
) -> asyncpg.Record | None:
    """Obtiene y bloquea una solicitud hasta el fin de la transacción."""
    query = """
        SELECT id, trip_id, passenger_id, seats_requested, status
        FROM trip_requests
        WHERE id = $1
        FOR UPDATE;
    """
    return await conn.fetchrow(query, request_id)


async def set_request_status(
    conn: asyncpg.Connection,
    request_id: int,
    status: str,
) -> None:
    """Cambia el estado de una solicitud."""
    await conn.execute(
        "UPDATE trip_requests SET status = $1 WHERE id = $2;",
        status,
        request_id,
    )


async def cancel_requests_by_trip(
    conn: asyncpg.Connection,
    trip_id: int,
) -> None:
    """Cancela las solicitudes vigentes (pendientes o aceptadas) de un viaje."""
    query = """
        UPDATE trip_requests
        SET status = 'cancelado'
        WHERE trip_id = $1 AND status IN ('pendiente', 'aceptado');
    """
    await conn.execute(query, trip_id)


async def has_pending_or_accepted_request(
    conn: asyncpg.Connection,
    trip_id: int,
    passenger_id: str,
) -> bool:
    """Verifica si un pasajero ya tiene una solicitud pendiente o aceptada."""
    query = """
        SELECT 1 FROM trip_requests
        WHERE trip_id = $1 AND passenger_id = $2
          AND status IN ('pendiente', 'aceptado');
    """
    return await conn.fetchval(query, trip_id, passenger_id) is not None


async def cancel_passenger_seat(
    conn: asyncpg.Connection,
    trip_id: int,
    passenger_id: str,
) -> int | None:
    """Marca la reserva aceptada de un pasajero como cancelada por él.

    Returns:
        Los asientos que se liberan, o None si no había reserva aceptada.
    """
    query = """
        UPDATE trip_requests
        SET status = 'cancelled_by_passen'
        WHERE trip_id = $1 AND passenger_id = $2 AND status = 'aceptado'
        RETURNING seats_requested;
    """
    rows = await conn.fetch(query, trip_id, passenger_id)
    if not rows:
        return None
    return sum(row["seats_requested"] for row in rows)


async def get_accepted_passenger_ids(
    conn: asyncpg.Connection,
    trip_id: int,
) -> list[str]:
    """Obtiene los IDs de los pasajeros aceptados en un viaje."""
    query = """
        SELECT DISTINCT passenger_id
        FROM trip_requests
        WHERE trip_id = $1 AND status = 'aceptado';
    """
    rows = await conn.fetch(query, trip_id)
    return [row["passenger_id"] for row in rows]
