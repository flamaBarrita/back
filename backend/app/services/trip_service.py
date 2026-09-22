"""Servicio de lógica de negocio para viajes."""

import asyncio

import asyncpg
from fastapi import BackgroundTasks

from app.core.config import get_settings
from app.core.database import get_db_context
from app.core.exceptions import (
    ConflictError,
    ForbiddenError,
    NotFoundError,
    ValidationError,
)
from app.core.logging import get_logger
from app.repositories import request_repo, trip_repo
from app.schemas.trip import TripCreate
from app.services.notification_service import notify_users
from app.utils.polyline import decode_polyline_to_wkt
from app.utils.time import now_local, to_local_naive

logger = get_logger(__name__)


async def get_owned_trip(
    conn: asyncpg.Connection,
    trip_id: int,
    driver_id: str,
    for_update: bool = False,
) -> asyncpg.Record:
    """Obtiene un viaje verificando que pertenezca al conductor indicado.

    Raises:
        NotFoundError: Si el viaje no existe.
        ForbiddenError: Si el viaje es de otro conductor.
    """
    trip = await trip_repo.get_trip_by_id(conn, trip_id, for_update=for_update)
    if not trip:
        raise NotFoundError("El viaje no existe")
    if trip["driver_id"] != driver_id:
        raise ForbiddenError("Este viaje no te pertenece")
    return trip


async def create_trip(
    conn: asyncpg.Connection,
    driver_id: str,
    trip: TripCreate,
) -> dict:
    """Crea un nuevo viaje publicado por un conductor."""
    settings = get_settings()
    departure = to_local_naive(trip.departure_time, settings.app_timezone)
    if departure <= now_local(settings.app_timezone):
        raise ValidationError("La hora de salida debe ser en el futuro")

    try:
        route_wkt = decode_polyline_to_wkt(trip.encoded_polyline)
    except ValueError as exc:
        raise ValidationError(str(exc)) from exc

    async with conn.transaction():
        # Evita que dos peticiones simultáneas publiquen dos viajes.
        await trip_repo.lock_driver(conn, driver_id)
        if await trip_repo.has_active_trip(
            conn, driver_id, settings.app_timezone, settings.trip_active_grace_minutes
        ):
            raise ConflictError(
                "Ya tienes un viaje activo. Debes finalizarlo antes de publicar uno nuevo."
            )
        trip_id = await trip_repo.create_trip(conn, driver_id, trip, departure, route_wkt)

    return {"message": "Viaje publicado correctamente", "trip_id": trip_id}


async def get_active_trip(
    conn: asyncpg.Connection,
    driver_id: str,
) -> dict | None:
    """Obtiene el viaje vigente de un conductor con sus pasajeros aceptados."""
    settings = get_settings()
    row = await trip_repo.get_active_trip_with_passengers(
        conn, driver_id, settings.app_timezone, settings.trip_active_grace_minutes
    )
    if not row:
        return None
    return dict(row)


async def get_trip_requests(
    conn: asyncpg.Connection,
    trip_id: int,
    driver_id: str,
) -> list[dict]:
    """Obtiene las solicitudes pendientes de un viaje del conductor."""
    await get_owned_trip(conn, trip_id, driver_id)
    rows = await request_repo.get_pending_requests(conn, trip_id)
    return [dict(r) for r in rows]


async def search_trips(
    conn: asyncpg.Connection,
    olat: float,
    olng: float,
    dlat: float,
    dlng: float,
    passenger_id: str,
) -> list[dict]:
    """Busca viajes disponibles cuya ruta pase por el origen y luego el destino."""
    settings = get_settings()
    rows = await trip_repo.search_trips(
        conn,
        olat,
        olng,
        dlat,
        dlng,
        passenger_id,
        settings.app_timezone,
        settings.search_radius_m,
    )
    return [dict(r) for r in rows]


async def cancel_trip(
    conn: asyncpg.Connection,
    trip_id: int,
    driver_id: str,
    background_tasks: BackgroundTasks,
) -> dict:
    """Cancela un viaje del conductor y notifica a los pasajeros aceptados."""
    async with conn.transaction():
        trip = await get_owned_trip(conn, trip_id, driver_id, for_update=True)
        if trip["status"] != "activo":
            raise ConflictError("El viaje ya no está activo")

        passenger_ids = await request_repo.get_accepted_passenger_ids(conn, trip_id)
        await trip_repo.cancel_trip(conn, trip_id)
        await request_repo.cancel_requests_by_trip(conn, trip_id)

    await notify_users(
        conn,
        background_tasks,
        passenger_ids,
        "Viaje Cancelado",
        "El conductor ha cancelado el viaje. Busca un nuevo transporte.",
    )

    return {"message": "Viaje cancelado correctamente", "exito": True}


async def get_approved_trips_for_passenger(
    conn: asyncpg.Connection,
    passenger_id: str,
) -> list[dict]:
    """Obtiene los viajes aprobados para un pasajero."""
    rows = await trip_repo.get_approved_trips_for_passenger(conn, passenger_id)
    return [dict(r) for r in rows]


async def expire_trips() -> int:
    """Marca como completados los viajes cuya ventana activa ya terminó."""
    settings = get_settings()
    async with get_db_context() as conn:
        count = await trip_repo.expire_trips(
            conn, settings.app_timezone, settings.trip_active_grace_minutes
        )
    if count:
        logger.info("Se marcaron %d viajes como completados", count)
    return count


async def run_trip_expiration_loop() -> None:
    """Job periódico de expiración de viajes; corre mientras viva la app.

    Las consultas ya filtran por hora de salida, así que este job solo
    mantiene ``status`` al día. Es idempotente: si corren varios workers no
    pasa nada.
    """
    interval = get_settings().trip_expiration_interval_seconds
    while True:
        try:
            await expire_trips()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Falló el job de expiración de viajes")
        await asyncio.sleep(interval)
