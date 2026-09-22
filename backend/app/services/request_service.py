"""Servicio de lógica de negocio para solicitudes de viaje.

Todas las operaciones que tocan asientos bloquean primero el viaje
(``SELECT ... FOR UPDATE``) dentro de una transacción. Así dos solicitudes
simultáneas no pueden sobrevender asientos, incluso con varios workers.
"""

import asyncpg
from fastapi import BackgroundTasks

from app.core.config import get_settings
from app.core.exceptions import (
    ConflictError,
    ForbiddenError,
    NotFoundError,
    ValidationError,
)
from app.repositories import profile_repo, request_repo, trip_repo
from app.schemas.request import TripRequestCreate
from app.services.notification_service import notify_users
from app.utils.time import now_local

_STATUS_MESSAGES = {
    "aceptado": (
        "Lugar Asegurado",
        "El conductor ha aceptado tu solicitud. Prepárate para el viaje.",
    ),
    "rechazado": (
        "Viaje Lleno",
        "El conductor no pudo aceptar tu solicitud esta vez.",
    ),
}


async def create_trip_request(
    conn: asyncpg.Connection,
    trip_id: int,
    passenger_id: str,
    req: TripRequestCreate,
    background_tasks: BackgroundTasks,
) -> dict:
    """Crea una solicitud para que el usuario autenticado se una a un viaje."""
    tz = get_settings().app_timezone

    async with conn.transaction():
        trip = await trip_repo.get_trip_by_id(conn, trip_id, for_update=True)

        if not trip:
            raise NotFoundError("El viaje no existe")

        departure = trip["departure_time"]
        if trip["status"] != "activo" or departure is None or departure <= now_local(tz):
            raise ValidationError("Este viaje ya no está activo")

        if trip["driver_id"] == passenger_id:
            raise ValidationError("No puedes solicitar tu propio viaje")

        if trip["seats_available"] < req.seats_requested:
            raise ValidationError("No hay suficientes asientos disponibles")

        if await request_repo.has_pending_or_accepted_request(
            conn, trip_id, passenger_id
        ):
            raise ConflictError(
                "Ya tienes un lugar en este viaje o una solicitud pendiente"
            )

        request_id = await request_repo.create_request(
            conn, trip_id, passenger_id, req.seats_requested
        )

    passenger_name = await profile_repo.get_user_name(conn, passenger_id) or "Un pasajero"
    await notify_users(
        conn,
        background_tasks,
        [trip["driver_id"]],
        "Nueva Solicitud",
        f"{passenger_name} solicita unirse a tu viaje.",
    )

    return {"message": "Solicitud enviada al conductor", "request_id": request_id}


async def update_request_status(
    conn: asyncpg.Connection,
    request_id: int,
    status: str,
    driver_id: str,
    background_tasks: BackgroundTasks,
) -> dict:
    """El conductor acepta o rechaza una solicitud pendiente de su viaje.

    Al aceptar se descuentan los asientos de forma atómica; si ya no alcanzan,
    la solicitud queda pendiente y se responde 409.
    """
    trip_id = await request_repo.get_request_trip_id(conn, request_id)
    if trip_id is None:
        raise NotFoundError("Solicitud no encontrada")

    async with conn.transaction():
        # Orden de bloqueo: viaje y después solicitud.
        trip = await trip_repo.get_trip_by_id(conn, trip_id, for_update=True)
        request = await request_repo.get_request_for_update(conn, request_id)
        if not trip or not request:
            raise NotFoundError("Solicitud no encontrada")

        if trip["driver_id"] != driver_id:
            raise ForbiddenError("Solo el conductor del viaje puede responder solicitudes")

        if request["status"] != "pendiente":
            raise ConflictError("Esta solicitud ya fue respondida o cancelada")

        if status == "aceptado":
            if trip["status"] != "activo":
                raise ConflictError("El viaje ya no está activo")
            if not await trip_repo.reserve_seats(
                conn, trip_id, request["seats_requested"]
            ):
                raise ConflictError("No quedan asientos suficientes en el viaje")

        await request_repo.set_request_status(conn, request_id, status)

    title, body = _STATUS_MESSAGES[status]
    await notify_users(
        conn, background_tasks, [request["passenger_id"]], title, body
    )

    return {"message": "Estado actualizado correctamente"}


async def cancel_passenger_seat(
    conn: asyncpg.Connection,
    trip_id: int,
    passenger_id: str,
    background_tasks: BackgroundTasks,
) -> dict:
    """El pasajero cancela su reserva aceptada; se liberan sus asientos."""
    async with conn.transaction():
        trip = await trip_repo.get_trip_by_id(conn, trip_id, for_update=True)
        if not trip:
            raise NotFoundError("El viaje no existe")

        freed_seats = await request_repo.cancel_passenger_seat(
            conn, trip_id, passenger_id
        )
        if freed_seats is None:
            raise ValidationError("No se encontró una reserva activa para cancelar")

        await trip_repo.release_seats(conn, trip_id, freed_seats)

    await notify_users(
        conn,
        background_tasks,
        [trip["driver_id"]],
        "Pasajero Canceló",
        "Un pasajero ha cancelado su asiento en el viaje.",
    )

    return {"message": "Asiento cancelado correctamente", "success": True}
