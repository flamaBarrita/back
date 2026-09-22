"""Router para endpoints de solicitudes de viaje."""

import asyncpg
from fastapi import APIRouter, BackgroundTasks, Depends

from app.core.database import get_db
from app.core.security import ensure_same_user, obtener_usuario_actual
from app.schemas.request import RequestStatusUpdate, TripRequestCreate
from app.services.request_service import (
    cancel_passenger_seat,
    create_trip_request,
    update_request_status,
)

router = APIRouter(
    prefix="",
    tags=["requests"],
    dependencies=[Depends(obtener_usuario_actual)],
)


@router.put("/requests/{request_id}/status")
async def update_request_status_endpoint(
    request_id: int,
    update: RequestStatusUpdate,
    background_tasks: BackgroundTasks,
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> dict:
    """El conductor acepta o rechaza una solicitud y se notifica al pasajero."""
    return await update_request_status(
        conn, request_id, update.status, current_user, background_tasks
    )


@router.post("/trips/{trip_id}/requests")
async def create_trip_request_endpoint(
    trip_id: int,
    req: TripRequestCreate,
    background_tasks: BackgroundTasks,
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> dict:
    """El usuario autenticado solicita unirse a un viaje."""
    return await create_trip_request(conn, trip_id, current_user, req, background_tasks)


@router.patch("/trips/{trip_id}/pasajeros/{passenger_id}/cancelar")
async def cancelar_asiento_pasajero(
    trip_id: int,
    passenger_id: str,
    background_tasks: BackgroundTasks,
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> dict:
    """El pasajero autenticado cancela su reserva en un viaje."""
    ensure_same_user(passenger_id, current_user)
    return await cancel_passenger_seat(conn, trip_id, current_user, background_tasks)
