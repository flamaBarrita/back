"""Router para endpoints de viajes."""

import asyncpg
from fastapi import APIRouter, BackgroundTasks, Depends, Query

from app.core.database import get_db
from app.core.security import ensure_same_user, obtener_usuario_actual
from app.schemas.trip import TripCreate
from app.services.trip_service import (
    cancel_trip,
    create_trip,
    get_active_trip,
    get_approved_trips_for_passenger,
    get_trip_requests,
    search_trips,
)

router = APIRouter(
    prefix="",
    tags=["trips"],
    dependencies=[Depends(obtener_usuario_actual)],
)


@router.get("/trips/search")
async def search_trips_endpoint(
    olat: float = Query(..., ge=-90, le=90),
    olng: float = Query(..., ge=-180, le=180),
    dlat: float = Query(..., ge=-90, le=90),
    dlng: float = Query(..., ge=-180, le=180),
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict]:
    """Busca viajes disponibles que pasen por el origen y luego el destino."""
    return await search_trips(conn, olat, olng, dlat, dlng, current_user)


@router.post("/trips/{driver_id}")
async def create_trip_endpoint(
    driver_id: str,
    trip: TripCreate,
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> dict:
    """Publica un nuevo viaje del conductor autenticado."""
    ensure_same_user(driver_id, current_user)
    return await create_trip(conn, current_user, trip)


@router.get("/trips/active/{driver_id}")
async def get_active_trip_endpoint(
    driver_id: str,
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> dict | None:
    """Obtiene el viaje vigente del conductor autenticado."""
    ensure_same_user(driver_id, current_user)
    return await get_active_trip(conn, current_user)


@router.get("/trips/{trip_id}/requests")
async def get_trip_requests_endpoint(
    trip_id: int,
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict]:
    """Obtiene las solicitudes pendientes de un viaje del conductor autenticado."""
    return await get_trip_requests(conn, trip_id, current_user)


@router.patch("/trips/{trip_id}/cancelar")
async def cancel_trip_endpoint(
    trip_id: int,
    background_tasks: BackgroundTasks,
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> dict:
    """Cancela un viaje del conductor autenticado y notifica a los pasajeros."""
    return await cancel_trip(conn, trip_id, current_user, background_tasks)


@router.get("/mis-viajes/aprobados/{passenger_id}")
async def get_viajes_aprobados(
    passenger_id: str,
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> list[dict]:
    """Obtiene los viajes aprobados del pasajero autenticado."""
    ensure_same_user(passenger_id, current_user)
    return await get_approved_trips_for_passenger(conn, current_user)
