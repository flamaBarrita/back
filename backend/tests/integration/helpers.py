"""Datos y utilidades compartidas por los tests de integración."""

from datetime import timedelta

import polyline
from httpx import AsyncClient

from app.utils.time import now_local
from tests.conftest import auth

TZ = "America/Mexico_City"

# Ruta de oeste a este sobre la latitud 19.40 (CDMX).
ROUTE_COORDS = [(19.40, -99.20 + i * 0.01) for i in range(11)]
ROUTE_POLYLINE = polyline.encode(ROUTE_COORDS)

# Puntos del pasajero a ~100m de la ruta, en el sentido del viaje.
NEAR_START = (19.401, -99.18)
NEAR_END = (19.401, -99.12)
FAR_AWAY = (19.60, -99.50)


def future_departure(hours: int = 24) -> str:
    """Fecha de salida futura en hora local, sin offset (como la manda la app)."""
    return (now_local(TZ) + timedelta(hours=hours)).replace(microsecond=0).isoformat()


def trip_payload(**overrides) -> dict:
    payload = {
        "origin_name": "Colonia A",
        "dest_name": "Campus",
        "duration_text": "25 min",
        "departure_time": future_departure(),
        "price": 35.0,
        "seats_available": 3,
        "origin_lat": ROUTE_COORDS[0][0],
        "origin_lng": ROUTE_COORDS[0][1],
        "dest_lat": ROUTE_COORDS[-1][0],
        "dest_lng": ROUTE_COORDS[-1][1],
        "encoded_polyline": ROUTE_POLYLINE,
        # Campos que manda la app y el backend ignora
        "distance_text": "999 km",
    }
    payload.update(overrides)
    return payload


async def register(client: AsyncClient, user_id: str, name: str | None = None) -> None:
    response = await client.post(
        f"/users/{user_id}", json={"name": name or user_id}, headers=auth(user_id)
    )
    assert response.status_code == 200, response.text


async def publish_trip(client: AsyncClient, driver_id: str, **overrides) -> int:
    response = await client.post(
        f"/trips/{driver_id}", json=trip_payload(**overrides), headers=auth(driver_id)
    )
    assert response.status_code == 200, response.text
    return response.json()["trip_id"]


async def request_seat(
    client: AsyncClient, passenger_id: str, trip_id: int, seats: int = 1
) -> int:
    response = await client.post(
        f"/trips/{trip_id}/requests",
        json={"seats_requested": seats},
        headers=auth(passenger_id),
    )
    assert response.status_code == 200, response.text
    return response.json()["request_id"]


async def respond(client: AsyncClient, driver_id: str, request_id: int, status: str):
    return await client.put(
        f"/requests/{request_id}/status",
        json={"status": status},
        headers=auth(driver_id),
    )
