"""Publicación, expiración (MEJORA_VANNA 1.3) y búsqueda (M-09) de viajes."""

from datetime import timedelta

from app.services.trip_service import expire_trips
from app.utils.time import now_local
from tests.conftest import auth
from tests.integration.helpers import (
    FAR_AWAY,
    NEAR_END,
    NEAR_START,
    TZ,
    publish_trip,
    register,
    trip_payload,
)

DRIVER = "driver"


async def _search(client, user, origin, dest):
    response = await client.get(
        "/trips/search",
        params={"olat": origin[0], "olng": origin[1], "dlat": dest[0], "dlng": dest[1]},
        headers=auth(user),
    )
    assert response.status_code == 200, response.text
    return [t["id"] for t in response.json()]


async def _move_departure(db, trip_id: int, minutes_from_now: int) -> None:
    departure = now_local(TZ) + timedelta(minutes=minutes_from_now)
    await db.execute("UPDATE trips SET departure_time = $1 WHERE id = $2", departure, trip_id)


async def test_publish_computes_server_side_fields(client, db):
    await register(client, DRIVER)
    trip_id = await publish_trip(client, DRIVER)
    row = await db.fetchrow("SELECT * FROM trips WHERE id = $1", trip_id)
    # ~10.5 km en línea recta; el "999 km" que mande el cliente se ignora.
    assert row["distance_text"] == "10.5 km"
    assert round(row["origin_lat"], 2) == 19.40 and round(row["dest_lng"], 2) == -99.10
    assert row["status"] == "activo"


async def test_publish_validations(client):
    await register(client, DRIVER)
    bad_payloads = [
        trip_payload(seats_available=0),
        trip_payload(price=-1),
        trip_payload(origin_lat=120),
        trip_payload(origin_name="x" * 300),
    ]
    for payload in bad_payloads:
        response = await client.post(f"/trips/{DRIVER}", json=payload, headers=auth(DRIVER))
        assert response.status_code == 422, payload

    past = (now_local(TZ) - timedelta(hours=1)).isoformat()
    response = await client.post(
        f"/trips/{DRIVER}", json=trip_payload(departure_time=past), headers=auth(DRIVER)
    )
    assert response.status_code == 400

    response = await client.post(
        f"/trips/{DRIVER}",
        json=trip_payload(encoded_polyline="??"),
        headers=auth(DRIVER),
    )
    assert response.status_code == 400


async def test_departure_with_offset_is_converted_to_local(client, db):
    await register(client, DRIVER)
    utc_departure = "2099-01-01T18:00:00+00:00"
    trip_id = await publish_trip(client, DRIVER, departure_time=utc_departure)
    stored = await db.fetchval("SELECT departure_time FROM trips WHERE id = $1", trip_id)
    assert stored.isoformat() == "2099-01-01T12:00:00"  # CDMX = UTC-6


async def test_active_trip_blocks_new_publication_until_grace_expires(client, db):
    await register(client, DRIVER)
    trip_id = await publish_trip(client, DRIVER)

    response = await client.post(f"/trips/{DRIVER}", json=trip_payload(), headers=auth(DRIVER))
    assert response.status_code == 409

    # Salió hace 1h: sigue vigente dentro de las 2h de gracia.
    await _move_departure(db, trip_id, -60)
    active = await client.get(f"/trips/active/{DRIVER}", headers=auth(DRIVER))
    assert active.json()["id"] == trip_id
    response = await client.post(f"/trips/{DRIVER}", json=trip_payload(), headers=auth(DRIVER))
    assert response.status_code == 409

    # Salió hace 3h: ya no bloquea (antes quedaba bloqueado para siempre).
    await _move_departure(db, trip_id, -180)
    active = await client.get(f"/trips/active/{DRIVER}", headers=auth(DRIVER))
    assert active.json() is None
    await publish_trip(client, DRIVER)


async def test_expiration_job_marks_trips_completed(client, db):
    await register(client, DRIVER)
    trip_id = await publish_trip(client, DRIVER)
    await _move_departure(db, trip_id, -60)
    assert await expire_trips() == 0
    await _move_departure(db, trip_id, -121)
    assert await expire_trips() == 1
    assert await db.fetchval("SELECT status FROM trips WHERE id = $1", trip_id) == "completado"


async def test_search_matches_direction_and_hides_unavailable(client, db):
    await register(client, DRIVER)
    await register(client, "p1")
    trip_id = await publish_trip(client, DRIVER, seats_available=1)

    assert await _search(client, "p1", NEAR_START, NEAR_END) == [trip_id]
    # Sentido contrario: la ruta pasa por ambos puntos pero al revés.
    assert await _search(client, "p1", NEAR_END, NEAR_START) == []
    assert await _search(client, "p1", NEAR_START, FAR_AWAY) == []
    # El conductor no ve su propio viaje.
    assert await _search(client, DRIVER, NEAR_START, NEAR_END) == []

    # Sin asientos: no aparece.
    await db.execute("UPDATE trips SET seats_available = 0 WHERE id = $1", trip_id)
    assert await _search(client, "p1", NEAR_START, NEAR_END) == []

    # Ya salió (aunque siga 'activo' dentro de la gracia): no aparece.
    await db.execute("UPDATE trips SET seats_available = 1 WHERE id = $1", trip_id)
    await _move_departure(db, trip_id, -5)
    assert await _search(client, "p1", NEAR_START, NEAR_END) == []


async def test_cannot_request_departed_trip(client, db):
    await register(client, DRIVER)
    await register(client, "p1")
    trip_id = await publish_trip(client, DRIVER)
    await _move_departure(db, trip_id, -5)
    response = await client.post(
        f"/trips/{trip_id}/requests", json={"seats_requested": 1}, headers=auth("p1")
    )
    assert response.status_code == 400
