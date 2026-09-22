"""Regresión del IDOR (MEJORA_VANNA 1.1): nadie actúa en nombre de otro."""

from tests.conftest import auth
from tests.integration.helpers import publish_trip, register, request_seat, trip_payload

ALICE, BOB, MALLORY = "alice", "bob", "mallory"


async def _setup(client):
    for user in (ALICE, BOB, MALLORY):
        await register(client, user)
    trip_id = await publish_trip(client, ALICE)
    request_id = await request_seat(client, BOB, trip_id)
    return trip_id, request_id


async def test_cannot_edit_other_profile(client):
    await _setup(client)
    response = await client.put(
        f"/profile/{ALICE}", json={"biography": "hackeado"}, headers=auth(MALLORY)
    )
    assert response.status_code == 403


async def test_cannot_change_other_photo(client):
    await _setup(client)
    response = await client.patch(
        f"/usuarios/{ALICE}/foto",
        json={"foto_url": "https://evil.example.com/x.png"},
        headers=auth(MALLORY),
    )
    assert response.status_code == 403


async def test_cannot_register_as_other_user(client):
    response = await client.post(f"/users/{ALICE}", json={"name": "x"}, headers=auth(MALLORY))
    assert response.status_code == 403


async def test_cannot_publish_trip_as_other_driver(client):
    await _setup(client)
    response = await client.post(f"/trips/{BOB}", json=trip_payload(), headers=auth(MALLORY))
    assert response.status_code == 403


async def test_cannot_cancel_other_driver_trip(client, db):
    trip_id, _ = await _setup(client)
    response = await client.patch(f"/trips/{trip_id}/cancelar", headers=auth(MALLORY))
    assert response.status_code == 403
    assert await db.fetchval("SELECT status FROM trips WHERE id = $1", trip_id) == "activo"


async def test_cannot_answer_requests_of_other_driver_trip(client, db):
    _, request_id = await _setup(client)
    # Ni un tercero ni el propio pasajero pueden auto-aprobarse.
    for intruder in (MALLORY, BOB):
        response = await client.put(
            f"/requests/{request_id}/status",
            json={"status": "aceptado"},
            headers=auth(intruder),
        )
        assert response.status_code == 403
    status = await db.fetchval("SELECT status FROM trip_requests WHERE id = $1", request_id)
    assert status == "pendiente"


async def test_cannot_cancel_other_passenger_seat(client):
    trip_id, request_id = await _setup(client)
    await client.put(
        f"/requests/{request_id}/status", json={"status": "aceptado"}, headers=auth(ALICE)
    )
    response = await client.patch(
        f"/trips/{trip_id}/pasajeros/{BOB}/cancelar", headers=auth(MALLORY)
    )
    assert response.status_code == 403


async def test_request_body_cannot_impersonate_passenger(client, db):
    trip_id, _ = await _setup(client)
    response = await client.post(
        f"/trips/{trip_id}/requests",
        json={
            "seats_requested": 1,
            "passenger_id": BOB,
            "sender_id": BOB,
            "passenger_name": "Bob",
        },
        headers=auth(MALLORY),
    )
    assert response.status_code == 200
    passenger = await db.fetchval(
        "SELECT passenger_id FROM trip_requests WHERE id = $1",
        response.json()["request_id"],
    )
    assert passenger == MALLORY


async def test_cannot_read_other_users_private_data(client):
    trip_id, _ = await _setup(client)
    assert (await client.get(f"/trips/active/{ALICE}", headers=auth(MALLORY))).status_code == 403
    assert (await client.get(f"/trips/{trip_id}/requests", headers=auth(MALLORY))).status_code == 403
    assert (
        await client.get(f"/mis-viajes/aprobados/{BOB}", headers=auth(MALLORY))
    ).status_code == 403


async def test_owner_can_access_own_resources(client):
    trip_id, _ = await _setup(client)
    assert (await client.get(f"/trips/active/{ALICE}", headers=auth(ALICE))).status_code == 200
    requests = await client.get(f"/trips/{trip_id}/requests", headers=auth(ALICE))
    assert requests.status_code == 200
    assert [r["passenger_id"] for r in requests.json()] == [BOB]
    assert (await client.get(f"/mis-viajes/aprobados/{BOB}", headers=auth(BOB))).status_code == 200


async def test_public_profile_is_readable_by_others(client):
    await _setup(client)
    response = await client.get(f"/profile/{ALICE}", headers=auth(BOB))
    assert response.status_code == 200
    assert response.json()["name"] == ALICE
    assert "fcm_token" not in response.json()
