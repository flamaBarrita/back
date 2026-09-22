"""Solicitudes y asientos (MEJORA_VANNA 1.2): sin sobreventa."""

import asyncio

from tests.conftest import auth
from tests.integration.helpers import publish_trip, register, request_seat, respond

DRIVER = "driver"


async def _seats(db, trip_id: int) -> int:
    return await db.fetchval("SELECT seats_available FROM trips WHERE id = $1", trip_id)


async def test_accept_decrements_seats_and_cancel_restores(client, db):
    await register(client, DRIVER)
    await register(client, "p1")
    trip_id = await publish_trip(client, DRIVER, seats_available=3)

    request_id = await request_seat(client, "p1", trip_id, seats=2)
    assert (await respond(client, DRIVER, request_id, "aceptado")).status_code == 200
    assert await _seats(db, trip_id) == 1

    approved = await client.get("/mis-viajes/aprobados/p1", headers=auth("p1"))
    assert [t["id"] for t in approved.json()] == [trip_id]

    response = await client.patch(f"/trips/{trip_id}/pasajeros/p1/cancelar", headers=auth("p1"))
    assert response.status_code == 200
    assert await _seats(db, trip_id) == 3


async def test_cannot_overbook_by_accepting_many(client, db):
    await register(client, DRIVER)
    trip_id = await publish_trip(client, DRIVER, seats_available=2)
    request_ids = []
    for passenger in ("p1", "p2", "p3"):
        await register(client, passenger)
        request_ids.append(await request_seat(client, passenger, trip_id))

    results = [await respond(client, DRIVER, rid, "aceptado") for rid in request_ids]
    assert [r.status_code for r in results] == [200, 200, 409]
    assert await _seats(db, trip_id) == 0
    assert (
        await db.fetchval(
            "SELECT status FROM trip_requests WHERE id = $1", request_ids[2]
        )
        == "pendiente"
    )


async def test_concurrent_accepts_do_not_oversell(client, db):
    await register(client, DRIVER)
    trip_id = await publish_trip(client, DRIVER, seats_available=1)
    request_ids = []
    for i in range(5):
        passenger = f"p{i}"
        await register(client, passenger)
        request_ids.append(await request_seat(client, passenger, trip_id))

    results = await asyncio.gather(
        *(respond(client, DRIVER, rid, "aceptado") for rid in request_ids)
    )
    assert sorted(r.status_code for r in results) == [200, 409, 409, 409, 409]
    assert await _seats(db, trip_id) == 0
    accepted = await db.fetchval(
        "SELECT COUNT(*) FROM trip_requests WHERE trip_id = $1 AND status = 'aceptado'",
        trip_id,
    )
    assert accepted == 1


async def test_concurrent_duplicate_requests_create_only_one(client, db):
    await register(client, DRIVER)
    await register(client, "p1")
    trip_id = await publish_trip(client, DRIVER)

    results = await asyncio.gather(
        *(
            client.post(
                f"/trips/{trip_id}/requests",
                json={"seats_requested": 1},
                headers=auth("p1"),
            )
            for _ in range(4)
        )
    )
    assert sorted(r.status_code for r in results) == [200, 409, 409, 409]


async def test_request_validations(client):
    await register(client, DRIVER)
    await register(client, "p1")
    trip_id = await publish_trip(client, DRIVER, seats_available=2)

    own = await client.post(
        f"/trips/{trip_id}/requests", json={"seats_requested": 1}, headers=auth(DRIVER)
    )
    assert own.status_code == 400

    too_many = await client.post(
        f"/trips/{trip_id}/requests", json={"seats_requested": 3}, headers=auth("p1")
    )
    assert too_many.status_code == 400

    zero = await client.post(
        f"/trips/{trip_id}/requests", json={"seats_requested": 0}, headers=auth("p1")
    )
    assert zero.status_code == 422

    missing = await client.post(
        "/trips/9999/requests", json={"seats_requested": 1}, headers=auth("p1")
    )
    assert missing.status_code == 404


async def test_status_must_be_valid_and_request_answered_once(client):
    await register(client, DRIVER)
    await register(client, "p1")
    trip_id = await publish_trip(client, DRIVER)
    request_id = await request_seat(client, "p1", trip_id)

    assert (await respond(client, DRIVER, request_id, "aprobado")).status_code == 422
    assert (await respond(client, DRIVER, request_id, "rechazado")).status_code == 200
    assert (await respond(client, DRIVER, request_id, "aceptado")).status_code == 409
    assert (await respond(client, DRIVER, 9999, "aceptado")).status_code == 404


async def test_rejected_passenger_can_request_again(client):
    await register(client, DRIVER)
    await register(client, "p1")
    trip_id = await publish_trip(client, DRIVER)
    request_id = await request_seat(client, "p1", trip_id)
    await respond(client, DRIVER, request_id, "rechazado")
    await request_seat(client, "p1", trip_id)


async def test_cancel_trip_cancels_requests(client, db):
    await register(client, DRIVER)
    await register(client, "p1")
    trip_id = await publish_trip(client, DRIVER)
    request_id = await request_seat(client, "p1", trip_id)
    await respond(client, DRIVER, request_id, "aceptado")

    response = await client.patch(f"/trips/{trip_id}/cancelar", headers=auth(DRIVER))
    assert response.status_code == 200
    assert await db.fetchval("SELECT status FROM trips WHERE id = $1", trip_id) == "cancelado"
    assert (
        await db.fetchval("SELECT status FROM trip_requests WHERE id = $1", request_id)
        == "cancelado"
    )
    again = await client.patch(f"/trips/{trip_id}/cancelar", headers=auth(DRIVER))
    assert again.status_code == 409
