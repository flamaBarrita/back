"""Perfiles, fotos y tokens FCM."""

from tests.conftest import auth
from tests.integration.helpers import register


async def test_profile_roundtrip(client):
    await register(client, "u1", "Ana")
    response = await client.put(
        "/profile/u1",
        json={"biography": "Hola", "preferences": "Sin música", "vehicles": "Aveo"},
        headers=auth("u1"),
    )
    assert response.status_code == 200
    profile = (await client.get("/profile/u1", headers=auth("u1"))).json()
    assert profile["name"] == "Ana"
    assert profile["biography"] == "Hola"


async def test_profile_of_unknown_user_returns_defaults(client):
    response = await client.get("/profile/nadie", headers=auth("u1"))
    assert response.json() == {"biography": "", "preferences": "", "vehicles": ""}


async def test_photo_must_be_https_and_long_urls_fit(client, db):
    await register(client, "u1")
    bad = await client.patch(
        "/usuarios/u1/foto", json={"foto_url": "javascript:alert(1)"}, headers=auth("u1")
    )
    assert bad.status_code == 422

    long_url = "https://firebasestorage.googleapis.com/v0/b/x/o/" + "a" * 400
    ok = await client.patch("/usuarios/u1/foto", json={"foto_url": long_url}, headers=auth("u1"))
    assert ok.status_code == 200
    assert await db.fetchval("SELECT foto_url FROM users WHERE id = 'u1'") == long_url


async def test_fcm_token_moves_between_accounts(client, db):
    await register(client, "u1")
    await register(client, "u2")
    for user in ("u1", "u2"):
        response = await client.patch(
            "/api/users/update-fcm-token", json={"fcm_token": "device-1"}, headers=auth(user)
        )
        assert response.status_code == 200
    rows = await db.fetch("SELECT id, fcm_token FROM users ORDER BY id")
    assert [(r["id"], r["fcm_token"]) for r in rows] == [("u1", None), ("u2", "device-1")]


async def test_updated_at_is_maintained_by_trigger(client, db):
    await register(client, "u1")
    before = await db.fetchval("SELECT updated_at FROM users WHERE id = 'u1'")
    await client.put("/profile/u1", json={"biography": "nueva"}, headers=auth("u1"))
    after = await db.fetchval("SELECT updated_at FROM users WHERE id = 'u1'")
    assert after > before
