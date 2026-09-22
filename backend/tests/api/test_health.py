"""Tests del health check (sin base de datos real)."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app.main import app


def _client() -> TestClient:
    # Sin context manager: no se ejecuta el lifespan (no hay BD real).
    return TestClient(app)


def test_health_ok():
    conn = AsyncMock()
    conn.fetchval.return_value = "3.4 USE_GEOS=1"

    @asynccontextmanager
    async def fake_db():
        yield conn

    with patch("app.routers.health.get_db_context", fake_db):
        response = _client().get("/")

    assert response.status_code == 200
    assert response.json() == {
        "service": "Rides API Backend",
        "status": "ok",
        "checks": {"database": "ok"},
    }
    assert response.headers["X-Request-ID"]


def test_health_db_down_returns_503_without_leaking_details():
    @asynccontextmanager
    async def broken_db():
        raise ConnectionRefusedError("password authentication failed for user secreto")
        yield  # pragma: no cover

    with patch("app.routers.health.get_db_context", broken_db):
        response = _client().get("/")

    assert response.status_code == 503
    assert response.json()["checks"]["database"] == "error"
    assert "secreto" not in response.text


def test_request_id_is_propagated():
    conn = AsyncMock()

    @asynccontextmanager
    async def fake_db():
        yield conn

    with patch("app.routers.health.get_db_context", fake_db):
        response = _client().get("/", headers={"X-Request-ID": "abc-123"})

    assert response.headers["X-Request-ID"] == "abc-123"


def test_protected_endpoint_requires_token():
    response = _client().get("/profile/alguien")
    assert response.status_code in (401, 403)
