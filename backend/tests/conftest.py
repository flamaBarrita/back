"""Fixtures y configuración común para los tests.

Los tests de integración (``tests/integration``) usan una base PostGIS real
indicada en ``TEST_DATABASE_URL`` y se saltan si no está definida. Ejemplo:

    docker run -d --rm --name rides_test_db -p 55432:5432 \
        -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=rides_test \
        postgis/postgis:15-3.4
    TEST_DATABASE_URL=postgresql://test:test@localhost:55432/rides_test pytest

¡La base indicada se vacía en cada test! No usar una base con datos reales.
"""

import asyncio
import os
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import Request
from httpx import ASGITransport, AsyncClient

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")

# La configuración se lee al importar la app, así que se define antes.
os.environ["FIREBASE_ENABLED"] = "false"
os.environ["TRIP_EXPIRATION_INTERVAL_SECONDS"] = "3600"
os.environ["APP_TIMEZONE"] = "America/Mexico_City"
if TEST_DATABASE_URL:
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

from app.core.security import obtener_usuario_actual  # noqa: E402
from app.main import app  # noqa: E402

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "esquema.sql"
TEST_USER_HEADER = "X-Test-User"


def _fake_current_user(request: Request) -> str:
    """Sustituye la validación de Cognito: el usuario viene de un header."""
    return request.headers[TEST_USER_HEADER]


def auth(user_id: str) -> dict:
    """Headers para actuar como ``user_id`` en los tests."""
    return {TEST_USER_HEADER: user_id}


@pytest.fixture(scope="session")
def database_schema() -> str:
    """Recrea el esquema completo una vez por sesión."""
    if not TEST_DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL no definida; se omiten tests de integración")

    async def _create() -> None:
        conn = await asyncpg.connect(TEST_DATABASE_URL)
        try:
            await conn.execute(
                "DROP TABLE IF EXISTS trip_requests, trips, users, schema_migrations CASCADE;"
            )
            await conn.execute(SCHEMA_PATH.read_text())
        finally:
            await conn.close()

    asyncio.run(_create())
    return TEST_DATABASE_URL


@pytest_asyncio.fixture
async def db(database_schema: str):
    """Conexión directa a la BD de pruebas, con las tablas vacías."""
    conn = await asyncpg.connect(database_schema)
    await conn.execute("TRUNCATE trip_requests, trips, users RESTART IDENTITY CASCADE;")
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def client(db):
    """Cliente HTTP contra la app real, con autenticación simulada."""
    app.dependency_overrides[obtener_usuario_actual] = _fake_current_user
    try:
        async with LifespanManager(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                yield ac
    finally:
        app.dependency_overrides.clear()
