"""Router de health check.

Es público (lo usa el HEALTHCHECK de Docker), así que no expone versiones ni
mensajes de error internos: esos detalles solo van al log.
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.core.database import get_db_context
from app.core.logging import get_logger

router = APIRouter(tags=["health"])
logger = get_logger(__name__)


@router.get("/")
async def health_check() -> JSONResponse:
    """Verifica la conexión con PostgreSQL/PostGIS. Responde 503 si falla."""
    database_ok = True
    try:
        async with get_db_context() as conn:
            await conn.fetchval("SELECT PostGIS_version()")
    except Exception as exc:
        logger.error("Health check falló en PostgreSQL: %s", exc)
        database_ok = False

    return JSONResponse(
        status_code=200 if database_ok else 503,
        content={
            "service": "Rides API Backend",
            "status": "ok" if database_ok else "degraded",
            "checks": {"database": "ok" if database_ok else "error"},
        },
    )
