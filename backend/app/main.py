"""Punto de entrada de la aplicación FastAPI.

Este módulo solo se encarga de crear la aplicación, configurar el ciclo de vida,
el middleware y los manejadores de errores, y registrar los routers. Toda la
lógica de negocio vive en los módulos ``routers``, ``services`` y
``repositories``.
"""

import asyncio
import contextlib
import re
import uuid
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.core.database import close_pool, get_pool
from app.core.exceptions import AppError
from app.core.firebase import initialize_firebase
from app.core.logging import configure_logging, get_logger, request_id_var
from app.routers import health, notifications, profile, requests, trips
from app.services.trip_service import run_trip_expiration_loop

settings = get_settings()
configure_logging(settings.log_level, settings.log_format)
logger = get_logger(__name__)

_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Ciclo de vida: Firebase, pool de PostgreSQL y job de expiración."""
    initialize_firebase()
    await get_pool()
    expiration_task = asyncio.create_task(run_trip_expiration_loop())
    logger.info("API iniciada")
    try:
        yield
    finally:
        expiration_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await expiration_task
        await close_pool()
        logger.info("API detenida")


app = FastAPI(
    title="Rides API",
    description="API de rides/compartir viajes con autenticación Cognito.",
    docs_url="/mario_docs-production_secure",
    redoc_url=None,
    debug=False,
    lifespan=lifespan,
)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """Asigna un request-id a cada petición para correlacionar logs.

    Reutiliza el header ``X-Request-ID`` si viene uno válido y lo devuelve en
    la respuesta.
    """
    incoming = request.headers.get("x-request-id", "")
    request_id = incoming if _REQUEST_ID_RE.match(incoming) else uuid.uuid4().hex
    token = request_id_var.set(request_id)
    try:
        response = await call_next(request)
    except Exception:
        # Último recurso: se registra el traceback y nunca se expone al cliente.
        logger.exception(
            "Error no controlado en %s %s", request.method, request.url.path
        )
        response = JSONResponse(
            status_code=500,
            content={"detail": "Error interno del servidor"},
        )
    finally:
        request_id_var.reset(token)
    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    """Convierte excepciones de dominio en respuestas HTTP uniformes."""
    logger.warning(
        "Error de dominio en %s %s: %s",
        request.method,
        request.url.path,
        exc.message,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.message},
    )


@app.exception_handler(asyncpg.IntegrityConstraintViolationError)
async def integrity_error_handler(
    request: Request, exc: asyncpg.IntegrityConstraintViolationError
) -> JSONResponse:
    """Traduce violaciones de constraints a errores de cliente, no 500."""
    logger.warning("Violación de integridad en %s: %s", request.url.path, exc)
    if isinstance(exc, asyncpg.UniqueViolationError):
        return JSONResponse(
            status_code=409,
            content={"detail": "El registro ya existe o está duplicado"},
        )
    return JSONResponse(
        status_code=400,
        content={"detail": "Los datos no cumplen las reglas de la base de datos"},
    )


@app.exception_handler(asyncpg.PostgresError)
async def postgres_error_handler(request: Request, exc: asyncpg.PostgresError) -> JSONResponse:
    """Maneja errores de PostgreSQL devolviendo un error 500 controlado."""
    logger.error("Error de PostgreSQL en %s: %s", request.url.path, exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Error en base de datos"},
    )


# Registro de routers
app.include_router(health.router)
app.include_router(profile.router)
app.include_router(trips.router)
app.include_router(requests.router)
app.include_router(notifications.router)
