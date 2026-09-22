"""Gestión de la conexión a PostgreSQL mediante asyncpg.

La aplicación usa un único pool de conexiones por proceso que se crea al
iniciar y se libera al cerrar. Los routers obtienen conexiones del pool
mediante la dependencia ``get_db``.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Pool global de conexiones a PostgreSQL.
_pool: asyncpg.Pool | None = None


async def create_pool() -> asyncpg.Pool:
    """Crea el pool de conexiones a PostgreSQL."""
    settings = get_settings()
    logger.info("Creando pool de conexiones a PostgreSQL")
    return await asyncpg.create_pool(
        settings.database_url_for_asyncpg,
        min_size=2,
        max_size=10,
    )


async def close_pool() -> None:
    """Cierra el pool de conexiones."""
    global _pool
    if _pool is not None:
        logger.info("Cerrando pool de conexiones a PostgreSQL")
        await _pool.close()
        _pool = None


async def get_pool() -> asyncpg.Pool:
    """Devuelve el pool actual, creándolo si es necesario."""
    global _pool
    if _pool is None:
        _pool = await create_pool()
    return _pool


@asynccontextmanager
async def get_db_context() -> AsyncGenerator[asyncpg.Connection, None]:
    """Context manager que proporciona una conexión del pool.

    Útil fuera de las dependencias de FastAPI: health checks, tareas en
    segundo plano y jobs periódicos.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    """Dependencia de FastAPI que proporciona una conexión del pool.

    La conexión se libera automáticamente al terminar la petición.
    """
    async with get_db_context() as conn:
        yield conn
