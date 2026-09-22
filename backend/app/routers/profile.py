"""Router para endpoints relacionados con perfiles de usuario.

Las rutas conservan el ID de usuario en el path por compatibilidad con la app,
pero en las operaciones de escritura debe coincidir con el usuario del JWT.
"""

import asyncpg
from fastapi import APIRouter, Depends

from app.core.database import get_db
from app.core.security import ensure_same_user, obtener_usuario_actual
from app.schemas.profile import FotoActualizar, ProfileUpdate, UserCreate
from app.services.profile_service import (
    create_initial_user,
    get_profile_or_default,
    update_foto,
    update_profile,
)

router = APIRouter(
    prefix="",
    tags=["profile"],
    dependencies=[Depends(obtener_usuario_actual)],
)


@router.get("/profile/{user_id}")
async def get_profile(
    user_id: str,
    conn: asyncpg.Connection = Depends(get_db),
) -> dict:
    """Obtiene el perfil público de un usuario (cualquier usuario autenticado)."""
    return await get_profile_or_default(conn, user_id)


@router.put("/profile/{user_id}")
async def update_profile_endpoint(
    user_id: str,
    profile: ProfileUpdate,
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> dict:
    """Actualiza o crea el perfil del usuario autenticado."""
    ensure_same_user(user_id, current_user)
    return await update_profile(conn, current_user, profile)


@router.post("/users/{user_id}")
async def create_initial_user_endpoint(
    user_id: str,
    user: UserCreate,
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> dict:
    """Registra en la BD al usuario autenticado de Cognito."""
    ensure_same_user(user_id, current_user)
    return await create_initial_user(conn, current_user, user)


@router.patch("/usuarios/{usuario_id}/foto")
async def actualizar_foto_perfil(
    usuario_id: str,
    payload: FotoActualizar,
    current_user: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> dict:
    """Actualiza la URL de la foto de perfil del usuario autenticado."""
    ensure_same_user(usuario_id, current_user)
    return await update_foto(conn, current_user, payload)
