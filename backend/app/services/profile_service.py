"""Servicio de lógica de negocio para perfiles de usuario."""

import asyncpg

from app.core.exceptions import NotFoundError
from app.repositories import profile_repo
from app.schemas.profile import FotoActualizar, ProfileUpdate, UserCreate

# Nombre provisional si el usuario guarda su perfil antes de registrarse.
DEFAULT_USER_NAME = "Usuario"


async def get_profile_or_default(conn: asyncpg.Connection, user_id: str) -> dict:
    """Obtiene el perfil de un usuario o valores por defecto si no existe."""
    row = await profile_repo.get_profile(conn, user_id)
    if row:
        return dict(row)
    return {"biography": "", "preferences": "", "vehicles": ""}


async def update_profile(
    conn: asyncpg.Connection,
    user_id: str,
    profile: ProfileUpdate,
) -> dict:
    """Actualiza o crea el perfil de un usuario."""
    updated_id = await profile_repo.upsert_profile(
        conn, user_id, profile, DEFAULT_USER_NAME
    )
    return {"message": "Perfil guardado correctamente", "id": updated_id}


async def create_initial_user(
    conn: asyncpg.Connection,
    user_id: str,
    user: UserCreate,
) -> dict:
    """Registra un nuevo usuario proveniente de Cognito."""
    await profile_repo.create_initial_user(conn, user_id, user.name.strip())
    return {"message": "Usuario registrado en PostgreSQL correctamente"}


async def update_fcm_token(
    conn: asyncpg.Connection,
    user_id: str,
    fcm_token: str,
) -> dict:
    """Actualiza el token FCM de un usuario."""
    await profile_repo.update_fcm_token(conn, user_id, fcm_token)
    return {"success": True, "message": "Token de notificaciones actualizado"}


async def update_foto(
    conn: asyncpg.Connection,
    user_id: str,
    payload: FotoActualizar,
) -> dict:
    """Actualiza la foto de perfil de un usuario."""
    if not await profile_repo.update_foto_url(conn, user_id, payload.foto_url):
        raise NotFoundError("El usuario no existe")

    return {
        "mensaje": "Foto de perfil actualizada exitosamente",
        "foto_url": payload.foto_url,
    }
