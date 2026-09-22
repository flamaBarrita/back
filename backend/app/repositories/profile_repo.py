"""Repositorio de acceso a datos para perfiles de usuario."""

import asyncpg

from app.schemas.profile import ProfileUpdate


async def get_profile(conn: asyncpg.Connection, user_id: str) -> asyncpg.Record | None:
    """Obtiene el perfil público de un usuario por su ID."""
    query = """
        SELECT name, biography, preferences, vehicles, foto_url
        FROM users
        WHERE id = $1
    """
    return await conn.fetchrow(query, user_id)


async def get_user_name(conn: asyncpg.Connection, user_id: str) -> str | None:
    """Obtiene el nombre de un usuario."""
    return await conn.fetchval("SELECT name FROM users WHERE id = $1;", user_id)


async def upsert_profile(
    conn: asyncpg.Connection,
    user_id: str,
    profile: ProfileUpdate,
    default_name: str,
) -> str:
    """Crea o actualiza el perfil de un usuario.

    ``default_name`` solo se usa si el usuario aún no existe.

    Returns:
        El ID del usuario afectado.
    """
    query = """
        INSERT INTO users (id, name, biography, preferences, vehicles)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (id) DO UPDATE
        SET biography = EXCLUDED.biography,
            preferences = EXCLUDED.preferences,
            vehicles = EXCLUDED.vehicles
        RETURNING id;
    """
    return await conn.fetchval(
        query,
        user_id,
        default_name,
        profile.biography,
        profile.preferences,
        profile.vehicles,
    )


async def create_initial_user(
    conn: asyncpg.Connection,
    user_id: str,
    name: str,
) -> None:
    """Registra un nuevo usuario cuando se crea su cuenta en Cognito."""
    query = """
        INSERT INTO users (id, name)
        VALUES ($1, $2)
        ON CONFLICT (id) DO NOTHING;
    """
    await conn.execute(query, user_id, name)


async def update_fcm_token(
    conn: asyncpg.Connection,
    user_id: str,
    fcm_token: str,
) -> None:
    """Asigna el token FCM a un usuario.

    Un dispositivo pertenece a una sola sesión: si otro usuario tenía el mismo
    token (cambio de cuenta en el mismo teléfono), se le quita para que no
    reciba notificaciones ajenas.
    """
    async with conn.transaction():
        await conn.execute(
            "UPDATE users SET fcm_token = NULL WHERE fcm_token = $1 AND id <> $2;",
            fcm_token,
            user_id,
        )
        await conn.execute(
            "UPDATE users SET fcm_token = $1 WHERE id = $2;",
            fcm_token,
            user_id,
        )


async def clear_fcm_tokens(conn: asyncpg.Connection, fcm_tokens: list[str]) -> None:
    """Elimina tokens FCM que Firebase reportó como dados de baja."""
    await conn.execute(
        "UPDATE users SET fcm_token = NULL WHERE fcm_token = ANY($1::text[]);",
        fcm_tokens,
    )


async def update_foto_url(
    conn: asyncpg.Connection,
    user_id: str,
    foto_url: str,
) -> bool:
    """Actualiza la URL de la foto de perfil de un usuario.

    Returns:
        True si el usuario existía.
    """
    result = await conn.execute(
        "UPDATE users SET foto_url = $1 WHERE id = $2;",
        foto_url,
        user_id,
    )
    return result != "UPDATE 0"


async def get_fcm_tokens(
    conn: asyncpg.Connection,
    user_ids: list[str],
) -> list[str]:
    """Obtiene los tokens FCM (sin repetir) de una lista de usuarios."""
    query = """
        SELECT DISTINCT fcm_token
        FROM users
        WHERE id = ANY($1::varchar[]) AND fcm_token IS NOT NULL AND fcm_token <> '';
    """
    rows = await conn.fetch(query, user_ids)
    return [row["fcm_token"] for row in rows]
