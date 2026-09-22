"""Router para endpoints de notificaciones push."""

import asyncpg
from fastapi import APIRouter, Depends

from app.core.database import get_db
from app.core.security import obtener_usuario_actual
from app.schemas.notification import FCMTokenUpdate
from app.services.profile_service import update_fcm_token

router = APIRouter(
    prefix="",
    tags=["notifications"],
    dependencies=[Depends(obtener_usuario_actual)],
)


@router.patch("/api/users/update-fcm-token")
async def update_fcm_token_endpoint(
    data: FCMTokenUpdate,
    user_id: str = Depends(obtener_usuario_actual),
    conn: asyncpg.Connection = Depends(get_db),
) -> dict:
    """Actualiza el token FCM del usuario autenticado."""
    return await update_fcm_token(conn, user_id, data.fcm_token)
