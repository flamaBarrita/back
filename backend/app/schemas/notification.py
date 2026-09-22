"""Esquemas Pydantic para notificaciones push."""

from pydantic import BaseModel, Field


class FCMTokenUpdate(BaseModel):
    """Datos para actualizar el token FCM de un usuario."""

    fcm_token: str = Field(
        ...,
        min_length=1,
        max_length=4096,
        description="Token de Firebase Cloud Messaging",
    )
