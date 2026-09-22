"""Esquemas Pydantic para perfiles de usuario."""

from pydantic import BaseModel, Field


class ProfileUpdate(BaseModel):
    """Datos para actualizar el perfil de un usuario."""

    biography: str | None = Field(
        default=None, max_length=2000, description="Biografía del usuario"
    )
    preferences: str | None = Field(
        default=None, max_length=1000, description="Preferencias de viaje"
    )
    vehicles: str | None = Field(
        default=None, max_length=1000, description="Vehículos registrados"
    )


class UserCreate(BaseModel):
    """Datos para registrar un nuevo usuario."""

    name: str = Field(
        ..., min_length=1, max_length=100, description="Nombre del usuario"
    )


class FotoActualizar(BaseModel):
    """Datos para actualizar la foto de perfil."""

    foto_url: str = Field(
        ...,
        max_length=1024,
        pattern=r"^https://\S+$",
        description="URL pública (https) de la foto de perfil",
    )
