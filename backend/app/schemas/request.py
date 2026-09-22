"""Esquemas Pydantic para solicitudes de viaje."""

from typing import Literal

from pydantic import BaseModel, Field

MAX_SEATS = 8


class TripRequestCreate(BaseModel):
    """Datos para crear una solicitud de unión a un viaje.

    El pasajero siempre es el usuario autenticado (claim ``sub`` del JWT). Los
    campos ``passenger_id``, ``sender_id``, ``passenger_name``,
    ``passenger_photo`` y ``passenger_rating`` que envía la app actual se
    ignoran.
    """

    seats_requested: int = Field(
        ..., ge=1, le=MAX_SEATS, description="Asientos solicitados"
    )


class RequestStatusUpdate(BaseModel):
    """Datos para que el conductor responda a una solicitud pendiente."""

    status: Literal["aceptado", "rechazado"] = Field(
        ..., description="Nuevo estado de la solicitud"
    )
