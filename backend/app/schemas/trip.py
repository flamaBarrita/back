"""Esquemas Pydantic para viajes."""

from datetime import datetime

from pydantic import BaseModel, Field

from app.schemas.request import MAX_SEATS


class TripCreate(BaseModel):
    """Datos para crear un nuevo viaje.

    ``distance_text`` no se recibe: el backend la calcula a partir de la ruta.
    """

    origin_name: str = Field(..., min_length=1, max_length=255, description="Nombre del origen")
    dest_name: str = Field(..., min_length=1, max_length=255, description="Nombre del destino")
    duration_text: str = Field(..., max_length=50, description="Duración estimada en texto")
    departure_time: datetime = Field(
        ...,
        description=(
            "Fecha y hora de salida. Sin offset se interpreta en APP_TIMEZONE; "
            "con offset se convierte a APP_TIMEZONE."
        ),
    )
    price: float = Field(..., ge=0, le=100_000, description="Precio del viaje")
    seats_available: int = Field(..., ge=1, le=MAX_SEATS, description="Asientos disponibles")
    origin_lat: float = Field(..., ge=-90, le=90, description="Latitud del origen")
    origin_lng: float = Field(..., ge=-180, le=180, description="Longitud del origen")
    dest_lat: float = Field(..., ge=-90, le=90, description="Latitud del destino")
    dest_lng: float = Field(..., ge=-180, le=180, description="Longitud del destino")
    encoded_polyline: str = Field(
        ..., min_length=1, max_length=200_000, description="Ruta codificada como polyline"
    )
