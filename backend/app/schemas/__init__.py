"""Re-exporta los esquemas Pydantic del proyecto."""

from app.schemas.notification import FCMTokenUpdate
from app.schemas.profile import FotoActualizar, ProfileUpdate, UserCreate
from app.schemas.request import RequestStatusUpdate, TripRequestCreate
from app.schemas.trip import TripCreate

__all__ = [
    "FCMTokenUpdate",
    "FotoActualizar",
    "ProfileUpdate",
    "RequestStatusUpdate",
    "TripCreate",
    "TripRequestCreate",
    "UserCreate",
]
