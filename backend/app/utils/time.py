"""Utilidades de fecha y hora.

La columna ``trips.departure_time`` es ``TIMESTAMP WITHOUT TIME ZONE`` y guarda
la hora local de ``APP_TIMEZONE`` (así la envía la app). En SQL se compara
contra ``NOW() AT TIME ZONE <APP_TIMEZONE>``.
"""

from datetime import datetime
from zoneinfo import ZoneInfo


def now_local(tz_name: str) -> datetime:
    """Hora actual en ``tz_name`` como datetime naive."""
    return datetime.now(ZoneInfo(tz_name)).replace(tzinfo=None)


def to_local_naive(value: datetime, tz_name: str) -> datetime:
    """Normaliza un datetime a hora local naive de ``tz_name``.

    Un datetime sin zona se asume ya expresado en ``tz_name``; uno con zona se
    convierte.
    """
    if value.tzinfo is None:
        return value
    return value.astimezone(ZoneInfo(tz_name)).replace(tzinfo=None)
