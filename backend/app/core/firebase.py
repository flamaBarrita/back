"""Inicialización de Firebase Admin SDK.

Firebase solo se usa para notificaciones push, que no son críticas: si las
credenciales faltan o son inválidas, la API arranca igual y las notificaciones
se omiten (con un error claro en el log de arranque).
"""

import firebase_admin
from firebase_admin import credentials

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


def is_firebase_ready() -> bool:
    """Indica si Firebase Admin está inicializado y se pueden enviar push."""
    return bool(firebase_admin._apps)  # noqa: SLF001


def initialize_firebase() -> bool:
    """Inicializa la app de Firebase Admin si aún no está inicializada.

    Returns:
        True si Firebase quedó disponible.
    """
    if is_firebase_ready():
        return True

    settings = get_settings()
    if not settings.firebase_enabled:
        logger.info("Firebase deshabilitado por configuración; no se enviarán push")
        return False

    try:
        cred = credentials.Certificate(settings.firebase_credentials_path)
        firebase_admin.initialize_app(cred)
    except Exception as exc:
        logger.error(
            "No se pudo inicializar Firebase Admin con %s: %s. "
            "La API sigue funcionando sin notificaciones push.",
            settings.firebase_credentials_path,
            exc,
        )
        return False

    logger.info("Firebase Admin inicializado correctamente")
    return True
