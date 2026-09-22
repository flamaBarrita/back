"""Configuración centralizada del proyecto.

Todas las variables de entorno se leen una sola vez al arrancar la aplicación.
"""

import os
from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configuración de la aplicación.

    Los valores se cargan desde variables de entorno. Si una variable no está
    definida, se usa el valor por defecto indicado.
    """

    # Base de datos
    database_url: str = "postgresql+asyncpg://user:pass@localhost/rides_db"

    # Cognito
    cognito_region: str = "us-east-1"
    cognito_user_pool_id: str = ""
    # App client de Cognito. Si se define, se rechazan tokens emitidos para
    # otros app clients del mismo user pool.
    cognito_app_client_id: str = ""

    # Firebase
    firebase_enabled: bool = True
    firebase_credentials_path: str = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "firebase.json",
    )

    # Reglas de negocio de viajes
    # Zona horaria en la que la app envía ``departure_time`` (sin offset).
    app_timezone: str = "America/Mexico_City"
    # Minutos que un viaje sigue activo para el conductor después de su salida.
    trip_active_grace_minutes: int = 120
    # Cada cuántos segundos se marcan como completados los viajes vencidos.
    trip_expiration_interval_seconds: int = 300
    # Radio (metros) alrededor del origen/destino del pasajero para el matching.
    search_radius_m: int = 500

    # Logging
    log_level: str = "INFO"
    log_format: Literal["text", "json"] = "text"

    @property
    def database_url_for_asyncpg(self) -> str:
        """Devuelve la URL de PostgreSQL sin el driver +asyncpg.

        asyncpg espera el esquema ``postgresql://`` mientras que algunas
        configuraciones de Docker usan ``postgresql+asyncpg://``.
        """
        return self.database_url.replace("+asyncpg", "")

    @property
    def jwks_url(self) -> str:
        """URL del JWKS endpoint de Cognito."""
        return (
            f"https://cognito-idp.{self.cognito_region}.amazonaws.com/"
            f"{self.cognito_user_pool_id}/.well-known/jwks.json"
        )

    @property
    def cognito_issuer(self) -> str:
        """Issuer esperado en los tokens JWT de Cognito."""
        return (
            f"https://cognito-idp.{self.cognito_region}.amazonaws.com/"
            f"{self.cognito_user_pool_id}"
        )


@lru_cache
def get_settings() -> Settings:
    """Devuelve la configuración cacheada."""
    return Settings()
