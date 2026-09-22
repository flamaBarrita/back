"""Autenticación y autorización mediante AWS Cognito."""

from functools import lru_cache

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient

from app.core.config import get_settings
from app.core.exceptions import ForbiddenError
from app.core.logging import get_logger

logger = get_logger(__name__)
security = HTTPBearer()


@lru_cache
def get_jwks_client() -> PyJWKClient:
    """Devuelve el cliente JWKS de Cognito.

    Es un singleton para que el cache de llaves de PyJWT se reutilice entre
    peticiones en vez de descargar el JWKS en cada request.
    """
    settings = get_settings()
    return PyJWKClient(settings.jwks_url, cache_keys=True, lifespan=3600)


def validate_cognito_claims(payload: dict) -> None:
    """Valida los claims específicos de Cognito que PyJWT no revisa.

    Cognito no incluye ``aud`` en los access tokens; en su lugar se valida
    ``token_use`` y ``client_id``.

    Raises:
        jwt.InvalidTokenError: Si algún claim no es el esperado.
    """
    if payload.get("token_use") != "access":
        raise jwt.InvalidTokenError("token_use inválido: se esperaba un access token")

    expected_client_id = get_settings().cognito_app_client_id
    if expected_client_id and payload.get("client_id") != expected_client_id:
        raise jwt.InvalidTokenError("client_id no corresponde a esta aplicación")


async def obtener_usuario_actual(
    credenciales: HTTPAuthorizationCredentials = Depends(security),
) -> str:
    """Valida el token JWT de Cognito y devuelve el ID del usuario (sub).

    Args:
        credenciales: Credenciales de autorización extraídas del header.

    Returns:
        El identificador único del usuario (claim ``sub``).

    Raises:
        HTTPException: 401 si el token es inválido o expiró.
    """
    token = credenciales.credentials
    settings = get_settings()

    try:
        signing_key = get_jwks_client().get_signing_key_from_jwt(token)
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            issuer=settings.cognito_issuer,
            options={"verify_aud": False, "require": ["exp", "iss", "sub"]},
        )
        validate_cognito_claims(payload)
    except jwt.ExpiredSignatureError as exc:
        logger.warning("Token expirado")
        raise HTTPException(
            status_code=401,
            detail="Tu sesión expiró. Vuelve a iniciar sesión.",
        ) from exc
    except Exception as exc:
        logger.warning("Token inválido: %s", exc)
        raise HTTPException(
            status_code=401,
            detail="Acceso no autorizado.",
        ) from exc

    return payload["sub"]


def ensure_same_user(resource_user_id: str, current_user_id: str) -> None:
    """Verifica que el ID recibido en la URL sea el del usuario autenticado.

    Las rutas conservan el ID en el path por compatibilidad con la app, pero la
    identidad real siempre sale del JWT.

    Raises:
        ForbiddenError: Si los IDs no coinciden.
    """
    if resource_user_id != current_user_id:
        raise ForbiddenError()
