"""Tests de validación de tokens de Cognito (sin red: JWKS simulado)."""

import time
from types import SimpleNamespace
from unittest.mock import patch

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from app.core.config import get_settings
from app.core.exceptions import ForbiddenError
from app.core.security import ensure_same_user, get_jwks_client, obtener_usuario_actual

PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


class FakeJwksClient:
    def get_signing_key_from_jwt(self, token):
        return SimpleNamespace(key=PRIVATE_KEY.public_key())


def make_token(key=PRIVATE_KEY, **overrides) -> str:
    settings = get_settings()
    claims = {
        "sub": "user-123",
        "iss": settings.cognito_issuer,
        "token_use": "access",
        "client_id": "app-client",
        "exp": int(time.time()) + 300,
    }
    claims.update(overrides)
    claims = {k: v for k, v in claims.items() if v is not None}
    return jwt.encode(claims, key, algorithm="RS256")


async def authenticate(token: str) -> str:
    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
    with patch("app.core.security.get_jwks_client", return_value=FakeJwksClient()):
        return await obtener_usuario_actual(creds)


@pytest.fixture
def client_id_setting():
    settings = get_settings()
    original = settings.cognito_app_client_id
    settings.cognito_app_client_id = "app-client"
    yield
    settings.cognito_app_client_id = original


async def test_valid_access_token_returns_sub(client_id_setting):
    assert await authenticate(make_token()) == "user-123"


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"token_use": "id"}, id="id-token"),
        pytest.param({"token_use": None}, id="sin-token_use"),
        pytest.param({"client_id": "otra-app"}, id="otro-app-client"),
        pytest.param({"iss": "https://evil.example.com"}, id="otro-issuer"),
        pytest.param({"sub": None}, id="sin-sub"),
    ],
)
async def test_rejects_invalid_claims(client_id_setting, overrides):
    with pytest.raises(HTTPException) as exc:
        await authenticate(make_token(**overrides))
    assert exc.value.status_code == 401


async def test_rejects_token_signed_with_other_key(client_id_setting):
    with pytest.raises(HTTPException) as exc:
        await authenticate(make_token(key=OTHER_KEY))
    assert exc.value.status_code == 401


async def test_expired_token_has_specific_message(client_id_setting):
    with pytest.raises(HTTPException) as exc:
        await authenticate(make_token(exp=int(time.time()) - 10))
    assert exc.value.status_code == 401
    assert "expiró" in exc.value.detail


async def test_client_id_not_checked_when_not_configured():
    assert get_settings().cognito_app_client_id == ""
    assert await authenticate(make_token(client_id="cualquiera")) == "user-123"


def test_jwks_client_is_singleton():
    assert get_jwks_client() is get_jwks_client()


def test_ensure_same_user():
    ensure_same_user("a", "a")
    with pytest.raises(ForbiddenError):
        ensure_same_user("a", "b")
