"""Excepciones de dominio de la aplicación.

Estas excepciones permiten manejar errores de negocio de forma uniforme
mediante un manejador global en FastAPI.
"""


class AppError(Exception):
    """Excepción base de la aplicación."""

    def __init__(self, message: str, status_code: int = 500) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class NotFoundError(AppError):
    """Recurso no encontrado."""

    def __init__(self, message: str = "Recurso no encontrado") -> None:
        super().__init__(message, status_code=404)


class ConflictError(AppError):
    """Conflicto de estado o duplicado."""

    def __init__(self, message: str = "Conflicto de estado") -> None:
        super().__init__(message, status_code=409)


class ValidationError(AppError):
    """Error de validación de negocio."""

    def __init__(self, message: str = "Error de validación") -> None:
        super().__init__(message, status_code=400)


class UnauthorizedError(AppError):
    """Error de autenticación: no se pudo identificar al usuario."""

    def __init__(self, message: str = "Acceso no autorizado") -> None:
        super().__init__(message, status_code=401)


class ForbiddenError(AppError):
    """El usuario autenticado no tiene permiso sobre el recurso."""

    def __init__(self, message: str = "No tienes permiso para realizar esta acción") -> None:
        super().__init__(message, status_code=403)
