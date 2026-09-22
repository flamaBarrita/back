"""Re-exporta los repositorios del proyecto."""

from app.repositories import profile_repo, request_repo, trip_repo

__all__ = ["profile_repo", "request_repo", "trip_repo"]
