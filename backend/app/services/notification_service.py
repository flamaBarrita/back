"""Servicio de envío de notificaciones push."""

import asyncpg
from fastapi import BackgroundTasks
from fastapi.concurrency import run_in_threadpool
from firebase_admin import messaging

from app.core.database import get_db_context
from app.core.firebase import is_firebase_ready
from app.core.logging import get_logger
from app.repositories import profile_repo

logger = get_logger(__name__)

# Errores de FCM que indican que el token ya no sirve y debe borrarse.
_DEAD_TOKEN_ERRORS = (messaging.UnregisteredError, messaging.SenderIdMismatchError)


async def deliver_push(fcm_tokens: list[str], title: str, body: str) -> None:
    """Envía una notificación a varios dispositivos en una sola llamada.

    Los tokens que Firebase reporta como dados de baja se eliminan de la BD.
    Nunca lanza excepciones: una falla de push no debe afectar a la API.
    """
    if not fcm_tokens:
        return
    if not is_firebase_ready():
        logger.debug("Firebase no disponible; se omite push '%s'", title)
        return

    message = messaging.MulticastMessage(
        notification=messaging.Notification(title=title, body=body),
        tokens=fcm_tokens,
    )
    try:
        response = await run_in_threadpool(messaging.send_each_for_multicast, message)
    except Exception:
        logger.exception("Error al enviar notificación FCM '%s'", title)
        return

    dead_tokens = []
    for token, result in zip(fcm_tokens, response.responses):
        if result.success:
            continue
        if isinstance(result.exception, _DEAD_TOKEN_ERRORS):
            dead_tokens.append(token)
        else:
            logger.warning("Falló push FCM '%s': %s", title, result.exception)

    logger.info(
        "Push '%s': %d enviadas, %d fallidas",
        title,
        response.success_count,
        response.failure_count,
    )

    if dead_tokens:
        try:
            async with get_db_context() as conn:
                await profile_repo.clear_fcm_tokens(conn, dead_tokens)
            logger.info("Se eliminaron %d tokens FCM dados de baja", len(dead_tokens))
        except Exception:
            logger.exception("No se pudieron limpiar tokens FCM inválidos")


async def notify_users(
    conn: asyncpg.Connection,
    background_tasks: BackgroundTasks,
    user_ids: list[str],
    title: str,
    body: str,
) -> None:
    """Busca los tokens FCM de los usuarios y encola el envío en segundo plano.

    Los usuarios sin token se ignoran silenciosamente.
    """
    if not user_ids:
        return
    tokens = await profile_repo.get_fcm_tokens(conn, user_ids)
    if tokens:
        background_tasks.add_task(deliver_push, tokens, title, body)
