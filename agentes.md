# agentes.md — back_blabla

## Descripción del proyecto
Backend de una aplicación de viajes compartidos para estudiantes universitarios.
API REST en FastAPI, autenticación con AWS Cognito, PostgreSQL + PostGIS para datos geoespaciales. Notificaciones push mediante Firebase Cloud Messaging (FCM).

## Tecnologías principales
- Python 3.12
- FastAPI + Uvicorn
- asyncpg (pool de conexiones a PostgreSQL)
- PyJWT + PyJWKClient (validación de access tokens de Cognito)
- firebase-admin
- polyline

Versiones fijadas en `backend/requirements.txt` (runtime) y `backend/requirements-dev.txt` (tests).

## Estructura de carpetas
- `backend/app/main.py`: crea la app, lifespan (Firebase, pool de BD, job de expiración), middleware de request-id y manejadores de errores.
- `backend/app/core/`: configuración, seguridad, conexiones y logging.
  - `config.py`: configuración con pydantic-settings (ver "Variables de entorno").
  - `database.py`: pool asyncpg, dependencia `get_db()` y `get_db_context()`.
  - `firebase.py`: inicialización de Firebase Admin (no crítica: si falla, la API arranca sin push).
  - `security.py`: `obtener_usuario_actual` (JWT de Cognito) y `ensure_same_user`.
  - `logging.py`: logger con request-id; formato texto o JSON.
  - `exceptions.py`: excepciones de dominio (`NotFoundError`, `ConflictError`, `ValidationError`, `UnauthorizedError`, `ForbiddenError`).
- `backend/app/schemas/`: modelos Pydantic de entrada, con límites de longitud y rango.
- `backend/app/routers/`: routers de FastAPI por dominio.
- `backend/app/services/`: lógica de negocio y reglas de autorización.
  - `notification_service.py`: `notify_users()` (único punto para mandar push).
  - `trip_service.py`: incluye `run_trip_expiration_loop()`.
- `backend/app/repositories/`: queries SQL.
- `backend/app/utils/`: `polyline.py` y `time.py` (hora local de `APP_TIMEZONE`).
- `backend/migrations/`: migraciones SQL para bases existentes.
- `esquema.sql`: esquema completo para una base nueva.

## Convenciones de código
- Tipado estricto con Pydantic y type hints.
- Nunca abrir conexiones a PostgreSQL manualmente en endpoints; usar `get_db()`.
- No usar `print`; usar `logger` desde `app.core.logging`.
- Las queries SQL viven en `repositories/`.
- Mantener rutas y respuestas JSON estables; no romper el frontend sin aviso.
- Las excepciones de negocio se lanzan como `AppError` y el manejador global las convierte en respuestas HTTP.

### Autorización (obligatorio)
- **La identidad siempre sale del JWT.** Todo endpoint que actúe en nombre del usuario recibe `current_user: str = Depends(obtener_usuario_actual)`.
- Si la ruta trae un ID de usuario (`/profile/{user_id}`, `/trips/{driver_id}`...), se compara con `ensure_same_user(path_id, current_user)` y se usa `current_user` en adelante.
- La propiedad de viajes y solicitudes se resuelve en la BD dentro del service (`trip_service.get_owned_trip`), nunca con datos del cliente. Si no coincide → `ForbiddenError` (403).
- Nunca leer `passenger_id`, `sender_id`, `driver_id` ni nombres del body.
- Cada endpoint nuevo necesita un test en `tests/integration/test_authorization.py`.

### Asientos y concurrencia
- `trips.seats_available` = asientos que **quedan**. Se descuenta al aceptar una solicitud (`trip_repo.reserve_seats`) y se devuelve si el pasajero cancela.
- Las operaciones que tocan asientos van en `conn.transaction()` y bloquean **primero el viaje** (`get_trip_by_id(for_update=True)`) y después la solicitud. Respetar ese orden evita deadlocks.
- El índice único `uq_trip_requests_active` impide solicitudes vigentes duplicadas.

### Fechas
- La app envía `departure_time` sin zona. Se guarda como hora local naive de `APP_TIMEZONE` (default `America/Mexico_City`).
- En SQL, comparar contra `NOW() AT TIME ZONE <tz>`; en Python, usar `app.utils.time.now_local()`.
- Un viaje deja de aparecer en búsquedas a su hora de salida. Sigue "activo" para el conductor durante `TRIP_ACTIVE_GRACE_MINUTES` y después un job lo pasa a `completado`.

## Ejecutar localmente
```bash
cd /projects/back_blabla
docker compose up --build
```

## Variables de entorno
Definidas en `.env` y consumidas por `docker-compose.yml`:
- `DB_USER`, `DB_PASS` (obligatorias)
- `COGNITO_REGION`, `COGNITO_USER_POOL_ID` (obligatorias)
- `COGNITO_APP_CLIENT_ID`: recomendado. Si se define, se rechazan tokens de otros app clients.
- `APP_TIMEZONE` (default `America/Mexico_City`)
- `TRIP_ACTIVE_GRACE_MINUTES` (default `120`)
- `LOG_FORMAT` (`json` en docker-compose, `text` por defecto), `LOG_LEVEL`
- `POSTGIS_IMAGE` (solo docker-compose; default `postgis/postgis:15-3.4`, en ARM usar `imresamu/postgis:15-3.4`)
- `FIREBASE_ENABLED`, `FIREBASE_CREDENTIALS_PATH` (default `app/firebase.json`; en Docker, `/run/secrets/firebase.json`, montado desde `FIREBASE_CREDENTIALS_FILE`, por defecto `./backend/app/firebase.json`)
- También existen `SEARCH_RADIUS_M` (500) y `TRIP_EXPIRATION_INTERVAL_SECONDS` (300).
- `DATABASE_URL` se construye en `docker-compose.yml`.

## Base de datos
- Base nueva: `esquema.sql`.
- Base existente: aplicar `backend/migrations/*.sql` en orden (ver `backend/migrations/README.md`).
- `updated_at` y `origin_lat/lng`/`dest_lat/lng` se mantienen por triggers; no setearlos a mano.

## Testing
```bash
cd /projects/back_blabla/backend
pip install -r requirements-dev.txt

# Solo unitarios (sin BD):
pytest

# Todo, incluyendo integración contra PostGIS real (¡la BD se vacía!):
docker run -d --rm --name rides_test_db -p 55432:5432 \
  -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=rides_test \
  postgis/postgis:15-3.4
TEST_DATABASE_URL=postgresql://test:test@localhost:55432/rides_test pytest
```

- `tests/api/`: health check y validación de JWT (sin red ni BD).
- `tests/integration/`: autorización (IDOR), asientos y concurrencia, expiración, búsqueda y perfiles. En estos tests la autenticación se reemplaza por el header `X-Test-User`.

## Licencia
Internal project.
