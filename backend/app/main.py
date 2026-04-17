import os
import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime,timedelta
from typing import Optional
import jwt
from jwt import PyJWKClient
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import APIRouter, Depends
import polyline 
import firebase_admin
from firebase_admin import credentials, messaging

current_dir = os.path.dirname(os.path.abspath(__file__))

firebase_path = os.path.join(current_dir, "firebase.json")

# Pasa la ruta absoluta a Firebase
cred = credentials.Certificate(firebase_path)

firebase_admin.initialize_app(cred)

app = FastAPI(
    docs_url="/mario_docs-production_secure",
    redoc_url=None,
    title="Rides API - Health Check")

# Obtenemos las URLs de las variables de entorno definidas en docker-compose.yml
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")
COGNITO_REGION = os.getenv("COGNITO_REGION")
COGNITO_USER_POOL_ID = os.getenv("COGNITO_USER_POOL_ID")

##seguridad de endpoints

# Construimos la URL dinámica
JWKS_URL = f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/{COGNITO_USER_POOL_ID}/.well-known/jwks.json"

jwks_client = PyJWKClient(JWKS_URL)
security = HTTPBearer()


async def obtener_usuario_actual(credenciales: HTTPAuthorizationCredentials = Depends(security)):
    """
    Valida que el token JWT sea legítimo y firmado por AWS Cognito.
    
    Extrae la clave pública de Cognito, decodifica el token y verifica su validez.
    Si el token es válido, retorna el ID único del usuario (sub claim).
    Si falla, lanza excepciones HTTP con estado 401.
    """
    token = credenciales.credentials
    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            issuer=f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/{COGNITO_USER_POOL_ID}",
            options={"verify_aud": False}
        )
        
        cognito_user_id = payload.get("sub")
        if not cognito_user_id:
            raise HTTPException(status_code=401, detail="Token inválido: sin identificador")
            
        return cognito_user_id

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Tu sesión expiró. Vuelve a iniciar sesión.")
    except Exception as e:
        print(f"Intento de acceso denegado (Token inválido): {e}")
        raise HTTPException(status_code=401, detail="Acceso no autorizado.")
    
# Creamos un alias para evitar sobreescribir 
rutas_protegidas = APIRouter(dependencies=[Depends(obtener_usuario_actual)])
####

def enviar_notificacion_push(fcm_token: str, titulo: str, cuerpo: str):
    """
    Envía una notificación push a un dispositivo usando Firebase Cloud Messaging.
    
    Construye un mensaje con título y cuerpo, y lo envía al token FCM del usuario.
    Retorna True si se envía correctamente, False si hay error.
    """
    try:
        mensaje = messaging.Message(
            notification=messaging.Notification(
                title=titulo,
                body=cuerpo,
            ),
            token=fcm_token,
        )
        respuesta = messaging.send(mensaje)
        print(f"Notificación enviada exitosamente: {respuesta}")
        return True
    except Exception as e:
        print(f"Error al enviar notificación FCM: {e}")
        return False


# Creamos el modelo de datos esperado desde Flutter
class ProfileUpdate(BaseModel):
    biography: str | None = None
    preferences: str | None = None
    vehicles: str | None = None


class TripCreate(BaseModel):
    origin_name: str
    dest_name: str
    duration_text: str
    departure_time: datetime 
    price: float
    seats_available: int
    origin_lat: float
    origin_lng: float
    dest_lat: float
    dest_lng: float
    encoded_polyline: str
@app.get("/")
async def health_check():
    """
    Verifica el estado de conexión del servidor con PostgreSQL y Redis.
    
    Se conecta a la base de datos para consultar versión y soporte de PostGIS,
    luego se conecta a Redis para verificar disponibilidad.
    Retorna un diccionario con el estado de cada servicio.
    """
    status = {
        "service": "Rides API Backend",
        "environment": "Local Development",
        "checks": {}
    }

    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)
        version = await conn.fetchval('SELECT version()')
        postgis_version = await conn.fetchval('SELECT PostGIS_version()')
        await conn.close()

        status["checks"]["database"] = {
            "status": "OK",
            "version": version,
            "postgis": postgis_version
        }
    except Exception as e:
        status["checks"]["database"] = {
            "status": "ERROR",
            "detail": str(e)
        }

    try:
        r = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
        await r.set("health_check", "Redis is alive!")
        val = await r.get("health_check")
        await r.close()

        status["checks"]["redis"] = {
            "status": "OK",
            "read_test": val
        }
    except Exception as e:
        status["checks"]["redis"] = {
            "status": "ERROR",
            "detail": str(e)
        }

    return status

@rutas_protegidas.get("/profile/{user_id}")
async def get_profile(user_id: str):
    """
    Obtiene el perfil de un usuario, incluyendo biografía, preferencias y vehículos.
    
    Si el usuario existe, retorna sus datos. Si no existe, retorna campos vacíos.
    """
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)
        query = "SELECT name, biography, preferences, vehicles FROM drivers WHERE id = $1"
        row = await conn.fetchrow(query, user_id)
        await conn.close()

        if row:
            return dict(row)
        else:
            return {"biography": "", "preferences": "", "vehicles": ""}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@rutas_protegidas.put("/profile/{user_id}")
async def update_profile(user_id: str, profile: ProfileUpdate):
    """
    Actualiza o crea el perfil de un usuario con sus datos personales.
    
    Usa un UPSERT de SQL: si el usuario existe, actualiza sus datos;
    si no existe, lo crea. Guarda biografía, preferencias y vehículos.
    """
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)

        query = """
            INSERT INTO drivers (id, name, biography, preferences, vehicles)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE
            SET biography = EXCLUDED.biography,
                preferences = EXCLUDED.preferences,
                vehicles = EXCLUDED.vehicles
            RETURNING id;
        """

        updated_row = await conn.fetchrow(
            query,
            user_id,
            "Usuario Cognito",
            profile.biography,
            profile.preferences,
            profile.vehicles
        )

        await conn.close()
        return {"message": "Perfil guardado correctamente", "id": updated_row["id"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class UserCreate(BaseModel):
    name: str

class TripRequestCreate(BaseModel):
    passenger_id: str
    passenger_name: str
    passenger_photo: Optional[str] = "https://i.pravatar.cc/150?img=31"
    passenger_rating: Optional[str] = "5.0"
    seats_requested: int
    sender_id: str

@rutas_protegidas.post("/users/{user_id}")
async def create_initial_user(user_id: str, user: UserCreate):
    """
    Registra un nuevo usuario en la base de datos cuando se crea su cuenta en Cognito.
    
    Si el usuario ya existe, no hace nada (evita duplicados).
    Guarda el ID de Cognito y el nombre del usuario.
    """
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)
        query = """
            INSERT INTO drivers (id, name)
            VALUES ($1, $2)
            ON CONFLICT (id) DO NOTHING;
        """
        await conn.execute(query, user_id, user.name)
        await conn.close()
        return {"message": "Usuario registrado en PostgreSQL correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@rutas_protegidas.post("/trips/{driver_id}")
async def create_trip(trip: TripCreate, driver_id: str = Depends(obtener_usuario_actual)):
    """
    Crea un nuevo viaje publicado por un conductor.
    
    Valida que el conductor no tenga un viaje activo ya.
    Decodifica la ruta (polyline) a coordenadas y las guarda como geometría en PostGIS.
    Almacena ubicación de origen, destino y la ruta completa con soporte geoespacial.
    """
    conn = None
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)

        check_query = "SELECT id FROM trips WHERE driver_id = $1 AND status = 'activo';"
        viaje_activo = await conn.fetchrow(check_query, driver_id)

        if viaje_activo:
            raise HTTPException(
                status_code=400,
                detail="Ya tienes un viaje activo. Debes finalizarlo antes de publicar uno nuevo."
            )

        try:
            coords = polyline.decode(trip.encoded_polyline)
            linestring_coords = ", ".join([f"{lon} {lat}" for lat, lon in coords])
            route_wkt = f"LINESTRING({linestring_coords})"
        except Exception as e:
            raise HTTPException(status_code=400, detail="La polyline proporcionada no es válida.")

        insert_query = """
            INSERT INTO trips (
                driver_id, origin_name, dest_name, duration_text,
                departure_time, price, seats_available, status,
                origin_geom, dest_geom, route_geom
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, 'activo',
                ST_SetSRID(ST_MakePoint($8, $9), 4326),
                ST_SetSRID(ST_MakePoint($10, $11), 4326),
                ST_GeomFromText($12, 4326)
            ) RETURNING id;
        """

        nuevo_id = await conn.fetchval(
            insert_query,
            driver_id,
            trip.origin_name,
            trip.dest_name,
            trip.duration_text,
            trip.departure_time,
            trip.price,
            trip.seats_available,
            trip.origin_lng, trip.origin_lat,
            trip.dest_lng, trip.dest_lat,
            route_wkt
        )

        return {"message": "Viaje publicado correctamente", "trip_id": nuevo_id}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error al crear viaje: {e}")
        raise HTTPException(status_code=500, detail=f"Error en base de datos: {str(e)}")
    finally:
        if conn and not conn.is_closed():
            await conn.close()

class RequestStatusUpdate(BaseModel):
    status: str

@rutas_protegidas.get("/trips/active/{driver_id}")
async def get_active_trip(driver_id: str):
    """
    Obtiene el viaje activo actual del conductor.
    
    Retorna todos los detalles del viaje (origen, destino, precio, asientos, etc.)
    o None si no hay viaje activo.
    """
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    query = "SELECT * FROM trips WHERE driver_id = $1 and status = 'activo' ORDER BY id DESC LIMIT 1;"
    trip = await conn.fetchrow(query, driver_id)
    if not trip:
        await conn.close()
        return None
    trip_dict = dict(trip)
    print(trip_dict, driver_id)
    # Buscamos los pasajeros vinculados a este viaje 
    query_passengers = """
       SELECT d.id, d.name 
        FROM drivers d
        JOIN trip_requests r 
        ON d.id = r.passenger_id
        WHERE r.trip_id = $1
        AND r.status = 'aceptado';
    """
    passengers = await conn.fetch(query_passengers, trip_dict['id'])
    
    # Convertimos los registros en una lista de diccionarios y los anidamos
    trip_dict['passengers'] = [dict(p) for p in passengers]
    
    await conn.close()
    
    return trip_dict

@rutas_protegidas.get("/trips/{trip_id}/requests")
async def get_trip_requests(trip_id: int):
    """
    Obtiene todas las solicitudes de pasajeros pendientes para un viaje específico.
    
    Retorna información del pasajero (nombre, foto, rating) y cantidad de asientos solicitados.
    Ordena por ID para mostrar las solicitudes en orden de llegada.
    """
    conn = None
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)

        query = """
            SELECT id, trip_id, passenger_id, passenger_name, passenger_photo,
                   passenger_rating, seats_requested, status
            FROM trip_requests
            WHERE trip_id = $1 AND status = 'pendiente'
            ORDER BY id ASC;
        """

        resultados = await conn.fetch(query, trip_id)
        return [dict(r) for r in resultados]
    except Exception as e:
        print(f"Error obteniendo solicitudes: {e}")
        raise HTTPException(status_code=500, detail="Error al obtener solicitudes")
    finally:
        if conn and not conn.is_closed():
            await conn.close()

@rutas_protegidas.put("/requests/{request_id}/status")
async def update_request_status(
    request_id: int,
    update: RequestStatusUpdate,
    background_tasks: BackgroundTasks
):
    """
    Actualiza el estado de una solicitud de viaje (aceptada o rechazada).
    
    Cuando se acepta o rechaza, envía automáticamente una notificación push
    al pasajero informándole de la decisión del conductor.
    La notificación se envía en segundo plano para no bloquear la respuesta.
    """
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    try:
        query_update = """
            UPDATE trip_requests
            SET status = $1
            WHERE id = $2
            RETURNING passenger_id;
        """
        passenger_id = await conn.fetchval(query_update, update.status, request_id)

        if not passenger_id:
            raise HTTPException(status_code=404, detail="Solicitud no encontrada")

        query_token = "SELECT fcm_token FROM drivers WHERE id = $1;"
        fcm_token = await conn.fetchval(query_token, passenger_id)

        if fcm_token:
            titulo = ""
            cuerpo = ""

            if update.status == "aceptado":
                titulo = "Lugar Asegurado"
                cuerpo = "El conductor ha aceptado tu solicitud. Prepárate para el viaje."
            elif update.status == "rechazado":
                titulo = "Viaje Lleno"
                cuerpo = "El conductor no pudo aceptar tu solicitud esta vez."

            if titulo:
                background_tasks.add_task(enviar_notificacion_push, fcm_token, titulo, cuerpo)

        return {"message": "Estado actualizado correctamente"}
    finally:
        await conn.close()


@rutas_protegidas.post("/trips/{trip_id}/requests")
async def create_trip_request(trip_id: int, req: TripRequestCreate, background_tasks: BackgroundTasks):
    """
    Crea una solicitud para que un pasajero se una a un viaje existente.
    
    Valida que:
    - El viaje exista y esté activo
    - Haya asientos disponibles
    - El pasajero no tenga otra solicitud pendiente para el mismo viaje
    - El pasajero no sea el conductor del viaje
    
    Si todo es válido, guarda la solicitud como pendiente de aprobación.
    Notifica al conductor que un pasajero solicitó unirse al viaje.
    """
    conn = None
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)

        trip_query = "SELECT seats_available, status, driver_id FROM trips WHERE id = $1"
        trip = await conn.fetchrow(trip_query, trip_id)

        if not trip:
            raise HTTPException(status_code=404, detail="El viaje no existe")
        if trip['status'] != 'activo':
            raise HTTPException(status_code=400, detail="Este viaje ya no está activo")
        if trip['seats_available'] < req.seats_requested:
            raise HTTPException(status_code=400, detail="No hay suficientes asientos disponibles")

        check_dup = "SELECT id FROM trip_requests WHERE trip_id = $1 AND passenger_id = $2 AND status = 'pendiente'"
        duplicado = await conn.fetchrow(check_dup, trip_id, req.passenger_id)

        selfrequest = "select id from trip_requests where (select driver_id from trips where id = $1) <> $2;"
        selftrip = await conn.fetchrow(selfrequest, trip_id, req.sender_id)

        if not selftrip:
            raise HTTPException(status_code=400, detail="No puedes solicitar tu propio viaje")

        if duplicado:
            raise HTTPException(status_code=400, detail="Ya enviaste una solicitud para este viaje")

        insert_query = """
            INSERT INTO trip_requests (
                trip_id, passenger_id, passenger_name, passenger_photo,
                passenger_rating, seats_requested, status
            ) VALUES ($1, $2, $3, $4, $5, $6, 'pendiente') RETURNING id;
        """

        request_id = await conn.fetchval(
            insert_query,
            trip_id, req.passenger_id, req.passenger_name,
            req.passenger_photo, req.passenger_rating, req.seats_requested
        )

        driver_id = trip['driver_id']
        query_token = "SELECT fcm_token FROM drivers WHERE id = $1;"
        fcm_token = await conn.fetchval(query_token, driver_id)

        if fcm_token:
            titulo = "Nueva Solicitud"
            cuerpo = f"{req.passenger_name} solicita unirse a tu viaje."
            background_tasks.add_task(enviar_notificacion_push, fcm_token, titulo, cuerpo)

        return {"message": "Solicitud enviada al conductor", "request_id": request_id}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error al crear solicitud de viaje: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn and not conn.is_closed():
            await conn.close()

@rutas_protegidas.get("/trips/search")
async def search_trips(olat: float, olng: float, dlat: float, dlng: float):
    """
    Busca viajes disponibles cercanos a las coordenadas de origen y destino.
    
    Utiliza geolocalización PostGIS para encontrar viajes cuyo origen esté
    dentro de 1km de la ubicación solicitada y cuyo destino también esté
    dentro de 1km de la ubicación destino. Retorna solo viajes activos
    con asientos disponibles, ordenados por hora de salida.
    """
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))

    query = """
        SELECT
            t.id, t.origin_name, t.dest_name, t.departure_time, t.price, t.seats_available, t.distance_text, t.duration_text,
            u.id AS driver_id, u.name AS driver_name, u.biography, u.vehicles, u.preferences, u.vehicles
        FROM trips t
        JOIN drivers u ON t.driver_id = u.id
        WHERE t.status = 'activo'
          AND t.seats_available > 0
          AND ST_DWithin(t.origin_geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, 1000)
          AND ST_DWithin(t.dest_geom::geography, ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography, 1000)
        ORDER BY t.departure_time ASC;
    """

    resultados = await conn.fetch(query, olng, olat, dlng, dlat)
    await conn.close()
    return [dict(r) for r in resultados]

@rutas_protegidas.patch("/trips/{trip_id}/cancelar")
async def delete_trip(trip_id: int):
    """
    Cancela un viaje y todas sus solicitudes asociadas.
    
    Cambia el estado del viaje a 'cancelado' (soft delete).
    También marca todas las solicitudes del viaje como canceladas
    para mantener historial de transacciones.
    """
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    try:
        query = "UPDATE trips SET status = 'cancelado' WHERE id = $1;"
        query_delete_requests = "UPDATE trip_requests SET status = 'cancelado' WHERE trip_id = $1;"

        resultado = await conn.execute(query, trip_id)

        if resultado == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Viaje no encontrado o ya cancelado")

        await conn.execute(query_delete_requests, trip_id)
        return {"message": "Viaje cancelado correctamente", "exito": True}
    except Exception as e:
        print(f"Error al cancelar viaje: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    finally:
        await conn.close()

@rutas_protegidas.get("/mis-viajes/aprobados/{passenger_id}")
async def get_viajes_aprobados(passenger_id: str):
    """
    Obtiene todos los viajes aprobados para un pasajero específico.
    
    Busca las solicitudes aceptadas del pasajero y retorna información
    completa del viaje y del conductor (nombre, biografía, vehículos, preferencias).
    Ordena los viajes por hora de salida próxima.
    """
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    try:
        query = """
            SELECT
                t.*,
                tr.status as request_status,
                u.name as driver_name,
                u.biography as driver_biography,
                u.vehicles as driver_vehicles,
                u.preferences as driver_preferences
            FROM trips t
            INNER JOIN trip_requests tr ON t.id = tr.trip_id
            INNER JOIN drivers u ON t.driver_id = u.id
            WHERE tr.passenger_id = $1
            AND tr.status = 'aceptado'
            AND t.status != 'cancelled'
            ORDER BY t.departure_time ASC;
        """
        viajes = await conn.fetch(query, passenger_id)
        return [dict(viaje) for viaje in viajes]
    except Exception as e:
        print(f"Error al obtener viajes aprobados: {e}")
        raise HTTPException(status_code=500, detail="Error al consultar la base de datos")
    finally:
        await conn.close()


@rutas_protegidas.patch("/trips/{trip_id}/pasajeros/{passenger_id}/cancelar")
async def cancelar_asiento_pasajero(trip_id: int, passenger_id: str, background_tasks: BackgroundTasks):
    """
    Cancela la reserva de un pasajero en un viaje específico.
    
    Marca la solicitud como cancelada por el pasajero (soft delete).
    Mantiene el registro histórico sin eliminar la solicitud de la base de datos.
    Notifica al conductor que un pasajero canceló su reserva.
    """
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    try:
        query_driver = "SELECT driver_id FROM trips WHERE id = $1;"
        driver_id = await conn.fetchval(query_driver, trip_id)

        if not driver_id:
            raise HTTPException(status_code=404, detail="El viaje no existe")

        query_update = """
            UPDATE trip_requests
            SET status = 'cancelled_by_passen'
            WHERE trip_id = $1 AND passenger_id = $2 AND status = 'aceptado';
        """
        resultado = await conn.execute(query_update, trip_id, passenger_id)

        if resultado == "UPDATE 0":
            raise HTTPException(status_code=400, detail="No se encontró una reserva activa para cancelar")

        query_token = "SELECT fcm_token FROM drivers WHERE id = $1;"
        fcm_token = await conn.fetchval(query_token, driver_id)

        if fcm_token:
            titulo = "Pasajero Canceló"
            cuerpo = "Un pasajero ha cancelado su asiento en el viaje."
            background_tasks.add_task(enviar_notificacion_push, fcm_token, titulo, cuerpo)

        return {"message": "Asiento cancelado correctamente", "success": True}
    except Exception as e:
        print(f"Error cancelando asiento: {e}")
        raise HTTPException(status_code=500, detail="Error al cancelar")
    finally:
        await conn.close()

class FCMTokenUpdate(BaseModel):
    fcm_token: str

@rutas_protegidas.patch("/api/users/update-fcm-token")
async def update_fcm_token(
    data: FCMTokenUpdate,
    user_id: str = Depends(obtener_usuario_actual)
):
    """
    Actualiza el token FCM de un usuario para recibir notificaciones push.
    
    El token es proporcionado por Firebase en el dispositivo del usuario
    y se usa para enviarle notificaciones sobre cambios en sus viajes.
    """
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    try:
        query = "UPDATE drivers SET fcm_token = $1 WHERE id = $2;"
        await conn.execute(query, data.fcm_token, user_id)
        return {"success": True, "message": "Token de notificaciones actualizado"}
    except Exception as e:
        print(f"Error actualizando FCM token: {e}")
        raise HTTPException(status_code=500, detail="Error interno")
    finally:
        await conn.close()

app.include_router(rutas_protegidas)


