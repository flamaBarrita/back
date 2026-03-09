import os
import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime,timedelta
from typing import Optional
import jwt
from jwt import PyJWKClient
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import APIRouter, Depends

app = FastAPI(title="Rides API - Health Check")

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
    """Verifica que el token JWT haya sido firmado realmente por tu AWS Cognito"""
    token = credenciales.credentials
    try:
        # 1. Buscamos la llave pública correcta de Cognito para este token específico
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        
        # 2. Decodificamos y validamos matemáticamente el token
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"], # Cognito siempre usa RS256
            issuer=f"https://cognito-idp.{COGNITO_REGION}.amazonaws.com/{COGNITO_USER_POOL_ID}",
            options={"verify_aud": False}
        )
        
        # En Cognito, el ID único e inmutable del usuario viene en la variable 'sub'
        cognito_user_id = payload.get("sub")
        if not cognito_user_id:
            raise HTTPException(status_code=401, detail="Token inválido: sin identificador")
            
        return cognito_user_id

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Tu sesión expiró. Vuelve a iniciar sesión en la rutas_protegidas.")
    except Exception as e:
        print(f"🔥 Intento de acceso denegado (Token inválido): {e}")
        raise HTTPException(status_code=401, detail="Acceso no autorizado.")
    

rutas_protegidas = APIRouter(dependencies=[Depends(obtener_usuario_actual)])
####




# 1. Creamos el modelo de datos esperado desde Flutter
class ProfileUpdate(BaseModel):
    biography: str | None = None
    preferences: str | None = None
    vehicles: str | None = None


class TripCreate(BaseModel):
    origin_name: str
    dest_name: str
    duration_text: str
    departure_time: datetime # o str, dependiendo de cómo lo mande Flutter
    price: float
    seats_available: int
    origin_lat: float
    origin_lng: float
    dest_lat: float
    dest_lng: float
@app.get("/")
async def health_check():
    """
    Ruta de diagnóstico para verificar conectividad con DB y Redis.
    """
    status = {
        "service": "Rides API Backend",
        "environment": "Local Development",
        "checks": {}
    }

    # 1. PRUEBA DE BASE DE DATOS (PostgreSQL/PostGIS)

    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")

        # Conexión directa con asyncpg para probar la red
        conn = await asyncpg.connect(url_limpia)
        # Consultamos la versión y si PostGIS está instalado
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

    # 2. PRUEBA DE CACHÉ (Redis)
    try:
        # Conexión a Redis
        r = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
        # Escribimos y leemos un valor de prueba
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

@rutas_protegidas.get("/drivers")
async def get_drivers():

    """
    Consulta la tabla 'drivers' que creamos manualmente.
    """
    results = []
    try:
        # 1. Limpiamos la URL igual que antes
        url_limpia = DATABASE_URL.replace("+asyncpg", "")

        # 2. Conectamos
        conn = await asyncpg.connect(url_limpia)

        # 3. Ejecutamos la consulta SQL directa
        # fetch: trae todas las filas
        rows = await conn.fetch("SELECT id, name, status FROM drivers")

        # 4. Convertimos los resultados (que son objetos Record) a Diccionarios
        for row in rows:
            results.append(dict(row))

        await conn.close()
        return results
    
    except Exception as e:
        return {"error": str(e)}
    

@rutas_protegidas.get("/profile/{user_id}")
async def get_profile(user_id: str):
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)

        query = "SELECT name, biography, preferences, vehicles FROM drivers WHERE id = $1"
        row = await conn.fetchrow(query, user_id)

        await conn.close()

        # Si el usuario existe, devolvemos sus datos. Si no, devolvemos campos vacíos.
        if row:
            return dict(row)
        else:
            return {"biography": "", "preferences": "", "vehicles": ""}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@rutas_protegidas.put("/profile/{user_id}")
async def update_profile(user_id: str, profile: ProfileUpdate): # user_id AHORA ES str
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)

        # LÓGICA UPSERT: Inserta o Actualiza en la misma consulta
        query = """
            INSERT INTO drivers (id, name, biography, preferences, vehicles)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE
            SET biography = EXCLUDED.biography,
                preferences = EXCLUDED.preferences,
                vehicles = EXCLUDED.vehicles
            RETURNING id;
        """

        # Como no sabemos el nombre al crear el perfil, pasamos "Usuario Cognito" por defecto
        # (Idealmente luego sacarás el nombre real desde Flutter o Cognito)
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
        # AQUÍ ESTÁ LA MAGIA: Esto sí lanza un error 500 real hacia Flutter
        raise HTTPException(status_code=500, detail=str(e))


class UserCreate(BaseModel):
    name: str

class TripRequestCreate(BaseModel):
    passenger_id: str
    passenger_name: str
    passenger_photo: Optional[str] = "https://i.pravatar.cc/150"
    passenger_rating: Optional[str] = "5.0"
    seats_requested: int
    sender_id: str

# Este será nuestro "Trigger"
@rutas_protegidas.post("/users/{user_id}")
async def create_initial_user(user_id: str, user: UserCreate):
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)

        # Insertamos el usuario con su ID de Cognito y su nombre real.
        # Si por alguna razón ya existe (ON CONFLICT), no hacemos nada (DO NOTHING).
        query = """
            INSERT INTO drivers (id, name)
            VALUES ($1, $2)
            ON CONFLICT (id) DO NOTHING;
        """
        await conn.execute(query, user_id, user.name)
        await conn.close()

        return {"message": "Usuario registrado en PostgreSQL exitosamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@rutas_protegidas.post("/trips/{driver_id}")
async def create_trip( trip: TripCreate, driver_id: str = Depends(obtener_usuario_actual)): # trip es un objeto Pydantic
    conn = None
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)

        # 1. Validación de viaje activo
        check_query = "SELECT id FROM trips WHERE driver_id = $1 AND status = 'activo';"
        viaje_activo = await conn.fetchrow(check_query, driver_id)

        if viaje_activo:
            # Lanzamos el error directo. El bloque 'finally' se encargará de cerrar la conexión
            raise HTTPException(
                status_code=400,
                detail="Ya tienes un viaje activo. Debes finalizarlo antes de publicar uno nuevo."
            )

        # 2. Query de inserción espacial
        insert_query = """
            INSERT INTO trips (
                driver_id, origin_name, dest_name, duration_text,
                departure_time, price, seats_available, status,
                origin_geom, dest_geom
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, 'activo',
                ST_SetSRID(ST_MakePoint($8, $9), 4326),  -- Longitud, Latitud
                ST_SetSRID(ST_MakePoint($10, $11), 4326) -- Longitud, Latitud
            ) RETURNING id;
        """

        # 3. Ejecución usando la notación de punto del modelo Pydantic (trip.campo)
        nuevo_id = await conn.fetchval(
            insert_query,
            driver_id,
            trip.origin_name,
            trip.dest_name,
            trip.duration_text,
            trip.departure_time,
            trip.price,
            trip.seats_available,
            trip.origin_lng, trip.origin_lat,  # Origen: Longitud primero
            trip.dest_lng, trip.dest_lat       # Destino: Longitud primero
        )

        return {"message": "Viaje publicado con éxito", "trip_id": nuevo_id}

    except HTTPException:
        # Re-lanzamos la excepción HTTP para que FastAPI la envíe correctamente al frontend
        raise
    except Exception as e:
        print(f"Error detectado: {e}")
        raise HTTPException(status_code=500, detail=f"Error en base de datos: {str(e)}")

    finally:
        # Nos aseguramos de cerrar la conexión de forma segura si sigue abierta
        if conn and not conn.is_closed():
            await conn.close()

class RequestStatusUpdate(BaseModel):
    status: str

# 1. Obtener el viaje activo del conductor
@rutas_protegidas.get("/trips/active/{driver_id}")
async def get_active_trip(driver_id: str):
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    # Buscamos el último viaje del conductor (puedes ajustar la lógica después)
    query = "SELECT * FROM trips WHERE driver_id = $1 and status = 'activo' ORDER BY id DESC LIMIT 1;"
    trip = await conn.fetchrow(query, driver_id)
    await conn.close()
    return dict(trip) if trip else None

# 2. Obtener las solicitudes de ese viaje
@rutas_protegidas.get("/trips/{trip_id}/requests")
async def get_trip_requests(trip_id: int):
    conn = None
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)

        # Traemos todas las solicitudes pendientes para este viaje
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
        print(f"Error obteniendo peticiones: {e}")
        raise HTTPException(status_code=500, detail="Error al obtener peticiones")
    finally:
        if conn and not conn.is_closed():
            await conn.close()

# 3. Aceptar o rechazar una solicitud
@rutas_protegidas.put("/requests/{request_id}/status")
async def update_request_status(request_id: int, update: RequestStatusUpdate):
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    query = "UPDATE trip_requests SET status = $1 WHERE id = $2;"
    await conn.execute(query, update.status, request_id)
    await conn.close()
    return {"message": "Estado actualizado"}


@rutas_protegidas.post("/trips/{trip_id}/requests")
async def create_trip_request(trip_id: int, req: TripRequestCreate):
    conn = None
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)

        # 1. Validar que el viaje exista y tenga asientos suficientes
        trip_query = "SELECT seats_available, status FROM trips WHERE id = $1"
        trip = await conn.fetchrow(trip_query, trip_id)

        if not trip:
            raise HTTPException(status_code=404, detail="El viaje no existe")
        if trip['status'] != 'activo':
            raise HTTPException(status_code=400, detail="Este viaje ya no está activo")
        if trip['seats_available'] < req.seats_requested:
            raise HTTPException(status_code=400, detail="No hay suficientes asientos disponibles")

        # 2. Evitar solicitudes duplicadas del mismo pasajero
        check_dup = "SELECT id FROM trip_requests WHERE trip_id = $1 AND passenger_id = $2 AND status = 'pendiente'"
        duplicado = await conn.fetchrow(check_dup, trip_id, req.passenger_id)

        selfrequest = "select id from trip_requests where (select driver_id from trips where id = $1) <> $2;" #
        selftrip = await conn.fetchrow(selfrequest, trip_id, req.sender_id)

        if not selftrip:
            raise HTTPException(status_code=400, detail="No puedes solicitar tu propio viaje")

        if duplicado:
            raise HTTPException(status_code=400, detail="Ya enviaste una solicitud para este viaje")

        # 3. Insertar la solicitud
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

        return {"message": "Solicitud enviada al conductor AAAHHH", "request_id": request_id}

    except HTTPException:
        raise
    except Exception as e:
        print(f"🔥 ERROR AL SOLICITAR VIAJE: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn and not conn.is_closed():
            await conn.close()

@rutas_protegidas.get("/trips/search")
async def search_trips(olat: float, olng: float, dlat: float, dlng: float):
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))

    # ST_DWithin calcula la distancia en metros sobre la esfera terrestre
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

    # Cuidado: En PostGIS el orden es (Longitud, Latitud)
    resultados = await conn.fetch(query, olng, olat, dlng, dlat)
    await conn.close()

    return [dict(r) for r in resultados]

@rutas_protegidas.patch("/trips/{trip_id}/cancelar")
async def delete_trip(trip_id: int):
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    
    try:
       
        query = "UPDATE trips SET status = 'cancelado' WHERE id = $1;"
        quey_delete_requests = "UPDATE trip_requests SET status = 'cancelado' WHERE trip_id = $1;"
        
        resultado = await conn.execute(query, trip_id)
        
        # Verificamos si realmente se afectó alguna fila
        if resultado == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Viaje no encontrado o ya eliminado")
        
        await conn.execute(quey_delete_requests, trip_id)
            
        return {"message": "Viaje eliminado exitosamente", "exito": True}
        
    except Exception as e:
        print(f"Error al eliminar viaje: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
        
    finally:
        # SIEMPRE cerrar la conexión, incluso si el código falla
        await conn.close()

@rutas_protegidas.get("/mis-viajes/aprobados/{passenger_id}")
async def get_viajes_aprobados(passenger_id: str):
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    try:
        # Hacemos un JOIN entre trips y trip_requests. 
        # Adaptalo a los nombres exactos de tus columnas.
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
        
        # Convertimos los registros a una lista de diccionarios para mandar el JSON
        return [dict(viaje) for viaje in viajes]
    except Exception as e:
        print(f"Error al obtener viajes aprobados: {e}")
        raise HTTPException(status_code=500, detail="Error al consultar la base de datos")
    finally:
        await conn.close()


@rutas_protegidas.patch("/trips/{trip_id}/pasajeros/{passenger_id}/cancelar")
async def cancelar_asiento_pasajero(trip_id: int, passenger_id: str):
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    try:
        # 1. Actualizamos el estatus en la tabla de solicitudes a "cancelado por pasajero"
        # Usamos Soft Delete para mantener el registro de que alguna vez estuvo ahí
        query_update = """
            UPDATE trip_requests 
            SET status = 'cancelled_by_passen' 
            WHERE trip_id = $1 AND passenger_id = $2 AND status = 'aceptado';
        """
        resultado = await conn.execute(query_update, trip_id, passenger_id)
        
        if resultado == "UPDATE 0":
            raise HTTPException(status_code=400, detail="No se encontró una reserva activa para cancelar")

        # 2. (Opcional pero recomendado) Sumarle +1 a los asientos disponibles en la tabla trips
        # query_seats = "UPDATE trips SET seats_available = seats_available + 1 WHERE id = $1;"
        # await conn.execute(query_seats, trip_id)

        return {"message": "Asiento cancelado con éxito", "success": True}
        
    except Exception as e:
        print(f"Error cancelando asiento: {e}")
        raise HTTPException(status_code=500, detail="Error al cancelar")
    finally:
        await conn.close()

app.include_router(rutas_protegidas)


