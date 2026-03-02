import os
import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi import HTTPException

app = FastAPI(title="Rides API - Health Check")

# Obtenemos las URLs de las variables de entorno definidas en docker-compose.yml
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

# 1. Creamos el modelo de datos esperado desde Flutter
class ProfileUpdate(BaseModel):
    biography: str | None = None
    preferences: str | None = None
    vehicles: str | None = None


class TripCreate(BaseModel):
    origin_name: str
    origin_lat: float
    origin_lng: float
    dest_name: str
    dest_lat: float
    dest_lng: float
    distance_text: str
    duration_text: str

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

@app.get("/drivers")
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

@app.get("/profile/{user_id}")
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

@app.put("/profile/{user_id}")
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

# Este será nuestro "Trigger"
@app.post("/users/{user_id}")
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
    
@app.post("/trips/{driver_id}")
async def create_trip(driver_id: str, trip: TripCreate):
    try:
        url_limpia = DATABASE_URL.replace("+asyncpg", "")
        conn = await asyncpg.connect(url_limpia)

        query = """
            INSERT INTO trips 
            (driver_id, origin_name,x origin_lat, origin_lng, dest_name, dest_lat, dest_lng, distance_text, duration_text) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id;
        """
        
        # Guardamos en Postgres
        new_trip = await conn.fetchrow(
            query, 
            driver_id, 
            trip.origin_name, trip.origin_lat, trip.origin_lng,
            trip.dest_name, trip.dest_lat, trip.dest_lng,
            trip.distance_text, trip.duration_text
        )
        
        await conn.close()
        return {"message": "Viaje publicado con éxito", "trip_id": new_trip["id"]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

class RequestStatusUpdate(BaseModel):
    status: str

# 1. Obtener el viaje activo del conductor
@app.get("/trips/active/{driver_id}")
async def get_active_trip(driver_id: str):
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    # Buscamos el último viaje del conductor (puedes ajustar la lógica después)
    query = "SELECT * FROM trips WHERE driver_id = $1 ORDER BY id DESC LIMIT 1;"
    trip = await conn.fetchrow(query, driver_id)
    await conn.close()
    return dict(trip) if trip else None

# 2. Obtener las solicitudes de ese viaje
@app.get("/trips/{trip_id}/requests")
async def get_trip_requests(trip_id: int):
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    query = "SELECT * FROM trip_requests WHERE trip_id = $1 AND status = 'pendiente';"
    requests = await conn.fetch(query, trip_id)
    await conn.close()
    return [dict(r) for r in requests]

# 3. Aceptar o rechazar una solicitud
@app.put("/requests/{request_id}/status")
async def update_request_status(request_id: int, update: RequestStatusUpdate):
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    query = "UPDATE trip_requests SET status = $1 WHERE id = $2;"
    await conn.execute(query, update.status, request_id)
    await conn.close()
    return {"message": "Estado actualizado"}