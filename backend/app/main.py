import os
import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi import HTTPException
from datetime import datetime

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
async def create_trip(driver_id: str, trip: TripCreate): # trip es un objeto Pydantic
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

@app.get("/trips/search")
async def search_trips(olat: float, olng: float, dlat: float, dlng: float):
    conn = await asyncpg.connect(DATABASE_URL.replace("+asyncpg", ""))
    
    # ST_DWithin calcula la distancia en metros sobre la esfera terrestre
    query = """
        SELECT 
            t.id, t.origin_name, t.dest_name, t.departure_time, t.price, t.seats_available,
            u.name AS driver_name, u.photo_url AS driver_photo, u.rating, u.vehicles AS car
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