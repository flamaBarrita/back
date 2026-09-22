# Migraciones de base de datos

- **Base nueva:** ejecutar `esquema.sql` (raíz del repo). Ya incluye todas las
  migraciones y las registra en `schema_migrations`.
- **Base existente:** aplicar cada archivo pendiente en orden, dentro de una
  transacción. Cada migración aborta sola si ya fue aplicada.

```bash
docker compose exec -T db psql -U "$DB_USER" -d rides_db \
  -v ON_ERROR_STOP=1 --single-transaction < backend/migrations/001_mejora_vanna.sql
```

Aplicar la migración **antes** de desplegar el backend nuevo: a partir de
`001_mejora_vanna`, `trips.seats_available` representa los asientos que quedan
libres y el backend lo descuenta al aceptar solicitudes.
