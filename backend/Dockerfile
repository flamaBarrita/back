# Usamos 'slim' porque es ligera pero contiene lo necesario para Debian
FROM python:3.11-slim-bookworm

# 1. Optimizaciones de Python para Contenedores
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 2. Instalación de dependencias de C para Geoespacial (PostGIS)
# Esto es CRUCIAL. Sin estas librerías, SQLAlchemy/GeoAlchemy fallarán en Linux.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    binutils \
    libproj-dev \
    gdal-bin \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 3. Instalación de paquetes de Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 4. Copiamos el código
COPY . .

EXPOSE 8000
