# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Dependencias del sistema para psycopg2 y cryptography
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .

# Puerto (Render asigna PORT dinámico)
EXPOSE 8000

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT:-8000}/health')" || exit 1

# Arrancar con puerto dinámico
CMD uvicorn api.server:app --host 0.0.0.0 --port ${PORT:-8000}
