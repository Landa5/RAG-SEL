# syntax=docker/dockerfile:1

# ── Stage 1: Build (instalar deps pesadas) ──
FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Instalar PyTorch CPU-only PRIMERO (mucho más ligero que la versión GPU)
RUN pip install --no-cache-dir \
    torch --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Stage 2: Runtime (imagen mínima) ──
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar paquetes Python del builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copiar código
COPY . .

# Puerto (Railway asigna PORT dinámico)
EXPOSE 8000

# Variables de entorno para tenants multi-BD (configurar en Railway, NO en código)
# ENV APP_CROMOS_DATABASE_URL debe configurarse como variable de servicio

# Healthcheck (start-period alto porque PyTorch tarda en cargar)
HEALTHCHECK --interval=30s --timeout=15s --retries=5 --start-period=120s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT:-8000}/health')" || exit 1

# Arrancar
CMD uvicorn api.server:app --host 0.0.0.0 --port ${PORT:-8000}
