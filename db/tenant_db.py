"""
db/tenant_db.py — Capa de datos para la plataforma multi-tenant (schema: platform)
Separado de rag_engine (modelo routing, predicción, lógica interna).
"""
import os
import hashlib
import secrets
import uuid
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
SCHEMA = "platform"

# ─────────────────────────────────────────────
# Scopes válidos
# ─────────────────────────────────────────────

VALID_SCOPES = {
    # Operativos
    "query:run",
    "rag:query",
    "documents:upload",
    "documents:list",
    "documents:delete",
    "analytics:query",
    "predictions:run",
    "executions:read",
    # Admin finos
    "admin:tenants",
    "admin:apps",
    "admin:credentials",
    "admin:providers",
    "admin:usage",
    "admin:reviews",
    # Super-admin
    "superadmin:*",
}


# ─────────────────────────────────────────────
# Conexión
# ─────────────────────────────────────────────

def _clean_url(url: str) -> str:
    from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params.pop("pgbouncer", None)
    params.pop("sslmode", None)
    clean_query = urlencode({k: v[0] for k, v in params.items()})
    return urlunparse(parsed._replace(query=clean_query))


def _get_conn():
    clean_url = _clean_url(DATABASE_URL)
    conn = psycopg2.connect(
        clean_url,
        cursor_factory=psycopg2.extras.RealDictCursor,
        sslmode="require"
    )
    conn.set_session(autocommit=True)
    return conn


def _execute(sql: str, params=None, fetch=True):
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetch:
                return [dict(r) for r in cur.fetchall()]


# ─────────────────────────────────────────────
# Creación de tablas
# ─────────────────────────────────────────────

def create_platform_tables():
    """Crea todas las tablas del schema platform."""
    _execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}", fetch=False)

    _execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.tenants (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            slug TEXT NOT NULL UNIQUE,
            connection_ref TEXT,
            qdrant_collection TEXT,
            timezone TEXT DEFAULT 'UTC',
            max_documents INT DEFAULT 500,
            max_document_size_mb INT DEFAULT 50,
            max_queries_per_minute INT DEFAULT 20,
            max_queries_per_day INT DEFAULT 1000,
            max_monthly_cost_usd NUMERIC(10,2),
            allowed_mime_types TEXT[] DEFAULT '{{application/pdf}}',
            active BOOLEAN DEFAULT TRUE,
            config JSONB DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now()
        )
    """, fetch=False)

    _execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.connected_apps (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            tenant_id UUID NOT NULL REFERENCES {SCHEMA}.tenants(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            max_queries_per_minute INT,
            max_queries_per_day INT,
            max_monthly_cost_usd NUMERIC(10,2),
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT now()
        )
    """, fetch=False)

    _execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.api_credentials (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            app_id UUID NOT NULL REFERENCES {SCHEMA}.connected_apps(id) ON DELETE CASCADE,
            api_key_hash TEXT NOT NULL,
            api_key_prefix TEXT NOT NULL,
            active BOOLEAN DEFAULT TRUE,
            expires_at TIMESTAMPTZ,
            last_used_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT now()
        )
    """, fetch=False)

    _execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.app_scopes (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            app_id UUID NOT NULL REFERENCES {SCHEMA}.connected_apps(id) ON DELETE CASCADE,
            scope TEXT NOT NULL,
            UNIQUE(app_id, scope)
        )
    """, fetch=False)

    _execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.tenant_documents (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            tenant_id UUID NOT NULL REFERENCES {SCHEMA}.tenants(id) ON DELETE CASCADE,
            app_id UUID NOT NULL REFERENCES {SCHEMA}.connected_apps(id),
            filename TEXT NOT NULL,
            original_name TEXT NOT NULL,
            file_size_bytes INT NOT NULL,
            mime_type TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            chunk_count INT DEFAULT 0,
            file_hash TEXT NOT NULL,
            metadata JSONB DEFAULT '{{}}'::jsonb,
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT now(),
            indexed_at TIMESTAMPTZ,
            UNIQUE(tenant_id, file_hash)
        )
    """, fetch=False)

    _execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.execution_logs (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            tenant_id UUID NOT NULL,
            app_id UUID NOT NULL,
            query_preview TEXT,
            pipeline_selected TEXT,
            pipeline_executed TEXT,
            structured_result_type TEXT,
            sql_executed BOOLEAN DEFAULT FALSE,
            retrieval_executed BOOLEAN DEFAULT FALSE,
            forecast_engine_executed BOOLEAN DEFAULT FALSE,
            llm_only_response BOOLEAN DEFAULT FALSE,
            degraded_from TEXT,
            degraded_to TEXT,
            model_used TEXT,
            tokens_in INT DEFAULT 0,
            tokens_out INT DEFAULT 0,
            cost_usd NUMERIC(10,6) DEFAULT 0,
            latency_ms INT DEFAULT 0,
            tool_executions JSONB DEFAULT '[]',
            error TEXT,
            created_at TIMESTAMPTZ DEFAULT now()
        )
    """, fetch=False)

    # Índices
    for idx_sql in [
        f"CREATE INDEX IF NOT EXISTS idx_platform_exec_tenant ON {SCHEMA}.execution_logs(tenant_id)",
        f"CREATE INDEX IF NOT EXISTS idx_platform_exec_app ON {SCHEMA}.execution_logs(app_id)",
        f"CREATE INDEX IF NOT EXISTS idx_platform_docs_tenant ON {SCHEMA}.tenant_documents(tenant_id)",
        f"CREATE INDEX IF NOT EXISTS idx_platform_docs_hash ON {SCHEMA}.tenant_documents(tenant_id, file_hash)",
        f"CREATE INDEX IF NOT EXISTS idx_platform_creds_app ON {SCHEMA}.api_credentials(app_id)",
    ]:
        try:
            _execute(idx_sql, fetch=False)
        except Exception:
            pass

    print("Platform tables created/verified")


# ─────────────────────────────────────────────
# Connection Ref
# ─────────────────────────────────────────────

def resolve_database_url(connection_ref: str) -> str:
    """
    Resuelve la URL real de BD desde la referencia segura.
    Formatos soportados:
      - env:VARIABLE_NAME  → lee os.environ[VARIABLE_NAME]
      - vault:SECRET_NAME  → (futuro) lee desde secret manager
    """
    if not connection_ref:
        raise ValueError("connection_ref vacío")

    if connection_ref.startswith("env:"):
        var_name = connection_ref[4:]
        url = os.getenv(var_name)
        if not url:
            raise ValueError(f"Variable de entorno '{var_name}' no encontrada")
        return url

    if connection_ref.startswith("vault:"):
        raise NotImplementedError("Vault secret manager no implementado aún")

    raise ValueError(f"connection_ref con formato desconocido: {connection_ref}")


# ─────────────────────────────────────────────
# CRUD — Tenants
# ─────────────────────────────────────────────

def create_tenant(name: str, slug: str, connection_ref: str = None,
                  max_documents: int = 500, max_queries_per_day: int = 1000,
                  config: dict = None) -> dict:
    """Crea un nuevo tenant."""
    rows = _execute(f"""
        INSERT INTO {SCHEMA}.tenants (name, slug, connection_ref, max_documents,
                                      max_queries_per_day, config)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING *
    """, (name, slug, connection_ref, max_documents, max_queries_per_day,
          psycopg2.extras.Json(config or {})))
    return rows[0] if rows else None


def get_tenant(tenant_id: str) -> Optional[dict]:
    rows = _execute(f"SELECT * FROM {SCHEMA}.tenants WHERE id = %s", (tenant_id,))
    return rows[0] if rows else None


def get_tenant_by_slug(slug: str) -> Optional[dict]:
    rows = _execute(f"SELECT * FROM {SCHEMA}.tenants WHERE slug = %s AND active = TRUE", (slug,))
    return rows[0] if rows else None


def list_tenants(active_only: bool = True) -> list[dict]:
    where = "WHERE active = TRUE" if active_only else ""
    return _execute(f"SELECT * FROM {SCHEMA}.tenants {where} ORDER BY name")


def update_tenant(
    tenant_id: str,
    name: str = None,
    connection_ref: str = None,
    timezone: str = None,
    max_documents: int = None,
    max_document_size_mb: int = None,
    max_queries_per_minute: int = None,
    max_queries_per_day: int = None,
    max_monthly_cost_usd: float = None,
    active: bool = None,
) -> Optional[dict]:
    """Actualiza campos de un tenant. Solo actualiza los que no son None."""
    sets = ["updated_at = now()"]
    params = []

    if name is not None:
        sets.append("name = %s"); params.append(name)
    if connection_ref is not None:
        sets.append("connection_ref = %s"); params.append(connection_ref)
    if timezone is not None:
        sets.append("timezone = %s"); params.append(timezone)
    if max_documents is not None:
        sets.append("max_documents = %s"); params.append(max_documents)
    if max_document_size_mb is not None:
        sets.append("max_document_size_mb = %s"); params.append(max_document_size_mb)
    if max_queries_per_minute is not None:
        sets.append("max_queries_per_minute = %s"); params.append(max_queries_per_minute)
    if max_queries_per_day is not None:
        sets.append("max_queries_per_day = %s"); params.append(max_queries_per_day)
    if max_monthly_cost_usd is not None:
        sets.append("max_monthly_cost_usd = %s"); params.append(max_monthly_cost_usd)
    if active is not None:
        sets.append("active = %s"); params.append(active)

    params.append(tenant_id)
    rows = _execute(f"""
        UPDATE {SCHEMA}.tenants SET {", ".join(sets)}
        WHERE id = %s RETURNING *
    """, params)
    return rows[0] if rows else None


def get_usage_stats(tenant_id: str) -> dict:
    """Estadísticas de consumo de un tenant."""
    rows = _execute(f"""
        SELECT
            COUNT(*) as total_queries,
            COUNT(*) FILTER (WHERE created_at >= date_trunc('day', now() AT TIME ZONE 'UTC')) as queries_today,
            COUNT(*) FILTER (WHERE created_at >= date_trunc('month', now())) as queries_this_month,
            COALESCE(SUM(cost_usd), 0) as total_cost_usd,
            COALESCE(SUM(cost_usd) FILTER (WHERE created_at >= date_trunc('month', now())), 0) as cost_this_month,
            COALESCE(SUM(tokens_in + tokens_out), 0) as total_tokens
        FROM {SCHEMA}.execution_logs WHERE tenant_id = %s
    """, (tenant_id,))
    stats = rows[0] if rows else {}

    # Documentos
    docs = _execute(f"""
        SELECT COUNT(*) as total_docs,
               COALESCE(SUM(file_size_bytes), 0) as total_size_bytes
        FROM {SCHEMA}.tenant_documents WHERE tenant_id = %s
    """, (tenant_id,))
    if docs:
        stats.update(docs[0])

    # Apps activas
    apps = _execute(f"""
        SELECT COUNT(*) as active_apps
        FROM {SCHEMA}.connected_apps WHERE tenant_id = %s AND active = TRUE
    """, (tenant_id,))
    if apps:
        stats["active_apps"] = apps[0]["active_apps"]

    return stats


# ─────────────────────────────────────────────
# CRUD — Connected Apps
# ─────────────────────────────────────────────

def create_app(tenant_id: str, name: str, description: str = "") -> dict:
    """Crea una app conectada para el tenant."""
    rows = _execute(f"""
        INSERT INTO {SCHEMA}.connected_apps (tenant_id, name, description)
        VALUES (%s, %s, %s) RETURNING *
    """, (tenant_id, name, description))
    return rows[0] if rows else None


def get_app(app_id: str) -> Optional[dict]:
    rows = _execute(f"""
        SELECT a.*, t.slug as tenant_slug, t.active as tenant_active
        FROM {SCHEMA}.connected_apps a
        JOIN {SCHEMA}.tenants t ON t.id = a.tenant_id
        WHERE a.id = %s
    """, (app_id,))
    return rows[0] if rows else None


def list_apps(tenant_id: str) -> list[dict]:
    return _execute(f"""
        SELECT * FROM {SCHEMA}.connected_apps
        WHERE tenant_id = %s ORDER BY name
    """, (tenant_id,))


def update_app_status(app_id: str, active: bool) -> bool:
    _execute(f"""
        UPDATE {SCHEMA}.connected_apps SET active = %s WHERE id = %s
    """, (active, app_id), fetch=False)
    return True


# ─────────────────────────────────────────────
# API Credentials
# ─────────────────────────────────────────────

def _hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()


def generate_api_key(app_id: str, expires_at: datetime = None) -> dict:
    """
    Genera un api_key seguro para la app.
    Devuelve el api_key en texto claro (SOLO una vez) + los metadatos.
    """
    raw_key = f"rsk_{secrets.token_urlsafe(32)}"
    key_hash = _hash_api_key(raw_key)
    key_prefix = raw_key[:12]

    rows = _execute(f"""
        INSERT INTO {SCHEMA}.api_credentials (app_id, api_key_hash, api_key_prefix, expires_at)
        VALUES (%s, %s, %s, %s) RETURNING id, app_id, api_key_prefix, active, expires_at, created_at
    """, (app_id, key_hash, key_prefix, expires_at))

    result = rows[0] if rows else {}
    result["api_key"] = raw_key  # Solo se devuelve una vez
    return result


def validate_api_key(app_id: str, api_key: str) -> Optional[dict]:
    """
    Valida app_id + api_key. Devuelve credencial si válida, None si no.
    Actualiza last_used_at.
    """
    key_hash = _hash_api_key(api_key)
    rows = _execute(f"""
        SELECT c.*, a.tenant_id, a.name as app_name, a.active as app_active,
               t.active as tenant_active, t.connection_ref, t.qdrant_collection,
               t.max_documents, t.max_document_size_mb, t.max_queries_per_day,
               t.allowed_mime_types, t.config as tenant_config
        FROM {SCHEMA}.api_credentials c
        JOIN {SCHEMA}.connected_apps a ON a.id = c.app_id
        JOIN {SCHEMA}.tenants t ON t.id = a.tenant_id
        WHERE c.app_id = %s AND c.api_key_hash = %s AND c.active = TRUE
    """, (app_id, key_hash))

    if not rows:
        return None

    cred = rows[0]

    # Verificar expiración
    if cred.get("expires_at") and cred["expires_at"] < datetime.now(timezone.utc):
        return None

    # Verificar app y tenant activos
    if not cred.get("app_active") or not cred.get("tenant_active"):
        return None

    # Actualizar last_used_at
    try:
        _execute(f"""
            UPDATE {SCHEMA}.api_credentials SET last_used_at = now() WHERE id = %s
        """, (cred["id"],), fetch=False)
    except Exception:
        pass

    return cred


def rotate_api_key(app_id: str) -> dict:
    """Desactiva todas las keys anteriores y genera una nueva."""
    _execute(f"""
        UPDATE {SCHEMA}.api_credentials SET active = FALSE WHERE app_id = %s
    """, (app_id,), fetch=False)
    return generate_api_key(app_id)


# ─────────────────────────────────────────────
# Scopes
# ─────────────────────────────────────────────

def set_scopes(app_id: str, scopes: list[str]) -> list[str]:
    """Establece los scopes de una app (reemplaza los existentes)."""
    invalid = [s for s in scopes if s not in VALID_SCOPES]
    if invalid:
        raise ValueError(f"Scopes inválidos: {invalid}. Válidos: {sorted(VALID_SCOPES)}")

    _execute(f"DELETE FROM {SCHEMA}.app_scopes WHERE app_id = %s", (app_id,), fetch=False)
    for scope in scopes:
        _execute(f"""
            INSERT INTO {SCHEMA}.app_scopes (app_id, scope) VALUES (%s, %s)
            ON CONFLICT (app_id, scope) DO NOTHING
        """, (app_id, scope), fetch=False)
    return scopes


def get_scopes(app_id: str) -> list[str]:
    rows = _execute(f"SELECT scope FROM {SCHEMA}.app_scopes WHERE app_id = %s", (app_id,))
    return [r["scope"] for r in rows]


# ─────────────────────────────────────────────
# Documents
# ─────────────────────────────────────────────

_MAX_FILE_SIZE_DEFAULT_MB = 50
_DEFAULT_MIME_TYPES = ["application/pdf"]


def check_upload_allowed(tenant_id: str, file_size_bytes: int, mime_type: str,
                         file_hash: str) -> tuple[bool, str]:
    """
    Verifica si el upload está permitido.
    Reglas: tamaño, MIME, cuota, deduplicación.
    """
    tenant = get_tenant(tenant_id)
    if not tenant:
        return False, "Tenant no encontrado"
    if not tenant["active"]:
        return False, "Tenant desactivado"

    # Tamaño
    max_bytes = (tenant.get("max_document_size_mb") or _MAX_FILE_SIZE_DEFAULT_MB) * 1024 * 1024
    if file_size_bytes > max_bytes:
        return False, f"Archivo excede el limite ({file_size_bytes} > {max_bytes} bytes)"

    # MIME
    allowed = tenant.get("allowed_mime_types") or _DEFAULT_MIME_TYPES
    if mime_type not in allowed:
        return False, f"Tipo MIME no permitido: {mime_type}. Permitidos: {allowed}"

    # Cuota
    doc_count = _execute(f"""
        SELECT COUNT(*) as count FROM {SCHEMA}.tenant_documents
        WHERE tenant_id = %s AND status != 'deleting'
    """, (tenant_id,))
    current = doc_count[0]["count"] if doc_count else 0
    max_docs = tenant.get("max_documents") or 500
    if current >= max_docs:
        return False, f"Cuota de documentos alcanzada ({current}/{max_docs})"

    # Dedup
    existing = _execute(f"""
        SELECT id FROM {SCHEMA}.tenant_documents
        WHERE tenant_id = %s AND file_hash = %s AND status != 'deleting'
    """, (tenant_id, file_hash))
    if existing:
        return False, f"Documento duplicado (hash: {file_hash[:16]}...)"

    return True, "OK"


def create_document(tenant_id: str, app_id: str, filename: str,
                    original_name: str, file_size_bytes: int,
                    mime_type: str, file_hash: str,
                    metadata: dict = None) -> dict:
    rows = _execute(f"""
        INSERT INTO {SCHEMA}.tenant_documents
            (tenant_id, app_id, filename, original_name, file_size_bytes,
             mime_type, file_hash, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING *
    """, (tenant_id, app_id, filename, original_name, file_size_bytes,
          mime_type, file_hash, psycopg2.extras.Json(metadata or {})))
    return rows[0] if rows else None


def update_document_status(doc_id: str, status: str, chunk_count: int = None,
                           error_message: str = None):
    sets = ["status = %s"]
    params = [status]
    if status == "indexed":
        sets.append("indexed_at = now()")
    if chunk_count is not None:
        sets.append("chunk_count = %s")
        params.append(chunk_count)
    if error_message:
        sets.append("error_message = %s")
        params.append(error_message)
    params.append(doc_id)
    _execute(f"""
        UPDATE {SCHEMA}.tenant_documents SET {", ".join(sets)} WHERE id = %s
    """, params, fetch=False)


def get_document(doc_id: str, tenant_id: str) -> Optional[dict]:
    rows = _execute(f"""
        SELECT * FROM {SCHEMA}.tenant_documents WHERE id = %s AND tenant_id = %s
    """, (doc_id, tenant_id))
    return rows[0] if rows else None


def list_documents(tenant_id: str, status: str = None) -> list[dict]:
    where = f"WHERE tenant_id = %s"
    params = [tenant_id]
    if status:
        where += " AND status = %s"
        params.append(status)
    return _execute(f"""
        SELECT id, filename, original_name, file_size_bytes, mime_type,
               status, chunk_count, created_at, indexed_at
        FROM {SCHEMA}.tenant_documents {where} ORDER BY created_at DESC
    """, params)


def mark_document_deleting(doc_id: str, tenant_id: str) -> bool:
    rows = _execute(f"""
        UPDATE {SCHEMA}.tenant_documents SET status = 'deleting'
        WHERE id = %s AND tenant_id = %s RETURNING id
    """, (doc_id, tenant_id))
    return len(rows) > 0 if rows else False


def delete_document_record(doc_id: str):
    _execute(f"DELETE FROM {SCHEMA}.tenant_documents WHERE id = %s", (doc_id,), fetch=False)


# ─────────────────────────────────────────────
# Execution Logs
# ─────────────────────────────────────────────

def log_execution(tenant_id: str, app_id: str, query_preview: str,
                  pipeline_selected: str, pipeline_executed: str,
                  structured_result_type: str = None,
                  sql_executed: bool = False, retrieval_executed: bool = False,
                  forecast_engine_executed: bool = False,
                  llm_only_response: bool = False,
                  degraded_from: str = None, degraded_to: str = None,
                  model_used: str = None,
                  tokens_in: int = 0, tokens_out: int = 0,
                  cost_usd: float = 0, latency_ms: int = 0,
                  tool_executions: str = "[]", error: str = None) -> str:
    """Registra ejecución con tenant_id y app_id obligatorios."""
    import json as _json
    rows = _execute(f"""
        INSERT INTO {SCHEMA}.execution_logs
            (tenant_id, app_id, query_preview, pipeline_selected, pipeline_executed,
             structured_result_type, sql_executed, retrieval_executed,
             forecast_engine_executed, llm_only_response,
             degraded_from, degraded_to, model_used,
             tokens_in, tokens_out, cost_usd, latency_ms,
             tool_executions, error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (tenant_id, app_id, query_preview[:500] if query_preview else None,
          pipeline_selected, pipeline_executed, structured_result_type,
          sql_executed, retrieval_executed, forecast_engine_executed,
          llm_only_response, degraded_from, degraded_to, model_used,
          tokens_in, tokens_out, cost_usd, latency_ms,
          tool_executions, error))
    return str(rows[0]["id"]) if rows else None


def get_execution(execution_id: str, tenant_id: str) -> Optional[dict]:
    rows = _execute(f"""
        SELECT * FROM {SCHEMA}.execution_logs
        WHERE id = %s AND tenant_id = %s
    """, (execution_id, tenant_id))
    return rows[0] if rows else None


def get_usage_stats(tenant_id: str, app_id: str = None, days: int = 30) -> dict:
    where = "WHERE tenant_id = %s AND created_at > now() - interval '%s days'"
    params = [tenant_id, days]
    if app_id:
        where += " AND app_id = %s"
        params.append(app_id)

    rows = _execute(f"""
        SELECT
            COUNT(*) as total_queries,
            SUM(tokens_in) as total_tokens_in,
            SUM(tokens_out) as total_tokens_out,
            SUM(cost_usd) as total_cost_usd,
            AVG(latency_ms) as avg_latency_ms,
            COUNT(*) FILTER (WHERE sql_executed) as sql_queries,
            COUNT(*) FILTER (WHERE retrieval_executed) as rag_queries,
            COUNT(*) FILTER (WHERE forecast_engine_executed) as prediction_queries,
            COUNT(*) FILTER (WHERE error IS NOT NULL) as error_count
        FROM {SCHEMA}.execution_logs {where}
    """, params)
    return rows[0] if rows else {}
