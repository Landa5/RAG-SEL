"""
db/provider_db.py — Gestión de proveedores IA (schema: platform)
CRUD, cifrado Fernet de API keys, health check, resolución global + override por tenant.

Decisiones de diseño:
  - Master key Fernet desde env:RAGSEL_FERNET_KEY (NUNCA en BD ni código)
  - tenant_id = NULL → proveedor global
  - tenant_id = X → override para ese tenant (prioridad sobre global)
  - is_default = TRUE marca el proveedor preferido (1 por scope)
  - monthly_spent_usd es CACHE; fuente de verdad = execution_logs
"""
import os
import warnings
from typing import Optional
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
SCHEMA = "platform"

# ─────────────────────────────────────────────
# Cifrado Fernet
# ─────────────────────────────────────────────

_FERNET_KEY = os.getenv("RAGSEL_FERNET_KEY")
_RAGSEL_ENV = os.getenv("RAGSEL_ENV", "development")


def _get_fernet():
    """Obtiene instancia Fernet. Key SIEMPRE desde env.
    Fail-fast fuera de development."""
    from cryptography.fernet import Fernet
    global _FERNET_KEY
    if not _FERNET_KEY:
        if _RAGSEL_ENV != "development":
            raise RuntimeError(
                "RAGSEL_FERNET_KEY es obligatoria en staging/producción. "
                "Genera una con: python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
            )
        warnings.warn(
            "⚠️ RAGSEL_FERNET_KEY no definida. Key temporal solo para dev local.",
            RuntimeWarning,
        )
        _FERNET_KEY = Fernet.generate_key().decode()
    if isinstance(_FERNET_KEY, str):
        _FERNET_KEY = _FERNET_KEY.encode()
    return Fernet(_FERNET_KEY)


def encrypt_api_key(api_key: str) -> str:
    """Cifra una API key con Fernet."""
    f = _get_fernet()
    return f.encrypt(api_key.encode()).decode()


def decrypt_api_key(encrypted: str) -> str:
    """Descifra una API key con Fernet."""
    f = _get_fernet()
    return f.decrypt(encrypted.encode()).decode()


# ─────────────────────────────────────────────
# Conexión BD
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


def _execute_one(sql: str, params=None) -> Optional[dict]:
    rows = _execute(sql, params)
    return rows[0] if rows else None


def _sanitize_provider(row: dict) -> dict:
    """Oculta api_key_encrypted y expone has_api_key como boolean."""
    if not row:
        return row
    d = dict(row)
    has_key = bool(d.pop("api_key_encrypted", None))
    d["has_api_key"] = has_key
    # Asegurar serialización de tipos especiales
    for k in ("capabilities",):
        if k in d and d[k] is None:
            d[k] = {}
    for k in ("models_available",):
        if k in d and d[k] is None:
            d[k] = []
    return d


# ─────────────────────────────────────────────
# Tabla
# ─────────────────────────────────────────────

def create_provider_tables():
    """Crea la tabla ai_providers en schema platform."""
    _execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}", fetch=False)

    _execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.ai_providers (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            tenant_id UUID REFERENCES {SCHEMA}.tenants(id) ON DELETE CASCADE,
            provider_name TEXT NOT NULL,
            config_name TEXT DEFAULT 'default',
            display_name TEXT NOT NULL,
            api_key_encrypted TEXT,
            key_version INT DEFAULT 1,
            api_base_url TEXT,
            status TEXT DEFAULT 'pending_setup',
            is_default BOOLEAN DEFAULT FALSE,
            priority INT DEFAULT 100,
            models_available TEXT[] DEFAULT '{{}}',
            capabilities JSONB DEFAULT '{{}}'::jsonb,
            max_rpm INT DEFAULT 60,
            max_tpm INT DEFAULT 100000,
            monthly_budget_usd NUMERIC(10,2),
            monthly_spent_usd NUMERIC(10,2) DEFAULT 0,
            last_health_check TIMESTAMPTZ,
            last_error TEXT,
            notes TEXT DEFAULT '',
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now()
        )
    """, fetch=False)

    _NULL_UUID = "00000000-0000-0000-0000-000000000000"
    for idx in [
        f"""CREATE INDEX IF NOT EXISTS idx_providers_tenant
            ON {SCHEMA}.ai_providers(tenant_id)""",
        f"""CREATE UNIQUE INDEX IF NOT EXISTS idx_providers_unique
            ON {SCHEMA}.ai_providers(
                provider_name, config_name,
                COALESCE(tenant_id, '{_NULL_UUID}')
            )""",
        f"""CREATE UNIQUE INDEX IF NOT EXISTS idx_providers_one_default
            ON {SCHEMA}.ai_providers(
                COALESCE(tenant_id, '{_NULL_UUID}')
            )
            WHERE is_default = TRUE""",
    ]:
        try:
            _execute(idx, fetch=False)
        except Exception:
            pass

    print("✅ ai_providers table created/verified (v2)")


# ─────────────────────────────────────────────
# CRUD
# ─────────────────────────────────────────────

def create_provider(
    provider_name: str,
    display_name: str,
    api_key: str = None,
    tenant_id: str = None,
    api_base_url: str = None,
    models_available: list = None,
    capabilities: dict = None,
    max_rpm: int = 60,
    max_tpm: int = 100000,
    monthly_budget_usd: float = None,
    is_default: bool = False,
    priority: int = 100,
    notes: str = "",
) -> dict:
    """Registra un nuevo proveedor IA."""
    encrypted_key = encrypt_api_key(api_key) if api_key else None
    status = "active" if api_key else "pending_setup"

    # Si se marca como default, desmarcar otros en el mismo scope
    if is_default:
        _unset_default(tenant_id)

    rows = _execute(f"""
        INSERT INTO {SCHEMA}.ai_providers
            (tenant_id, provider_name, display_name, api_key_encrypted,
             api_base_url, status, is_default, priority,
             models_available, capabilities,
             max_rpm, max_tpm, monthly_budget_usd, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """, (
        tenant_id, provider_name, display_name, encrypted_key,
        api_base_url, status, is_default, priority,
        models_available or [], psycopg2.extras.Json(capabilities or {}),
        max_rpm, max_tpm, monthly_budget_usd, notes,
    ))
    return _sanitize_provider(rows[0]) if rows else None


def get_provider(provider_id: str) -> Optional[dict]:
    rows = _execute(
        f"SELECT * FROM {SCHEMA}.ai_providers WHERE id = %s",
        (provider_id,)
    )
    return _sanitize_provider(rows[0]) if rows else None


def list_providers(tenant_id: str = None) -> list[dict]:
    """Lista proveedores: globales + override del tenant si se indica."""
    if tenant_id:
        rows = _execute(f"""
            SELECT * FROM {SCHEMA}.ai_providers
            WHERE tenant_id IS NULL OR tenant_id = %s
            ORDER BY priority ASC, provider_name
        """, (tenant_id,))
    else:
        rows = _execute(f"""
            SELECT * FROM {SCHEMA}.ai_providers
            ORDER BY priority ASC, provider_name
        """)
    return [_sanitize_provider(r) for r in rows]


def update_provider(
    provider_id: str,
    api_key: str = None,
    status: str = None,
    is_default: bool = None,
    priority: int = None,
    monthly_budget_usd: float = None,
    api_base_url: str = None,
    models_available: list = None,
    capabilities: dict = None,
    notes: str = None,
) -> Optional[dict]:
    """Actualiza campos del proveedor. Solo actualiza los que no son None."""
    sets = ["updated_at = now()"]
    params = []

    if api_key is not None:
        sets.append("api_key_encrypted = %s")
        params.append(encrypt_api_key(api_key))
        sets.append("status = 'active'")
    if status is not None:
        sets.append("status = %s")
        params.append(status)
    if priority is not None:
        sets.append("priority = %s")
        params.append(priority)
    if monthly_budget_usd is not None:
        sets.append("monthly_budget_usd = %s")
        params.append(monthly_budget_usd)
    if api_base_url is not None:
        sets.append("api_base_url = %s")
        params.append(api_base_url)
    if models_available is not None:
        sets.append("models_available = %s")
        params.append(models_available)
    if capabilities is not None:
        sets.append("capabilities = %s")
        params.append(psycopg2.extras.Json(capabilities))
    if notes is not None:
        sets.append("notes = %s")
        params.append(notes)

    if is_default is not None:
        sets.append("is_default = %s")
        params.append(is_default)
        if is_default:
            # Obtener tenant_id actual para desmarcar otros defaults
            prov = get_provider(provider_id)
            if prov:
                _unset_default(prov.get("tenant_id"))

    params.append(provider_id)
    rows = _execute(f"""
        UPDATE {SCHEMA}.ai_providers SET {", ".join(sets)}
        WHERE id = %s RETURNING *
    """, params)
    return _sanitize_provider(rows[0]) if rows else None


def delete_provider(provider_id: str) -> bool:
    _execute(
        f"DELETE FROM {SCHEMA}.ai_providers WHERE id = %s",
        (provider_id,), fetch=False
    )
    return True


# ─────────────────────────────────────────────
# Resolución de proveedor para model_router
# ─────────────────────────────────────────────

def resolve_provider(provider_name: str, tenant_id: str = None) -> Optional[dict]:
    """
    Resuelve el proveedor efectivo para un tenant:
    1. Override del tenant (tenant_id = X) → prioridad
    2. Global (tenant_id IS NULL) → fallback
    Solo devuelve proveedores activos.
    """
    if tenant_id:
        # Buscar override del tenant primero
        tenant_prov = _execute_one(f"""
            SELECT * FROM {SCHEMA}.ai_providers
            WHERE provider_name = %s AND tenant_id = %s AND status = 'active'
        """, (provider_name, tenant_id))
        if tenant_prov:
            return _with_decrypted_key(tenant_prov)

    # Fallback a global
    global_prov = _execute_one(f"""
        SELECT * FROM {SCHEMA}.ai_providers
        WHERE provider_name = %s AND tenant_id IS NULL AND status = 'active'
    """, (provider_name,))
    if global_prov:
        return _with_decrypted_key(global_prov)

    return None


def get_default_provider(tenant_id: str = None) -> Optional[dict]:
    """Devuelve el proveedor default operativo (is_default=TRUE AND status='active').
    Ignora defaults en inactive/error/pending_setup."""
    if tenant_id:
        prov = _execute_one(f"""
            SELECT * FROM {SCHEMA}.ai_providers
            WHERE tenant_id = %s AND is_default = TRUE AND status = 'active'
        """, (tenant_id,))
        if prov:
            return _with_decrypted_key(prov)

    # Fallback a default global (solo si operativo)
    prov = _execute_one(f"""
        SELECT * FROM {SCHEMA}.ai_providers
        WHERE tenant_id IS NULL AND is_default = TRUE AND status = 'active'
    """)
    return _with_decrypted_key(prov) if prov else None


def get_active_providers(tenant_id: str = None) -> list[dict]:
    """Devuelve proveedores activos, priorizando override tenant sobre global."""
    all_provs = list_providers(tenant_id)
    active = [p for p in all_provs if p["status"] == "active"]

    # Deduplicar: si hay override tenant y global, queda el tenant
    if tenant_id:
        seen = {}
        for p in active:
            name = p["provider_name"]
            if name not in seen or p.get("tenant_id"):
                seen[name] = p
        return sorted(seen.values(), key=lambda p: p.get("priority", 100))

    return active


# ─────────────────────────────────────────────
# Health Check
# ─────────────────────────────────────────────

_HEALTH_CHECK_URLS = {
    "google": "https://generativelanguage.googleapis.com/v1beta/models?key={api_key}",
    "openai": "https://api.openai.com/v1/models",
    "anthropic": "https://api.anthropic.com/v1/messages",
    "mistral": "https://api.mistral.ai/v1/models",
}


def health_check(provider_id: str) -> dict:
    """
    Testea la conectividad real con el proveedor.
    Devuelve {"ok": bool, "latency_ms": int, "error": str|None, "models": list}
    """
    import time
    import requests

    prov = get_provider(provider_id)
    if not prov:
        return {"ok": False, "error": "Proveedor no encontrado", "latency_ms": 0}

    if not prov.get("api_key_encrypted"):
        return {"ok": False, "error": "Sin API key configurada", "latency_ms": 0}

    try:
        api_key = decrypt_api_key(prov["api_key_encrypted"])
    except Exception as e:
        _update_health(provider_id, False, f"Error descifrando key: {e}")
        return {"ok": False, "error": f"Error descifrando key: {e}", "latency_ms": 0}

    provider_name = prov["provider_name"]
    base_url = prov.get("api_base_url") or ""
    start = time.time()

    try:
        if provider_name == "google":
            url = base_url or _HEALTH_CHECK_URLS["google"].format(api_key=api_key)
            resp = requests.get(url, timeout=10)
        elif provider_name == "openai":
            url = base_url or _HEALTH_CHECK_URLS["openai"]
            resp = requests.get(url, headers={"Authorization": f"Bearer {api_key}"}, timeout=10)
        elif provider_name == "anthropic":
            url = base_url or _HEALTH_CHECK_URLS["anthropic"]
            resp = requests.get(url, headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01"
            }, timeout=10)
        elif provider_name == "mistral":
            url = base_url or _HEALTH_CHECK_URLS["mistral"]
            resp = requests.get(url, headers={"Authorization": f"Bearer {api_key}"}, timeout=10)
        else:
            return {"ok": False, "error": f"Health check no soportado para {provider_name}", "latency_ms": 0}

        latency = int((time.time() - start) * 1000)

        if resp.status_code < 400:
            _update_health(provider_id, True)
            # Intentar extraer modelos disponibles
            models = []
            try:
                data = resp.json()
                if "models" in data:
                    models = [m.get("name", m.get("id", "")) for m in data["models"][:20]]
            except Exception:
                pass
            return {"ok": True, "latency_ms": latency, "error": None, "models": models}
        else:
            error = f"HTTP {resp.status_code}: {resp.text[:200]}"
            _update_health(provider_id, False, error)
            return {"ok": False, "latency_ms": latency, "error": error, "models": []}

    except requests.Timeout:
        latency = int((time.time() - start) * 1000)
        _update_health(provider_id, False, "Timeout")
        return {"ok": False, "latency_ms": latency, "error": "Timeout (>10s)", "models": []}
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        error = str(e)[:200]
        _update_health(provider_id, False, error)
        return {"ok": False, "latency_ms": latency, "error": error, "models": []}


# ─────────────────────────────────────────────
# Coste mensual (fuente: execution_logs)
# ─────────────────────────────────────────────

def get_monthly_spent(tenant_id: str = None) -> float:
    """
    Calcula coste del mes actual desde execution_logs (fuente de verdad).
    """
    if tenant_id:
        rows = _execute(f"""
            SELECT COALESCE(SUM(cost_usd), 0) as spent
            FROM {SCHEMA}.execution_logs
            WHERE tenant_id = %s
              AND created_at >= date_trunc('month', now())
        """, (tenant_id,))
    else:
        rows = _execute(f"""
            SELECT COALESCE(SUM(cost_usd), 0) as spent
            FROM {SCHEMA}.execution_logs
            WHERE created_at >= date_trunc('month', now())
        """)
    return float(rows[0]["spent"]) if rows else 0.0


def refresh_monthly_spent_cache(tenant_id: str = None):
    """Job periódico: actualiza cache monthly_spent_usd desde execution_logs.
    NO es fuente de verdad — solo cache para consultas rápidas."""
    if tenant_id:
        _execute(f"""
            UPDATE {SCHEMA}.ai_providers p SET monthly_spent_usd = sub.spent
            FROM (
                SELECT COALESCE(SUM(cost_usd), 0) as spent
                FROM {SCHEMA}.execution_logs
                WHERE tenant_id = %s
                  AND created_at >= date_trunc('month', now())
            ) sub
            WHERE p.tenant_id = %s
        """, (tenant_id, tenant_id), fetch=False)
    else:
        _execute(f"""
            UPDATE {SCHEMA}.ai_providers p SET monthly_spent_usd = COALESCE(sub.spent, 0)
            FROM (
                SELECT tenant_id, SUM(cost_usd) as spent
                FROM {SCHEMA}.execution_logs
                WHERE created_at >= date_trunc('month', now())
                GROUP BY tenant_id
            ) sub
            WHERE p.tenant_id = sub.tenant_id
        """, fetch=False)


# ─────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────

def _unset_default(tenant_id: str = None):
    """Desmarca is_default en el scope del tenant (o global)."""
    if tenant_id:
        _execute(f"""
            UPDATE {SCHEMA}.ai_providers SET is_default = FALSE
            WHERE tenant_id = %s AND is_default = TRUE
        """, (tenant_id,), fetch=False)
    else:
        _execute(f"""
            UPDATE {SCHEMA}.ai_providers SET is_default = FALSE
            WHERE tenant_id IS NULL AND is_default = TRUE
        """, fetch=False)


def _update_health(provider_id: str, ok: bool, error: str = None):
    """Actualiza estado de health check."""
    status = "active" if ok else "error"
    _execute(f"""
        UPDATE {SCHEMA}.ai_providers
        SET last_health_check = now(), status = %s, last_error = %s, updated_at = now()
        WHERE id = %s
    """, (status, error, provider_id), fetch=False)


def _sanitize_provider(row: dict) -> dict:
    """Elimina api_key_encrypted del output público."""
    if not row:
        return row
    result = dict(row)
    has_key = bool(result.get("api_key_encrypted"))
    result.pop("api_key_encrypted", None)
    result["has_api_key"] = has_key
    return result


def _with_decrypted_key(row: dict) -> dict:
    """Devuelve provider con api_key descifrada (uso interno, no exponer)."""
    if not row:
        return row
    result = dict(row)
    enc = result.pop("api_key_encrypted", None)
    if enc:
        try:
            result["api_key"] = decrypt_api_key(enc)
        except Exception:
            result["api_key"] = None
    else:
        result["api_key"] = None
    return result
