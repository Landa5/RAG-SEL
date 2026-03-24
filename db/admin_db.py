"""
db/admin_db.py — Capa de datos para panel de administración
Tablas: admin_users, admin_sessions, admin_audit_logs
Autenticación humana separada de connected_apps.
"""
import os
import sys
import hashlib
import secrets
from datetime import datetime, timezone, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DATABASE_URL = os.getenv("DATABASE_URL", "")
SCHEMA = "platform"

# Duración de sesión por defecto: 24h
SESSION_DURATION_HOURS = int(os.getenv("RAGSEL_SESSION_HOURS", "24"))


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


def _execute_one(sql: str, params=None):
    rows = _execute(sql, params)
    return rows[0] if rows else None


# ─────────────────────────────────────────────
# Creación de tablas
# ─────────────────────────────────────────────

def create_admin_tables():
    """Crea tablas de administración del panel."""

    _execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.admin_users (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            role TEXT NOT NULL CHECK (role IN ('superadmin', 'tenant_admin')),
            tenant_id UUID REFERENCES {SCHEMA}.tenants(id) ON DELETE SET NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now(),
            last_login_at TIMESTAMPTZ
        )
    """, fetch=False)

    _execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.admin_sessions (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            admin_user_id UUID NOT NULL REFERENCES {SCHEMA}.admin_users(id) ON DELETE CASCADE,
            session_token_hash TEXT NOT NULL UNIQUE,
            expires_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now(),
            last_seen_at TIMESTAMPTZ DEFAULT now(),
            ip TEXT,
            user_agent TEXT
        )
    """, fetch=False)

    _execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.admin_audit_logs (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            admin_user_id UUID REFERENCES {SCHEMA}.admin_users(id) ON DELETE SET NULL,
            username TEXT,
            role TEXT,
            tenant_id UUID,
            action TEXT NOT NULL,
            resource_type TEXT,
            resource_id TEXT,
            payload_summary TEXT,
            created_at TIMESTAMPTZ DEFAULT now(),
            ip TEXT
        )
    """, fetch=False)

    # Índices
    for idx in [
        f"CREATE INDEX IF NOT EXISTS idx_admin_sessions_user ON {SCHEMA}.admin_sessions(admin_user_id)",
        f"CREATE INDEX IF NOT EXISTS idx_admin_sessions_token ON {SCHEMA}.admin_sessions(session_token_hash)",
        f"CREATE INDEX IF NOT EXISTS idx_admin_audit_user ON {SCHEMA}.admin_audit_logs(admin_user_id)",
        f"CREATE INDEX IF NOT EXISTS idx_admin_audit_date ON {SCHEMA}.admin_audit_logs(created_at DESC)",
    ]:
        try:
            _execute(idx, fetch=False)
        except Exception:
            pass

    print("✅ admin_users / admin_sessions / admin_audit_logs created/verified")


# ─────────────────────────────────────────────
# Password hashing (bcrypt)
# ─────────────────────────────────────────────

def _hash_password(password: str) -> str:
    """Hash con bcrypt. Fallback a SHA-256 si bcrypt no disponible."""
    try:
        import bcrypt
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    except ImportError:
        # Fallback: SHA-256 con salt
        salt = secrets.token_hex(16)
        hashed = hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
        return f"sha256:{salt}:{hashed}"


def _verify_password(password: str, password_hash: str) -> bool:
    """Verifica password contra hash."""
    try:
        import bcrypt
        if password_hash.startswith("sha256:"):
            # Fallback hash
            _, salt, expected = password_hash.split(":", 2)
            actual = hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
            return actual == expected
        return bcrypt.checkpw(password.encode(), password_hash.encode())
    except ImportError:
        if password_hash.startswith("sha256:"):
            _, salt, expected = password_hash.split(":", 2)
            actual = hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
            return actual == expected
        return False


# ─────────────────────────────────────────────
# CRUD — Admin Users
# ─────────────────────────────────────────────

def create_admin_user(
    username: str,
    password: str,
    role: str,
    tenant_id: str = None,
    display_name: str = None,
) -> dict:
    """Crea un usuario admin del panel."""
    if role not in ("superadmin", "tenant_admin"):
        raise ValueError("role debe ser 'superadmin' o 'tenant_admin'")
    if role == "tenant_admin" and not tenant_id:
        raise ValueError("tenant_admin requiere tenant_id")
    if role == "superadmin":
        tenant_id = None

    password_hash = _hash_password(password)
    rows = _execute(f"""
        INSERT INTO {SCHEMA}.admin_users
            (username, password_hash, display_name, role, tenant_id)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id, username, display_name, role, tenant_id, is_active, created_at
    """, (username, password_hash, display_name or username, role, tenant_id))
    return rows[0] if rows else None


def get_admin_user(user_id: str) -> Optional[dict]:
    return _execute_one(f"""
        SELECT id, username, display_name, role, tenant_id, is_active,
               created_at, updated_at, last_login_at
        FROM {SCHEMA}.admin_users WHERE id = %s
    """, (user_id,))


def get_admin_user_by_username(username: str) -> Optional[dict]:
    """Obtiene usuario por username (incluye password_hash para login)."""
    return _execute_one(f"""
        SELECT * FROM {SCHEMA}.admin_users WHERE username = %s
    """, (username,))


def list_admin_users(tenant_id: str = None) -> list[dict]:
    if tenant_id:
        return _execute(f"""
            SELECT id, username, display_name, role, tenant_id, is_active,
                   created_at, last_login_at
            FROM {SCHEMA}.admin_users WHERE tenant_id = %s ORDER BY username
        """, (tenant_id,))
    return _execute(f"""
        SELECT id, username, display_name, role, tenant_id, is_active,
               created_at, last_login_at
        FROM {SCHEMA}.admin_users ORDER BY role, username
    """)


def update_admin_user_login(user_id: str):
    """Actualiza last_login_at."""
    _execute(f"""
        UPDATE {SCHEMA}.admin_users SET last_login_at = now() WHERE id = %s
    """, (user_id,), fetch=False)


# ─────────────────────────────────────────────
# Sessions
# ─────────────────────────────────────────────

def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def create_session(
    admin_user_id: str,
    ip: str = None,
    user_agent: str = None,
) -> str:
    """Crea sesión y devuelve el token en claro (una sola vez)."""
    raw_token = f"rss_{secrets.token_urlsafe(48)}"
    token_hash = _hash_token(raw_token)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=SESSION_DURATION_HOURS)

    _execute(f"""
        INSERT INTO {SCHEMA}.admin_sessions
            (admin_user_id, session_token_hash, expires_at, ip, user_agent)
        VALUES (%s, %s, %s, %s, %s)
    """, (admin_user_id, token_hash, expires_at, ip, user_agent), fetch=False)

    return raw_token


def validate_session(token: str) -> Optional[dict]:
    """Valida token de sesión. Devuelve user info si válido."""
    token_hash = _hash_token(token)
    row = _execute_one(f"""
        SELECT s.id as session_id, s.admin_user_id, s.expires_at,
               u.username, u.display_name, u.role, u.tenant_id, u.is_active
        FROM {SCHEMA}.admin_sessions s
        JOIN {SCHEMA}.admin_users u ON u.id = s.admin_user_id
        WHERE s.session_token_hash = %s
    """, (token_hash,))

    if not row:
        return None

    # Verificar expiración
    expires = row["expires_at"]
    if expires and expires.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
        delete_session(token)
        return None

    # Verificar usuario activo
    if not row.get("is_active"):
        return None

    # Actualizar last_seen_at
    try:
        _execute(f"""
            UPDATE {SCHEMA}.admin_sessions SET last_seen_at = now()
            WHERE session_token_hash = %s
        """, (token_hash,), fetch=False)
    except Exception:
        pass

    return row


def delete_session(token: str):
    """Invalida una sesión."""
    token_hash = _hash_token(token)
    _execute(f"""
        DELETE FROM {SCHEMA}.admin_sessions WHERE session_token_hash = %s
    """, (token_hash,), fetch=False)


def delete_user_sessions(admin_user_id: str):
    """Invalida todas las sesiones de un usuario."""
    _execute(f"""
        DELETE FROM {SCHEMA}.admin_sessions WHERE admin_user_id = %s
    """, (admin_user_id,), fetch=False)


def cleanup_expired_sessions():
    """Limpia sesiones expiradas (para job periódico)."""
    _execute(f"""
        DELETE FROM {SCHEMA}.admin_sessions WHERE expires_at < now()
    """, fetch=False)


# ─────────────────────────────────────────────
# Audit Logs
# ─────────────────────────────────────────────

def log_audit(
    admin_user_id: str = None,
    username: str = None,
    role: str = None,
    tenant_id: str = None,
    action: str = "",
    resource_type: str = None,
    resource_id: str = None,
    payload_summary: str = None,
    ip: str = None,
):
    """Registra acción de auditoría."""
    try:
        _execute(f"""
            INSERT INTO {SCHEMA}.admin_audit_logs
                (admin_user_id, username, role, tenant_id, action,
                 resource_type, resource_id, payload_summary, ip)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (admin_user_id, username, role, tenant_id, action,
              resource_type, resource_id,
              str(payload_summary)[:500] if payload_summary else None,
              ip), fetch=False)
    except Exception as e:
        print(f"Warning: audit log failed: {e}")


def list_audit_logs(
    tenant_id: str = None,
    admin_user_id: str = None,
    action: str = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """Lista logs de auditoría con filtros."""
    where = ["1=1"]
    params = []

    if tenant_id:
        where.append("tenant_id = %s")
        params.append(tenant_id)
    if admin_user_id:
        where.append("admin_user_id = %s")
        params.append(admin_user_id)
    if action:
        where.append("action LIKE %s")
        params.append(f"%{action}%")

    params.extend([limit, offset])
    return _execute(f"""
        SELECT * FROM {SCHEMA}.admin_audit_logs
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """, params)


# ─────────────────────────────────────────────
# Login flow
# ─────────────────────────────────────────────

def authenticate_admin(username: str, password: str) -> Optional[dict]:
    """Autentica usuario admin. Devuelve info si OK, None si no."""
    user = get_admin_user_by_username(username)
    if not user:
        return None
    if not user.get("is_active"):
        return None
    if not _verify_password(password, user["password_hash"]):
        return None

    # Actualizar last_login
    update_admin_user_login(str(user["id"]))

    return {
        "id": str(user["id"]),
        "username": user["username"],
        "display_name": user.get("display_name"),
        "role": user["role"],
        "tenant_id": str(user["tenant_id"]) if user.get("tenant_id") else None,
    }


# ─────────────────────────────────────────────
# Seed superadmin
# ─────────────────────────────────────────────

def seed_superadmin(username: str = "admin", password: str = "admin"):
    """Crea superadmin inicial si no existe ninguno."""
    existing = _execute(f"""
        SELECT id FROM {SCHEMA}.admin_users WHERE role = 'superadmin' LIMIT 1
    """)
    if existing:
        return None

    user = create_admin_user(
        username=username,
        password=password,
        role="superadmin",
        display_name="Super Admin",
    )
    print(f"✅ Superadmin '{username}' creado (¡cambia la contraseña!)")
    return user
