"""
api/auth.py — Autenticación multi-tenant para RAG-SEL API v1
El tenant_id se resuelve SIEMPRE desde app_id. X-Tenant-Id es solo crosscheck.
"""
import os
import sys
from dataclasses import dataclass, field
from typing import Optional
from functools import wraps

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import Header, HTTPException, Depends, Request
from db.tenant_db import validate_api_key, get_scopes, resolve_database_url


# ─────────────────────────────────────────────
# TenantContext
# ─────────────────────────────────────────────

@dataclass
class TenantContext:
    """Contexto de tenant inyectado en cada request autenticado."""
    tenant_id: str
    app_id: str
    app_name: str
    scopes: list[str] = field(default_factory=list)
    database_url: Optional[str] = None
    qdrant_collection: Optional[str] = None
    max_documents: int = 500
    max_document_size_mb: int = 50
    max_queries_per_day: int = 1000
    allowed_mime_types: list[str] = field(default_factory=lambda: ["application/pdf"])
    tenant_config: dict = field(default_factory=dict)


# ─────────────────────────────────────────────
# Dependencia FastAPI
# ─────────────────────────────────────────────

async def authenticate(
    request: Request = None,
    x_app_id: str = Header(None, alias="X-App-Id"),
    x_api_key: str = Header(None, alias="X-Api-Key"),
    x_tenant_id: Optional[str] = Header(None, alias="X-Tenant-Id"),
) -> TenantContext:
    """
    Dependencia de autenticación FastAPI — AUTH DUAL.
    1. Intenta cookie ragsel_session (panel admin humano)
    2. Si no hay cookie, usa X-App-Id + X-Api-Key (API machine-to-machine)
    """
    # ── 1) Intentar cookie de sesión del panel ──
    if request:
        session_token = request.cookies.get("ragsel_session")
        if session_token:
            try:
                from db.admin_db import validate_session
                user = validate_session(session_token)
                if user and user.get("is_active", True):
                    # Construir TenantContext desde sesión admin
                    role = user.get("role", "")
                    scopes = ["superadmin:*"] if role == "superadmin" else [
                        "admin:tenants", "admin:apps", "admin:credentials",
                        "admin:providers", "admin:usage", "admin:reviews",
                        "query:run", "rag:query", "documents:upload",
                        "documents:list", "documents:delete",
                        "analytics:query", "predictions:run", "executions:read",
                    ]
                    return TenantContext(
                        tenant_id=str(user.get("tenant_id") or ""),
                        app_id="panel",
                        app_name=f"Panel ({user.get('username', '?')})",
                        scopes=scopes,
                        database_url=None,
                    )
            except Exception:
                pass  # Continuar con auth por headers

    # ── 2) Auth por headers API (machine-to-machine) ──
    if not x_app_id:
        raise HTTPException(status_code=401, detail="Header X-App-Id requerido")
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Header X-Api-Key requerido")

    # Validar credenciales
    cred = validate_api_key(x_app_id, x_api_key)
    if not cred:
        raise HTTPException(status_code=401, detail="Credenciales invalidas")

    tenant_id = str(cred["tenant_id"])

    # Crosscheck X-Tenant-Id si se envía
    if x_tenant_id and str(x_tenant_id) != tenant_id:
        raise HTTPException(
            status_code=403,
            detail="X-Tenant-Id no coincide con el tenant de la app"
        )

    # Cargar scopes
    scopes = get_scopes(x_app_id)

    # Resolver database_url desde connection_ref
    database_url = None
    if cred.get("connection_ref"):
        try:
            database_url = resolve_database_url(cred["connection_ref"])
        except Exception as e:
            print(f"⚠️  Error resolviendo database_url para connection_ref={cred['connection_ref']}: {e}")

    return TenantContext(
        tenant_id=tenant_id,
        app_id=x_app_id,
        app_name=cred.get("app_name", ""),
        scopes=scopes,
        database_url=database_url,
        qdrant_collection=cred.get("qdrant_collection"),
        max_documents=cred.get("max_documents", 500),
        max_document_size_mb=cred.get("max_document_size_mb", 50),
        max_queries_per_day=cred.get("max_queries_per_day", 1000),
        allowed_mime_types=cred.get("allowed_mime_types") or ["application/pdf"],
        tenant_config=cred.get("tenant_config") or {},
    )


# ─────────────────────────────────────────────
# Scope enforcement
# ─────────────────────────────────────────────

def require_scope(scope: str):
    """
    Devuelve una dependencia FastAPI que verifica que el TenantContext
    tiene el scope requerido. superadmin:* pasa todos los checks.
    """
    async def _check(ctx: TenantContext = Depends(authenticate)):
        if "superadmin:*" in ctx.scopes:
            return ctx
        if scope not in ctx.scopes:
            raise HTTPException(
                status_code=403,
                detail=f"Scope '{scope}' requerido. Scopes disponibles: {ctx.scopes}"
            )
        return ctx
    return _check


def require_any_scope(*scopes: str):
    """Verifica que el contexto tiene AL MENOS uno de los scopes listados."""
    async def _check(ctx: TenantContext = Depends(authenticate)):
        if "superadmin:*" in ctx.scopes:
            return ctx
        if not any(s in ctx.scopes for s in scopes):
            raise HTTPException(
                status_code=403,
                detail=f"Requiere al menos uno de: {list(scopes)}"
            )
        return ctx
    return _check


# ─────────────────────────────────────────────
# RBAC explícito
# ─────────────────────────────────────────────

# Permisos por rol:
#
# superadmin:*
#   - CRUD de TODOS los tenants
#   - Crear tenants nuevos
#   - Ver/editar proveedores globales
#   - Ver todos los reviews/usage cross-tenant
#
# admin:tenants
#   - GET/PATCH su propio tenant (no crear ni borrar)
#
# admin:apps
#   - CRUD apps de su tenant
#
# admin:credentials
#   - Generar/revocar api_keys de apps de su tenant
#
# admin:providers
#   - CRUD proveedores override de su tenant
#   - NO puede tocar proveedores globales (solo superadmin)
#
# admin:usage
#   - Ver execution_logs y stats de su tenant
#
# admin:reviews
#   - Ver/gestionar AI Judge reviews de su tenant


def is_superadmin(ctx: TenantContext) -> bool:
    """¿El contexto tiene permisos de super-admin?"""
    return "superadmin:*" in ctx.scopes


def tenant_filter(ctx: TenantContext) -> str | None:
    """
    Devuelve tenant_id para filtros o None si es superadmin (sin filtro).
    Usar en endpoints admin para aislar datos por tenant.
    """
    if is_superadmin(ctx):
        return None  # ve todo
    return ctx.tenant_id


def require_tenant_access(ctx: TenantContext, target_tenant_id: str):
    """Verifica que el contexto puede acceder al tenant objetivo."""
    if is_superadmin(ctx):
        return  # superadmin accede a todo
    if ctx.tenant_id != target_tenant_id:
        raise HTTPException(
            status_code=403,
            detail="Sin acceso a este tenant"
        )


# ─────────────────────────────────────────────
# Health (sin auth)
# ─────────────────────────────────────────────

async def no_auth():
    """Dependencia sin autenticación (para /health)."""
    return None
