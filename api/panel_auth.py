"""
api/panel_auth.py — Autenticación humana del panel /app
Separada de la auth machine-to-machine (X-App-Id / X-Api-Key).
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Request, Response, HTTPException, Depends, Cookie
from pydantic import BaseModel
from typing import Optional

from db import admin_db as adb

router = APIRouter(prefix="/app/auth", tags=["Panel Auth"])

COOKIE_NAME = "ragsel_session"
COOKIE_MAX_AGE = adb.SESSION_DURATION_HOURS * 3600
COOKIE_SECURE = os.getenv("RAGSEL_ENV", "development") != "development"


# ─────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str


# ─────────────────────────────────────────────
# Dependencia: obtiene usuario de sesión actual
# ─────────────────────────────────────────────

async def get_current_admin(request: Request) -> dict:
    """Dependencia que valida la sesión del panel.
    Busca cookie ragsel_session → valida → devuelve info del usuario."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=401, detail="Sesión no encontrada. Inicia sesión.")

    user = adb.validate_session(token)
    if not user:
        raise HTTPException(status_code=401, detail="Sesión expirada o inválida.")

    return user


async def require_superadmin(admin: dict = Depends(get_current_admin)) -> dict:
    """Dependencia: solo superadmin."""
    if admin.get("role") != "superadmin":
        raise HTTPException(status_code=403, detail="Solo superadmin")
    return admin


# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────

@router.post("/login")
async def login(req: LoginRequest, request: Request, response: Response):
    """Login humano con usuario y contraseña."""
    try:
        user = adb.authenticate_admin(req.username, req.password)
    except Exception as e:
        print(f"❌ Login error: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {type(e).__name__}")
    if not user:
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")

    # Crear sesión
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent", "")[:200]
    token = adb.create_session(user["id"], ip=ip, user_agent=ua)

    # Audit log
    adb.log_audit(
        admin_user_id=user["id"],
        username=user["username"],
        role=user["role"],
        tenant_id=user.get("tenant_id"),
        action="login",
        ip=ip,
    )

    # Set cookie
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite="lax",
        path="/",
    )

    return {
        "status": "ok",
        "user": user,
    }


@router.post("/logout")
async def logout(request: Request, response: Response):
    """Logout: invalida sesión y borra cookie."""
    token = request.cookies.get(COOKIE_NAME)
    if token:
        # Obtener info para audit
        user = adb.validate_session(token)
        if user:
            adb.log_audit(
                admin_user_id=user.get("admin_user_id"),
                username=user.get("username"),
                role=user.get("role"),
                tenant_id=str(user.get("tenant_id")) if user.get("tenant_id") else None,
                action="logout",
                ip=request.client.host if request.client else None,
            )
        adb.delete_session(token)

    response.delete_cookie(key=COOKIE_NAME, path="/")
    return {"status": "ok"}


@router.get("/me")
async def me(admin: dict = Depends(get_current_admin)):
    """Info del usuario actual del panel."""
    # Obtener nombre del tenant si aplica
    tenant_name = None
    if admin.get("tenant_id"):
        from db import tenant_db as tdb
        tenant = tdb.get_tenant(str(admin["tenant_id"]))
        if tenant:
            tenant_name = tenant.get("name")

    return {
        "user_id": str(admin.get("admin_user_id", admin.get("id", ""))),
        "username": admin["username"],
        "display_name": admin.get("display_name"),
        "role": admin["role"],
        "tenant_id": str(admin["tenant_id"]) if admin.get("tenant_id") else None,
        "tenant_name": tenant_name,
    }


# ─────────────────────────────────────────────
# Helper para auditoría desde otros módulos
# ─────────────────────────────────────────────

def audit_action(
    admin: dict,
    action: str,
    resource_type: str = None,
    resource_id: str = None,
    payload_summary: str = None,
    ip: str = None,
):
    """Registra acción de auditoría desde endpoint admin."""
    adb.log_audit(
        admin_user_id=admin.get("admin_user_id", admin.get("id")),
        username=admin.get("username"),
        role=admin.get("role"),
        tenant_id=str(admin.get("tenant_id")) if admin.get("tenant_id") else None,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        payload_summary=payload_summary,
        ip=ip,
    )
