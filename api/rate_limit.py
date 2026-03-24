"""
api/rate_limit.py — Rate limiting granular multi-tenant

3 niveles de cuota aplicados por tenant_id + app_id:
  - Burst: max_queries_per_minute (default: 20)
  - Diario: max_queries_per_day (default: 1000)
  - Coste: max_monthly_cost_usd (default: NULL = sin límite)

Precedencia: effective_limit = min(limit_tenant, limit_app) ignorando NULL.
Si ambos NULL → sin límite.

Fuente de verdad de costes: platform.execution_logs (no campo desnormalizado).
"""
import os
import sys
import math
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import Depends, HTTPException
from fastapi.responses import JSONResponse

from api.auth import TenantContext, authenticate


# ─────────────────────────────────────────────
# Contadores (queries desde execution_logs)
# ─────────────────────────────────────────────

def _count_queries_window(tenant_id: str, app_id: str, interval_sql: str) -> int:
    """Cuenta queries en una ventana de tiempo desde execution_logs."""
    try:
        from db.tenant_db import _execute
        rows = _execute(f"""
            SELECT COUNT(*) as cnt FROM platform.execution_logs
            WHERE tenant_id = %s AND app_id = %s
              AND created_at > now() - interval '{interval_sql}'
        """, (tenant_id, app_id))
        return rows[0]["cnt"] if rows else 0
    except Exception:
        return 0


def _count_tenant_queries_window(tenant_id: str, interval_sql: str) -> int:
    """Cuenta queries del tenant completo en una ventana."""
    try:
        from db.tenant_db import _execute
        rows = _execute(f"""
            SELECT COUNT(*) as cnt FROM platform.execution_logs
            WHERE tenant_id = %s
              AND created_at > now() - interval '{interval_sql}'
        """, (tenant_id,))
        return rows[0]["cnt"] if rows else 0
    except Exception:
        return 0


def _count_tenant_queries_day_natural(tenant_id: str) -> int:
    """Cuenta queries del tenant en el día natural UTC actual.
    Preparado para tenant_timezone futuro."""
    try:
        from db.tenant_db import _execute
        rows = _execute("""
            SELECT COUNT(*) as cnt FROM platform.execution_logs
            WHERE tenant_id = %s
              AND created_at >= date_trunc('day', now() AT TIME ZONE 'UTC')
        """, (tenant_id,))
        return rows[0]["cnt"] if rows else 0
    except Exception:
        return 0


def _get_monthly_cost(tenant_id: str) -> float:
    """Coste del mes actual desde execution_logs (fuente de verdad)."""
    try:
        from db.tenant_db import _execute
        rows = _execute("""
            SELECT COALESCE(SUM(cost_usd), 0) as cost FROM platform.execution_logs
            WHERE tenant_id = %s
              AND created_at >= date_trunc('month', now())
        """, (tenant_id,))
        return float(rows[0]["cost"]) if rows else 0.0
    except Exception:
        return 0.0


# ─────────────────────────────────────────────
# Resolución de límites (min tenant, app ignorando NULL)
# ─────────────────────────────────────────────

def _effective_limit(tenant_limit, app_limit):
    """
    effective_limit = min(limit_tenant, limit_app) ignorando NULL.
    Si ambos NULL → None (sin límite).
    """
    values = [v for v in (tenant_limit, app_limit) if v is not None]
    if not values:
        return None
    return min(values)


def _get_app_limits(app_id: str) -> dict:
    """Obtiene límites específicos de la app (si existen)."""
    try:
        from db.tenant_db import _execute
        rows = _execute("""
            SELECT max_queries_per_minute, max_queries_per_day, max_monthly_cost_usd
            FROM platform.connected_apps WHERE id = %s
        """, (app_id,))
        return rows[0] if rows else {}
    except Exception:
        return {}


def _get_tenant_limits(tenant_id: str) -> dict:
    """Obtiene límites del tenant."""
    try:
        from db.tenant_db import _execute
        rows = _execute("""
            SELECT max_queries_per_minute, max_queries_per_day, max_monthly_cost_usd
            FROM platform.tenants WHERE id = %s
        """, (tenant_id,))
        return rows[0] if rows else {}
    except Exception:
        return {}


# ─────────────────────────────────────────────
# Check principal
# ─────────────────────────────────────────────

def check_rate_limit(ctx: TenantContext) -> TenantContext:
    """
    FastAPI dependency: verifica cuotas antes de ejecutar queries.
    Lanza HTTPException 429 si se supera algún límite.
    """
    tenant_limits = _get_tenant_limits(ctx.tenant_id)
    app_limits = _get_app_limits(ctx.app_id)

    # ── 1. Burst (por minuto) ──
    eff_burst = _effective_limit(
        tenant_limits.get("max_queries_per_minute"),
        app_limits.get("max_queries_per_minute"),
    )
    if eff_burst is not None:
        count_min = _count_queries_window(ctx.tenant_id, ctx.app_id, "1 minute")
        if count_min >= eff_burst:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit: {count_min}/{eff_burst} queries/minuto superado",
                headers={"Retry-After": "60"},
            )

    # ── 2. Diario (día natural UTC — preparado para tenant_timezone futuro) ──
    eff_daily = _effective_limit(
        tenant_limits.get("max_queries_per_day"),
        app_limits.get("max_queries_per_day"),
    )
    if eff_daily is not None:
        count_day = _count_tenant_queries_day_natural(ctx.tenant_id)
        if count_day >= eff_daily:
            now = datetime.now(timezone.utc)
            midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            from datetime import timedelta
            next_midnight = midnight + timedelta(days=1)
            retry_after = int((next_midnight - now).total_seconds())
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit: {count_day}/{eff_daily} queries/día superado",
                headers={"Retry-After": str(retry_after)},
            )

    # ── 3. Coste mensual ──
    eff_cost = _effective_limit(
        tenant_limits.get("max_monthly_cost_usd"),
        app_limits.get("max_monthly_cost_usd"),
    )
    if eff_cost is not None:
        spent = _get_monthly_cost(ctx.tenant_id)
        if spent >= eff_cost:
            # Calcular segundos hasta fin de mes
            now = datetime.now(timezone.utc)
            if now.month == 12:
                next_month = now.replace(year=now.year + 1, month=1, day=1,
                                         hour=0, minute=0, second=0, microsecond=0)
            else:
                next_month = now.replace(month=now.month + 1, day=1,
                                         hour=0, minute=0, second=0, microsecond=0)
            retry_after = int((next_month - now).total_seconds())
            raise HTTPException(
                status_code=429,
                detail=f"Presupuesto mensual agotado: ${spent:.2f}/${eff_cost:.2f} USD",
                headers={"Retry-After": str(retry_after)},
            )

    return ctx


def rate_limit_dependency():
    """Devuelve un Depends listo para usar en endpoints."""
    async def _dep(ctx: TenantContext = Depends(authenticate)):
        return check_rate_limit(ctx)
    return Depends(_dep)
