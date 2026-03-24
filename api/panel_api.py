"""
api/panel_api.py — Endpoints del panel /app/panel/*
Dashboard, ejecuciones, documentos, health, alertas, auditoría.
Requieren sesión de panel (cookie ragsel_session).
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, Query
from api.panel_auth import get_current_admin

router = APIRouter(prefix="/app/panel", tags=["Panel"])


# ─────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────

@router.get("/dashboard")
async def dashboard_stats(admin: dict = Depends(get_current_admin)):
    """KPIs del dashboard."""
    result = {
        "tenants_active": 0, "apps_active": 0,
        "queries_today": 0, "queries_month": 0,
        "cost_month": 0.0, "providers_active": 0,
        "providers_error": 0, "total_docs": 0,
        "pending_reviews": 0, "recent_executions": [],
        "recent_alerts": [],
    }
    try:
        from db import tenant_db as tdb
        tenants = tdb.list_tenants(active_only=True)
        result["tenants_active"] = len(tenants)
    except Exception:
        pass

    try:
        from db import provider_db as pdb
        provs = pdb.list_providers()
        result["providers_active"] = sum(1 for p in provs if p.get("status") == "active")
        result["providers_error"] = sum(1 for p in provs if p.get("status") == "error")
    except Exception:
        pass

    try:
        from db import provider_db as pdb
        conn = pdb._get_conn()
        with conn.cursor() as cur:
            # Queries today
            cur.execute("SELECT count(*) FROM rag_engine.execution_logs WHERE created_at >= date_trunc('day', now() AT TIME ZONE 'UTC')")
            row = cur.fetchone()
            result["queries_today"] = row[0] if row else 0
            # Queries month
            cur.execute("SELECT count(*) FROM rag_engine.execution_logs WHERE created_at >= date_trunc('month', now() AT TIME ZONE 'UTC')")
            row = cur.fetchone()
            result["queries_month"] = row[0] if row else 0
            # Cost month
            cur.execute("SELECT COALESCE(sum(cost_usd),0) FROM rag_engine.execution_logs WHERE created_at >= date_trunc('month', now() AT TIME ZONE 'UTC')")
            row = cur.fetchone()
            result["cost_month"] = float(row[0]) if row else 0.0
            # Recent executions
            cur.execute("""
                SELECT question, pipeline, model_used, cost_usd, latency_ms, created_at
                FROM rag_engine.execution_logs ORDER BY created_at DESC LIMIT 10
            """)
            result["recent_executions"] = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception:
        pass

    try:
        from db import review_db as rdb
        pending = rdb.get_pending_human_reviews(limit=100)
        result["pending_reviews"] = len(pending)
    except Exception:
        pass

    return result


# ─────────────────────────────────────────────
# Executions
# ─────────────────────────────────────────────

@router.get("/executions")
async def list_executions(
    limit: int = Query(100, le=500),
    admin: dict = Depends(get_current_admin),
):
    """Historial de ejecuciones."""
    try:
        from db import provider_db as pdb
        conn = pdb._get_conn()
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT * FROM rag_engine.execution_logs
                ORDER BY created_at DESC LIMIT %s
            """, (limit,))
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return {"executions": rows}
    except Exception as e:
        return {"executions": [], "error": str(e)}


# ─────────────────────────────────────────────
# Documents
# ─────────────────────────────────────────────

@router.get("/documents")
async def list_documents(admin: dict = Depends(get_current_admin)):
    """Listado de documentos indexados."""
    try:
        from db import provider_db as pdb
        conn = pdb._get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, filename, tenant_id, status, file_size_bytes, created_at
                FROM platform.documents ORDER BY created_at DESC LIMIT 200
            """)
            docs = [dict(r) for r in cur.fetchall()]
        conn.close()
        return {"documents": docs}
    except Exception:
        # Tabla puede no existir aún
        import glob
        files = glob.glob("data/uploads/*.pdf")
        docs = [{"id": i, "filename": os.path.basename(f), "status": "indexed",
                 "created_at": None} for i, f in enumerate(files)]
        return {"documents": docs}


@router.delete("/documents/{doc_id}")
async def delete_document(doc_id: str, admin: dict = Depends(get_current_admin)):
    from api.panel_auth import audit_action
    audit_action(admin, "delete_document", "document", doc_id)
    return {"status": "ok", "message": f"Documento {doc_id} marcado para eliminación"}


# ─────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────

@router.get("/health")
async def system_health(admin: dict = Depends(get_current_admin)):
    """Health check completo del sistema."""
    result = {
        "api": "ok",
        "database": "unknown",
        "qdrant": "unknown",
        "providers_status": "unknown",
        "providers_active": 0,
        "providers_error": 0,
        "providers_detail": [],
    }

    # Database
    try:
        from db import provider_db as pdb
        import time
        t0 = time.time()
        conn = pdb._get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        result["database"] = "ok"
        result["db_latency_ms"] = round((time.time()-t0)*1000)
    except Exception as e:
        result["database"] = f"error: {e}"

    # Qdrant
    try:
        from qdrant_client import QdrantClient
        qc = QdrantClient(path="qdrant_storage")
        cols = qc.get_collections().collections
        result["qdrant"] = "ok"
        result["qdrant_collections"] = len(cols)
    except Exception as e:
        result["qdrant"] = f"error: {e}"

    # Providers
    try:
        from db import provider_db as pdb
        provs = pdb.list_providers()
        result["providers_active"] = sum(1 for p in provs if p.get("status") == "active")
        result["providers_error"] = sum(1 for p in provs if p.get("status") == "error")
        result["providers_status"] = "ok" if result["providers_error"] == 0 else "degraded"
        result["providers_detail"] = [{
            "provider_name": p.get("display_name") or p.get("provider_name"),
            "status": p.get("status"),
            "last_health_check": p.get("last_health_check"),
            "last_error": p.get("last_error"),
        } for p in provs]
    except Exception:
        pass

    return result


# ─────────────────────────────────────────────
# Alerts
# ─────────────────────────────────────────────

@router.get("/alerts")
async def system_alerts(admin: dict = Depends(get_current_admin)):
    """Alertas activas del sistema."""
    alerts = []

    # Provider errors
    try:
        from db import provider_db as pdb
        provs = pdb.list_providers()
        for p in provs:
            if p.get("status") == "error":
                alerts.append({
                    "type": "provider_error", "severity": "critical",
                    "source": p.get("display_name") or p.get("provider_name"),
                    "message": f"Proveedor en estado error: {p.get('last_error', '?')}",
                    "created_at": p.get("updated_at"),
                })
            if p.get("monthly_budget_usd") and p.get("monthly_spent_usd", 0) >= p.get("monthly_budget_usd", 99999):
                alerts.append({
                    "type": "budget_exceeded", "severity": "warning",
                    "source": p.get("display_name"),
                    "message": f"Budget superado: ${p.get('monthly_spent_usd',0):.2f} / ${p.get('monthly_budget_usd',0):.2f}",
                })
    except Exception:
        pass

    # Pending reviews
    try:
        from db import review_db as rdb
        pending = rdb.get_pending_human_reviews(limit=5)
        if pending:
            alerts.append({
                "type": "pending_reviews", "severity": "warning",
                "source": "AI Judge",
                "message": f"{len(pending)} reviews pendientes de revisión humana",
            })
    except Exception:
        pass

    return {"alerts": alerts}


# ─────────────────────────────────────────────
# Audit
# ─────────────────────────────────────────────

@router.get("/audit")
async def audit_logs(
    limit: int = Query(100, le=500),
    admin: dict = Depends(get_current_admin),
):
    from db import admin_db as adb
    tid = None
    if admin.get("role") != "superadmin":
        tid = str(admin.get("tenant_id")) if admin.get("tenant_id") else None
    logs = adb.list_audit_logs(tenant_id=tid, limit=limit)
    return {"logs": logs}
