"""
api/admin_v1.py — Endpoints de administración multi-tenant V2
RBAC con 6 scopes admin finos + superadmin:*

Scopes admin:
  - admin:tenants      → GET/PATCH su tenant (superadmin: CRUD todos)
  - admin:apps         → CRUD apps de su tenant
  - admin:credentials  → Generar/revocar api_keys de su tenant
  - admin:providers    → CRUD proveedores override (superadmin: globales)
  - admin:usage        → Ver execution_logs y stats de su tenant
  - admin:reviews      → Gestionar AI Judge reviews de su tenant
  - superadmin:*       → Todo cross-tenant
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

from api.auth import (
    TenantContext, require_scope, require_any_scope,
    is_superadmin, tenant_filter, require_tenant_access,
)
from db import tenant_db as tdb
from db import provider_db as pdb
from db import review_db as rdb

router = APIRouter(prefix="/admin/v1", tags=["Admin v1"])


# ─────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────

class CreateTenantRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    slug: str = Field(..., min_length=1, max_length=50, pattern=r'^[a-z0-9\-]+$')
    connection_ref: Optional[str] = None
    timezone: str = "UTC"
    max_documents: int = 500
    max_queries_per_day: int = 1000

class UpdateTenantRequest(BaseModel):
    name: Optional[str] = None
    connection_ref: Optional[str] = None
    timezone: Optional[str] = None
    max_documents: Optional[int] = None
    max_document_size_mb: Optional[int] = None
    max_queries_per_minute: Optional[int] = None
    max_queries_per_day: Optional[int] = None
    max_monthly_cost_usd: Optional[float] = None
    active: Optional[bool] = None

class CreateAppRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str = ""
    scopes: list[str] = Field(default_factory=list)

class UpdateAppRequest(BaseModel):
    active: Optional[bool] = None
    scopes: Optional[list[str]] = None

class SetScopesRequest(BaseModel):
    scopes: list[str]

class CreateProviderRequest(BaseModel):
    provider_name: str = Field(..., min_length=1, max_length=50)
    config_name: str = "default"
    display_name: str = Field(..., min_length=1, max_length=200)
    api_key: Optional[str] = None
    api_base_url: Optional[str] = None
    models_available: list[str] = Field(default_factory=list)
    capabilities: dict = Field(default_factory=dict)
    max_rpm: int = 60
    max_tpm: int = 100000
    monthly_budget_usd: Optional[float] = None
    is_default: bool = False
    priority: int = 100
    is_global: bool = False
    notes: str = ""

class UpdateProviderRequest(BaseModel):
    api_key: Optional[str] = None
    status: Optional[str] = None
    is_default: Optional[bool] = None
    priority: Optional[int] = None
    monthly_budget_usd: Optional[float] = None
    api_base_url: Optional[str] = None
    models_available: Optional[list[str]] = None
    capabilities: Optional[dict] = None
    notes: Optional[str] = None


# ═════════════════════════════════════════════
# TENANTS  (scope: admin:tenants / superadmin:*)
# ═════════════════════════════════════════════

@router.post("/tenants")
async def create_tenant(
    req: CreateTenantRequest,
    ctx: TenantContext = Depends(require_scope("superadmin:*")),
):
    """Crear un nuevo tenant. Solo superadmin."""
    try:
        tenant = tdb.create_tenant(
            name=req.name, slug=req.slug,
            connection_ref=req.connection_ref,
            max_documents=req.max_documents,
            max_queries_per_day=req.max_queries_per_day,
            config={"timezone": req.timezone},
        )
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=409, detail=f"Tenant '{req.slug}' ya existe")
        raise HTTPException(status_code=500, detail=str(e)[:300])
    return {"tenant": tenant}


@router.get("/tenants")
async def list_tenants(
    active_only: bool = True,
    ctx: TenantContext = Depends(require_scope("admin:tenants")),
):
    """Listar tenants. Admin de tenant: solo el suyo. Superadmin: todos."""
    if is_superadmin(ctx):
        tenants = tdb.list_tenants(active_only=active_only)
    else:
        tenant = tdb.get_tenant(ctx.tenant_id)
        tenants = [tenant] if tenant else []
    return {"tenants": tenants}


@router.get("/tenants/{tenant_id}")
async def get_tenant(
    tenant_id: str,
    ctx: TenantContext = Depends(require_scope("admin:tenants")),
):
    """Detalle de un tenant."""
    require_tenant_access(ctx, tenant_id)
    tenant = tdb.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant no encontrado")
    return {"tenant": tenant}


@router.patch("/tenants/{tenant_id}")
async def update_tenant(
    tenant_id: str,
    req: UpdateTenantRequest,
    ctx: TenantContext = Depends(require_scope("admin:tenants")),
):
    """Editar un tenant. Admin de tenant: su propio tenant. Superadmin: cualquiera."""
    require_tenant_access(ctx, tenant_id)
    updated = tdb.update_tenant(
        tenant_id=tenant_id,
        name=req.name,
        connection_ref=req.connection_ref,
        timezone=req.timezone,
        max_documents=req.max_documents,
        max_document_size_mb=req.max_document_size_mb,
        max_queries_per_minute=req.max_queries_per_minute,
        max_queries_per_day=req.max_queries_per_day,
        max_monthly_cost_usd=req.max_monthly_cost_usd,
        active=req.active,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Tenant no encontrado")
    return {"tenant": updated}


# ═════════════════════════════════════════════
# APPS  (scope: admin:apps / superadmin:*)
# ═════════════════════════════════════════════

@router.get("/tenants/{tenant_id}/apps")
async def list_apps(
    tenant_id: str,
    ctx: TenantContext = Depends(require_scope("admin:apps")),
):
    """Listar apps conectadas de un tenant."""
    require_tenant_access(ctx, tenant_id)
    apps = tdb.list_apps(tenant_id)
    for app in apps:
        app["scopes"] = tdb.get_scopes(str(app["id"]))
    return {"apps": apps}


@router.post("/tenants/{tenant_id}/apps")
async def create_app(
    tenant_id: str,
    req: CreateAppRequest,
    ctx: TenantContext = Depends(require_scope("admin:apps")),
):
    """Crear una nueva app conectada."""
    require_tenant_access(ctx, tenant_id)
    app = tdb.create_app(tenant_id, req.name, req.description)
    if not app:
        raise HTTPException(status_code=500, detail="Error creando app")

    if req.scopes:
        try:
            tdb.set_scopes(str(app["id"]), req.scopes)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    key_info = tdb.generate_api_key(str(app["id"]))
    return {
        "app": app,
        "scopes": req.scopes,
        "credentials": {
            "app_id": str(app["id"]),
            "api_key": key_info["api_key"],
            "prefix": key_info["api_key_prefix"],
        },
        "warning": "Guarda el api_key ahora. No se podrá recuperar después."
    }


@router.patch("/apps/{app_id}")
async def update_app(
    app_id: str,
    req: UpdateAppRequest,
    ctx: TenantContext = Depends(require_scope("admin:apps")),
):
    """Actualizar estado o scopes de una app."""
    app = tdb.get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="App no encontrada")
    require_tenant_access(ctx, str(app["tenant_id"]))

    if req.active is not None:
        tdb.update_app_status(app_id, req.active)
    if req.scopes is not None:
        try:
            tdb.set_scopes(app_id, req.scopes)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    return {"status": "updated", "app_id": app_id}


@router.delete("/apps/{app_id}")
async def deactivate_app(
    app_id: str,
    ctx: TenantContext = Depends(require_scope("admin:apps")),
):
    """Desactivar una app."""
    app = tdb.get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="App no encontrada")
    require_tenant_access(ctx, str(app["tenant_id"]))
    tdb.update_app_status(app_id, False)
    return {"status": "deactivated", "app_id": app_id}


# ═════════════════════════════════════════════
# CREDENTIALS  (scope: admin:credentials / superadmin:*)
# ═════════════════════════════════════════════

@router.post("/apps/{app_id}/credentials")
async def generate_credentials(
    app_id: str,
    ctx: TenantContext = Depends(require_scope("admin:credentials")),
):
    """Generar nueva api_key para una app. Se muestra UNA sola vez."""
    app = tdb.get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="App no encontrada")
    require_tenant_access(ctx, str(app["tenant_id"]))

    key_info = tdb.generate_api_key(app_id)
    return {
        "app_id": app_id,
        "api_key": key_info["api_key"],
        "prefix": key_info["api_key_prefix"],
        "warning": "Guarda el api_key ahora. No se podrá recuperar después."
    }


@router.post("/apps/{app_id}/rotate-key")
async def rotate_key(
    app_id: str,
    ctx: TenantContext = Depends(require_scope("admin:credentials")),
):
    """Rotar API key (desactiva la anterior y genera una nueva)."""
    app = tdb.get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="App no encontrada")
    require_tenant_access(ctx, str(app["tenant_id"]))

    key_info = tdb.rotate_api_key(app_id)
    return {
        "app_id": app_id,
        "api_key": key_info["api_key"],
        "prefix": key_info["api_key_prefix"],
        "warning": "La key anterior ha sido desactivada. Guarda la nueva."
    }


# ═════════════════════════════════════════════
# SCOPES  (scope: admin:credentials / superadmin:*)
# ═════════════════════════════════════════════

@router.get("/apps/{app_id}/scopes")
async def get_scopes(
    app_id: str,
    ctx: TenantContext = Depends(require_scope("admin:credentials")),
):
    """Ver scopes actuales de una app."""
    app = tdb.get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="App no encontrada")
    require_tenant_access(ctx, str(app["tenant_id"]))
    scopes = tdb.get_scopes(app_id)
    return {"app_id": app_id, "scopes": scopes}


@router.put("/apps/{app_id}/scopes")
async def set_scopes(
    app_id: str,
    req: SetScopesRequest,
    ctx: TenantContext = Depends(require_scope("admin:credentials")),
):
    """Reemplazar scopes de una app. Valida contra VALID_SCOPES."""
    app = tdb.get_app(app_id)
    if not app:
        raise HTTPException(status_code=404, detail="App no encontrada")
    require_tenant_access(ctx, str(app["tenant_id"]))

    # Solo superadmin puede asignar superadmin:*
    if "superadmin:*" in req.scopes and not is_superadmin(ctx):
        raise HTTPException(status_code=403, detail="Solo superadmin puede asignar superadmin:*")

    try:
        tdb.set_scopes(app_id, req.scopes)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"app_id": app_id, "scopes": req.scopes}


# ═════════════════════════════════════════════
# USAGE  (scope: admin:usage / superadmin:*)
# ═════════════════════════════════════════════

@router.get("/tenants/{tenant_id}/usage")
async def get_usage(
    tenant_id: str,
    ctx: TenantContext = Depends(require_scope("admin:usage")),
):
    """Estadísticas de consumo de un tenant."""
    require_tenant_access(ctx, tenant_id)
    stats = tdb.get_usage_stats(tenant_id)
    return {"tenant_id": tenant_id, "usage": stats}


# ═════════════════════════════════════════════
# PROVIDERS  (scope: admin:providers / superadmin:*)
# ═════════════════════════════════════════════

@router.get("/providers")
async def list_providers(
    ctx: TenantContext = Depends(require_scope("admin:providers")),
):
    """Listar proveedores IA (globales + override del tenant)."""
    providers = pdb.list_providers(ctx.tenant_id)
    monthly_spent = pdb.get_monthly_spent(ctx.tenant_id)
    return {"providers": providers, "monthly_spent_usd": monthly_spent}


@router.post("/providers")
async def create_provider(
    req: CreateProviderRequest,
    ctx: TenantContext = Depends(require_scope("admin:providers")),
):
    """Registrar un nuevo proveedor IA.
    is_global=True solo permitido para superadmin."""
    if req.is_global and not is_superadmin(ctx):
        raise HTTPException(status_code=403, detail="Solo superadmin puede crear proveedores globales")

    tenant_id = None if req.is_global else ctx.tenant_id
    try:
        provider = pdb.create_provider(
            provider_name=req.provider_name,
            display_name=req.display_name,
            api_key=req.api_key,
            tenant_id=tenant_id,
            api_base_url=req.api_base_url,
            models_available=req.models_available,
            capabilities=req.capabilities,
            max_rpm=req.max_rpm,
            max_tpm=req.max_tpm,
            monthly_budget_usd=req.monthly_budget_usd,
            is_default=req.is_default,
            priority=req.priority,
            notes=req.notes,
        )
    except Exception as e:
        if "idx_providers_unique" in str(e):
            raise HTTPException(status_code=409, detail=f"Proveedor '{req.provider_name}/{req.config_name}' ya existe")
        raise HTTPException(status_code=500, detail=str(e)[:300])
    return {"provider": provider}


@router.patch("/providers/{provider_id}")
async def update_provider(
    provider_id: str,
    req: UpdateProviderRequest,
    ctx: TenantContext = Depends(require_scope("admin:providers")),
):
    """Actualizar un proveedor IA."""
    existing = pdb.get_provider(provider_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Proveedor no encontrado")

    prov_tenant = existing.get("tenant_id")
    if prov_tenant:
        require_tenant_access(ctx, str(prov_tenant))
    elif not is_superadmin(ctx):
        raise HTTPException(status_code=403, detail="Solo superadmin puede editar proveedores globales")

    updated = pdb.update_provider(
        provider_id,
        api_key=req.api_key, status=req.status,
        is_default=req.is_default, priority=req.priority,
        monthly_budget_usd=req.monthly_budget_usd,
        api_base_url=req.api_base_url,
        models_available=req.models_available,
        capabilities=req.capabilities, notes=req.notes,
    )
    return {"provider": updated}


@router.delete("/providers/{provider_id}")
async def delete_provider(
    provider_id: str,
    ctx: TenantContext = Depends(require_scope("admin:providers")),
):
    """Eliminar un proveedor IA."""
    existing = pdb.get_provider(provider_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Proveedor no encontrado")

    prov_tenant = existing.get("tenant_id")
    if prov_tenant:
        require_tenant_access(ctx, str(prov_tenant))
    elif not is_superadmin(ctx):
        raise HTTPException(status_code=403, detail="Solo superadmin puede borrar proveedores globales")

    pdb.delete_provider(provider_id)
    return {"status": "deleted", "provider_id": provider_id}


@router.get("/providers/{provider_id}/health")
async def provider_health(
    provider_id: str,
    ctx: TenantContext = Depends(require_scope("admin:providers")),
):
    """Test de conectividad real con el proveedor IA."""
    existing = pdb.get_provider(provider_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Proveedor no encontrado")

    result = pdb.health_check(provider_id)
    return {
        "provider_id": provider_id,
        "provider_name": existing["provider_name"],
        "health": result,
    }


# ═════════════════════════════════════════════
# AI JUDGE — REVIEWS  (scope: admin:reviews / superadmin:*)
# ═════════════════════════════════════════════

@router.get("/reviews")
async def list_reviews(
    app_id: Optional[str] = None,
    pipeline: Optional[str] = None,
    verdict: Optional[str] = None,
    risk_level: Optional[str] = None,
    quality_level: Optional[str] = None,
    review_status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    ctx: TenantContext = Depends(require_scope("admin:reviews")),
):
    """Panel de revisiones: listar reviews con filtros v2."""
    tid = tenant_filter(ctx) or (ctx.tenant_id if ctx.tenant_id else None)
    reviews = rdb.list_reviews(
        tenant_id=tid, app_id=app_id, pipeline=pipeline,
        verdict=verdict, risk_level=risk_level,
        quality_level=quality_level, review_status=review_status,
        limit=min(limit, 200), offset=offset,
    )
    return {"reviews": reviews, "count": len(reviews)}


@router.get("/reviews/stats")
async def review_stats(
    days: int = 30,
    ctx: TenantContext = Depends(require_scope("admin:reviews")),
):
    """Estadísticas agregadas v2."""
    tid = tenant_filter(ctx) or (ctx.tenant_id if ctx.tenant_id else None)
    stats = rdb.get_review_stats(tenant_id=tid, days=days)
    top_issues = rdb.get_top_issues(tenant_id=tid, limit=15)
    return {"period_days": days, "stats": stats, "top_issues": top_issues}


@router.get("/reviews/pending")
async def pending_human_reviews(
    limit: int = 50,
    ctx: TenantContext = Depends(require_scope("admin:reviews")),
):
    """Reviews que requieren revisión humana."""
    tid = tenant_filter(ctx) or (ctx.tenant_id if ctx.tenant_id else None)
    pending = rdb.list_pending_human_review(tenant_id=tid, limit=min(limit, 100))
    return {"pending": pending, "count": len(pending)}


@router.post("/reviews/{review_id}/human-review")
async def submit_human_review(
    review_id: str,
    human_verdict: str,
    human_notes: str = "",
    promote: bool = False,
    ctx: TenantContext = Depends(require_scope("admin:reviews")),
):
    """Registra el veredicto humano sobre una review."""
    review = rdb.get_review(review_id)
    if not review:
        raise HTTPException(status_code=404, detail="Review no encontrada")
    require_tenant_access(ctx, str(review.get("tenant_id")))

    if human_verdict not in ("confirmed_pass", "confirmed_fail", "overridden_pass", "overridden_fail"):
        raise HTTPException(
            status_code=400,
            detail="human_verdict: confirmed_pass, confirmed_fail, overridden_pass, overridden_fail"
        )

    updated = rdb.submit_human_review(
        review_id=review_id, human_verdict=human_verdict,
        human_notes=human_notes, reviewed_by=ctx.app_id or "admin",
        promote_to_candidate=promote,
    )
    return {"review": updated}


@router.post("/reviews/{review_id}/approve-training")
async def approve_training(
    review_id: str,
    ctx: TenantContext = Depends(require_scope("admin:reviews")),
):
    """Promueve candidate_for_training → approved_training_candidate."""
    review = rdb.get_review(review_id)
    if not review:
        raise HTTPException(status_code=404, detail="Review no encontrada")
    require_tenant_access(ctx, str(review.get("tenant_id")))
    if review.get("review_status") != "candidate_for_training":
        raise HTTPException(status_code=400, detail=f"Solo candidates. Estado: {review.get('review_status')}")

    updated = rdb.approve_training_candidate(review_id)
    return {"review": updated}


@router.get("/reviews/export")
async def export_reviews(
    verdict: Optional[str] = None,
    review_status: Optional[str] = None,
    limit: int = 500,
    format: str = "json",
    ctx: TenantContext = Depends(require_scope("admin:reviews")),
):
    """Export dataset diferenciado por review_status."""
    tid = tenant_filter(ctx) or (ctx.tenant_id if ctx.tenant_id else None)
    data = rdb.export_dataset(
        tenant_id=tid, verdict=verdict,
        review_status=review_status, limit=min(limit, 2000),
    )

    if format == "csv":
        import csv
        import io
        from fastapi.responses import StreamingResponse

        output = io.StringIO()
        if data:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            for row in data:
                row_clean = {}
                for k, v in row.items():
                    row_clean[k] = "; ".join(str(i) for i in v) if isinstance(v, list) else v
                writer.writerow(row_clean)
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=ragsel_reviews.csv"},
        )

    return {"dataset": data, "count": len(data)}


@router.get("/reviews/{review_id}")
async def get_review(
    review_id: str,
    ctx: TenantContext = Depends(require_scope("admin:reviews")),
):
    """Detalle de una review específica."""
    review = rdb.get_review(review_id)
    if not review:
        raise HTTPException(status_code=404, detail="Review no encontrada")
    require_tenant_access(ctx, str(review.get("tenant_id")))
    return {"review": review}
