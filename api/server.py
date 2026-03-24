"""
api/server.py — API FastAPI del sistema RAG-SEL V3.0 Multi-Tenant
Entrada ÚNICA por orquestador estricto. Sin bypass legacy.
API pública en /api/v1/, admin en /admin/v1/
"""
import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional

from llm.generate import generate_answer_stream
from db import model_db as mdb
from llm.leaderboard_sync import start_sync_scheduler, sync_leaderboard

# API v1 multi-tenant
from api.api_v1 import router as api_v1_router
from api.admin_v1 import router as admin_v1_router
# Panel admin
from api.panel_auth import router as panel_auth_router
from api.panel_api import router as panel_api_router

# ────────────────────────────────────────────────
# NOTA LEGACY: router.py está DESACTIVADO en producción.
# No importar classify_query, route_query ni nada de llm.router.
# ────────────────────────────────────────────────

app = FastAPI(
    title="RAG-SEL API",
    description="Motor inteligente multi-tenant con orquestación estricta V3.0",
    version="3.0.0"
)

# ── CORS ──
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Montar routers multi-tenant ──
app.include_router(api_v1_router)
app.include_router(admin_v1_router)
app.include_router(panel_auth_router)
app.include_router(panel_api_router)

# ── Montar frontend estático ──
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/app/static", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend_static")

# ── Startup: crear tablas platform y admin si no existen ──
@app.on_event("startup")
async def startup_create_platform_tables():
    try:
        from db.tenant_db import create_platform_tables
        create_platform_tables()
    except Exception as e:
        print(f"Warning: no se pudieron crear tablas platform: {e}")
    try:
        from db.admin_db import create_admin_tables, seed_superadmin
        create_admin_tables()
        seed_superadmin()
    except Exception as e:
        print(f"Warning: no se pudieron crear tablas admin: {e}")
    try:
        from db.review_db import create_review_tables
        create_review_tables()
    except Exception as e:
        print(f"Warning: no se pudieron crear tablas review: {e}")
    try:
        from db.provider_db import create_provider_tables
        create_provider_tables()
    except Exception as e:
        print(f"Warning: no se pudieron crear tablas provider: {e}")


# ─────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str
    provider: Optional[str] = "gemini"        # "gemini" | "openai"
    mode: Optional[str] = "auto"              # "auto" (único modo soportado)
    chat_history: Optional[list[dict]] = []
    force_model: Optional[str] = None  # "pro" | "flash" | None (auto)


class QueryResponse(BaseModel):
    answer: str
    sources: list[dict]
    source_mode: str
    detected_mode: str


# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    """Precarga caché y arranca scheduler de leaderboard."""
    try:
        mdb.preload_cache()
        start_sync_scheduler()
    except Exception as e:
        print(f"Warning: Error en startup rag_engine: {e}")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "RAG-SEL API v3.0", "orchestration": "strict"}


@app.get("/", response_class=HTMLResponse)
async def chat_ui():
    """Interfaz web de chat (legacy standalone)."""
    html_path = Path(__file__).parent / "chat_ui.html"
    return html_path.read_text(encoding="utf-8")


@app.get("/app/login", response_class=HTMLResponse)
async def app_login_page():
    """Página de login del panel admin."""
    html_path = FRONTEND_DIR / "login.html"
    return html_path.read_text(encoding="utf-8")


@app.get("/app/", response_class=HTMLResponse)
@app.get("/app", response_class=HTMLResponse)
async def app_main_page():
    """SPA principal del panel admin."""
    html_path = FRONTEND_DIR / "index.html"
    return html_path.read_text(encoding="utf-8")


@app.post("/chat/stream")
async def chat_stream_endpoint(req: QueryRequest):
    """
    Endpoint de Streaming usando Server-Sent Events (SSE).
    ENTRADA ÚNICA: todo pasa por run_orchestrated_pipeline() en generate.py.
    Si el orquestador falla, devuelve error controlado o degradación segura.
    PROHIBIDO: bypass legacy, respuesta libre del LLM sin orquestación.
    """
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="La pregunta no puede estar vacía.")

    def event_generator():
        try:
            for chunk in generate_answer_stream(
                req.question,
                req.chat_history or [],
                req.force_model,
            ):
                yield chunk
        except Exception as e:
            # Error controlado: nunca respuesta libre
            error_payload = json.dumps({
                "error": f"Error del orquestador: {str(e)[:200]}",
                "pipeline": "error",
                "llm_bypass": False,
            }, ensure_ascii=False)
            yield f"data: {error_payload}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


def _background_index_pdf(file_path: str):
    """Tarea en segundo plano para procesar PDF."""
    from ingestion.load_pdfs import load_pdf
    from ingestion.index_documents import index_chunks
    try:
        chunks = load_pdf(file_path)
        index_chunks(chunks, force=True)
        print(f"PDF indexado: '{file_path}'")
    except Exception as e:
        print(f"Error indexando '{file_path}': {e}")


@app.post("/upload")
async def upload_pdf(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """Sube un PDF y lo procesa en segundo plano."""
    try:
        import re
        filename = file.filename or "unknown.pdf"
        filename = filename.replace("\\", "/").split("/")[-1]
        filename = re.sub(r'[<>:"|?*]', '_', filename)

        if not filename.lower().endswith('.pdf'):
            return {"status": "skip", "message": f"'{filename}' no es PDF, ignorado."}

        temp_dir = Path("data/uploads")
        temp_dir.mkdir(parents=True, exist_ok=True)
        file_path = temp_dir / filename

        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)

        background_tasks.add_task(_background_index_pdf, str(file_path))

        return {"status": "ok", "message": f"Archivo '{filename}' recibido. Indexación en segundo plano."}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ─────────────────────────────────────────────
# Admin: Routing Dinámico V3.0
# ─────────────────────────────────────────────

@app.get("/admin/routing", response_class=HTMLResponse)
async def admin_routing_panel():
    html_path = Path(__file__).parent / "admin_models.html"
    if html_path.exists():
        return html_path.read_text(encoding="utf-8")
    return HTMLResponse("<h1>Panel admin de routing no disponible</h1>")


@app.get("/admin/api/routing/logs")
async def admin_routing_logs(limit: int = 50, pipeline: str = None, model: str = None):
    logs = mdb.get_routing_logs(limit=limit, pipeline_id=pipeline, model=model)
    return {"logs": logs, "total": len(logs)}


@app.get("/admin/api/routing/models")
async def admin_routing_models():
    models = mdb.get_all_models()
    return {"models": models}


@app.get("/admin/api/routing/pipelines")
async def admin_routing_pipelines():
    pipelines = mdb.get_pipelines(use_cache=False)
    return {"pipelines": pipelines}


@app.get("/admin/api/routing/rules")
async def admin_routing_rules():
    rules = mdb.get_routing_rules(use_cache=False)
    return {"rules": rules}


@app.post("/admin/api/routing/sync")
async def admin_trigger_sync():
    try:
        results = sync_leaderboard()
        return {"status": "ok", "results": results}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/admin/api/routing/recalculate-metrics")
async def admin_recalculate_metrics():
    try:
        mdb.recalculate_model_metrics()
        mdb.recalculate_pipeline_metrics()
        mdb.invalidate_cache()
        mdb.preload_cache()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/admin/api/routing/predictions")
async def admin_predictions(limit: int = 20):
    runs = mdb.get_prediction_runs(limit=limit)
    return {"predictions": runs}


@app.get("/admin/api/pipeline-logs")
async def admin_pipeline_logs(limit: int = 50):
    """Endpoint para los logs de ejecución de pipelines (V3.0)."""
    try:
        logs = mdb.get_pipeline_execution_logs(limit=limit)
        return {"logs": logs, "total": len(logs)}
    except Exception as e:
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api.server:app", host="0.0.0.0", port=port, reload=False)
