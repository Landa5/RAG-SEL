"""
api/api_v1.py — API pública multi-tenant v1 de RAG-SEL
Todos los endpoints requieren autenticación y scopes.
"""
import os
import sys
import json
import hashlib
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import Optional

from api.auth import TenantContext, authenticate, require_scope
from api.rate_limit import check_rate_limit
from db import tenant_db as tdb

router = APIRouter(prefix="/api/v1", tags=["API v1"])


# ─────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=5000)
    chat_history: list = Field(default_factory=list)
    force_model: Optional[str] = None

class QueryResponse(BaseModel):
    execution_id: Optional[str] = None
    answer: str
    pipeline: str = ""
    sources: list = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)

class RAGQueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=5000)

class SQLQueryRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=5000)

class PredictionRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=5000)

class DocumentResponse(BaseModel):
    id: str
    filename: str
    original_name: str
    file_size_bytes: int
    mime_type: str
    status: str
    chunk_count: int = 0
    created_at: str
    indexed_at: Optional[str] = None

class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "3.0.0"
    service: str = "rag-sel"


# ─────────────────────────────────────────────
# Health (sin auth)
# ─────────────────────────────────────────────

@router.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse()


# ─────────────────────────────────────────────
# Query general
# ─────────────────────────────────────────────

@router.post("/query", response_model=QueryResponse)
async def query(
    req: QueryRequest,
    background_tasks: BackgroundTasks,
    ctx: TenantContext = Depends(require_scope("query:run")),
    _rl: TenantContext = Depends(lambda ctx=Depends(require_scope("query:run")): check_rate_limit(ctx)),
):
    """Consulta general — el orquestador decide el pipeline."""
    from llm.generate import generate_answer_agentic

    start = time.time()
    try:
        result = generate_answer_agentic(
            req.question, req.chat_history,
            tenant_ctx=ctx,
        )
        latency = int((time.time() - start) * 1000)

        # Log
        exec_id = _log_execution(ctx, req.question, result, latency)

        # AI Judge (background)
        _schedule_review(background_tasks, exec_id, ctx, req.question, result)

        return QueryResponse(
            execution_id=exec_id,
            answer=result.get("answer", ""),
            pipeline=result.get("pipeline", ""),
            sources=result.get("sources", []),
            metadata={
                "model": result.get("model", ""),
                "latency_ms": latency,
                "engines": result.get("engines", {}),
            }
        )
    except Exception as e:
        _log_execution_error(ctx, req.question, str(e), int((time.time()-start)*1000))
        raise HTTPException(status_code=500, detail=f"Error del orquestador: {str(e)[:200]}")


@router.post("/query/stream")
async def query_stream(
    req: QueryRequest,
    ctx: TenantContext = Depends(require_scope("query:run")),
    _rl: TenantContext = Depends(lambda ctx=Depends(require_scope("query:run")): check_rate_limit(ctx)),
):
    """Consulta general con streaming SSE."""
    from llm.generate import generate_answer_stream

    def event_generator():
        try:
            for chunk in generate_answer_stream(
                req.question, req.chat_history,
                tenant_ctx=ctx,
            ):
                yield chunk
        except Exception as e:
            error_data = json.dumps({"error": f"Error del orquestador: {str(e)[:200]}"})
            yield f"data: {error_data}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ─────────────────────────────────────────────
# RAG
# ─────────────────────────────────────────────

@router.post("/rag/query", response_model=QueryResponse)
async def rag_query(
    req: RAGQueryRequest,
    ctx: TenantContext = Depends(require_scope("rag:query")),
    _rl: TenantContext = Depends(lambda ctx=Depends(require_scope("rag:query")): check_rate_limit(ctx)),
):
    """Consulta forzada a RAG documental."""
    from llm.generate import generate_answer_agentic

    start = time.time()
    try:
        result = generate_answer_agentic(
            req.question, [],
            tenant_ctx=ctx,
            force_pipeline="doc_retrieval",
        )
        latency = int((time.time() - start) * 1000)
        exec_id = _log_execution(ctx, req.question, result, latency)

        return QueryResponse(
            execution_id=exec_id,
            answer=result.get("answer", ""),
            pipeline="doc_retrieval",
            sources=result.get("sources", []),
            metadata={"model": result.get("model", ""), "latency_ms": latency}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)[:200])


# ─────────────────────────────────────────────
# Analytics (SQL)
# ─────────────────────────────────────────────

@router.post("/analytics/query", response_model=QueryResponse)
async def analytics_query(
    req: SQLQueryRequest,
    ctx: TenantContext = Depends(require_scope("analytics:query")),
    _rl: TenantContext = Depends(lambda ctx=Depends(require_scope("analytics:query")): check_rate_limit(ctx)),
):
    """Consulta forzada a SQL controlado."""
    if not ctx.database_url:
        raise HTTPException(status_code=400, detail="Tenant sin base de datos configurada")

    from llm.generate import generate_answer_agentic

    start = time.time()
    try:
        result = generate_answer_agentic(
            req.question, [],
            tenant_ctx=ctx,
            force_pipeline="agentic_sql",
        )
        latency = int((time.time() - start) * 1000)
        exec_id = _log_execution(ctx, req.question, result, latency)

        return QueryResponse(
            execution_id=exec_id,
            answer=result.get("answer", ""),
            pipeline="agentic_sql",
            sources=result.get("sources", []),
            metadata={"model": result.get("model", ""), "latency_ms": latency}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)[:200])


# ─────────────────────────────────────────────
# Predictions
# ─────────────────────────────────────────────

@router.post("/predictions/run", response_model=QueryResponse)
async def predictions_run(
    req: PredictionRequest,
    ctx: TenantContext = Depends(require_scope("predictions:run")),
    _rl: TenantContext = Depends(lambda ctx=Depends(require_scope("predictions:run")): check_rate_limit(ctx)),
):
    """Ejecutar predicción cuantitativa."""
    if not ctx.database_url:
        raise HTTPException(status_code=400, detail="Tenant sin base de datos configurada")

    from llm.generate import generate_answer_agentic

    start = time.time()
    try:
        result = generate_answer_agentic(
            req.question, [],
            tenant_ctx=ctx,
            force_pipeline="predictive_forecast",
        )
        latency = int((time.time() - start) * 1000)
        exec_id = _log_execution(ctx, req.question, result, latency)

        return QueryResponse(
            execution_id=exec_id,
            answer=result.get("answer", ""),
            pipeline=result.get("pipeline", "predictive_forecast"),
            sources=result.get("sources", []),
            metadata={"model": result.get("model", ""), "latency_ms": latency}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)[:200])


# ─────────────────────────────────────────────
# Documents
# ─────────────────────────────────────────────

@router.post("/documents/upload")
async def upload_document(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    ctx: TenantContext = Depends(require_scope("documents:upload"))
):
    """Subir un documento PDF para indexación."""
    # Leer contenido
    content = await file.read()
    file_size = len(content)
    mime = file.content_type or "application/octet-stream"
    file_hash = hashlib.sha256(content).hexdigest()

    # Validar upload
    ok, reason = tdb.check_upload_allowed(
        ctx.tenant_id, file_size, mime, file_hash
    )
    if not ok:
        raise HTTPException(status_code=400, detail=reason)

    # Guardar archivo temporal
    import tempfile
    upload_dir = os.path.join(tempfile.gettempdir(), "rag_sel_uploads", ctx.tenant_id)
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = f"{file_hash[:16]}_{file.filename}"
    file_path = os.path.join(upload_dir, safe_name)

    with open(file_path, "wb") as f:
        f.write(content)

    # Registrar documento
    doc = tdb.create_document(
        tenant_id=ctx.tenant_id,
        app_id=ctx.app_id,
        filename=safe_name,
        original_name=file.filename,
        file_size_bytes=file_size,
        mime_type=mime,
        file_hash=file_hash
    )

    # Indexar en background
    background_tasks.add_task(_index_document_background, doc["id"], file_path, ctx.tenant_id)

    return {
        "id": str(doc["id"]),
        "filename": file.filename,
        "status": "pending",
        "file_size_bytes": file_size,
        "file_hash": file_hash[:16] + "...",
    }


@router.get("/documents")
async def list_documents(
    ctx: TenantContext = Depends(require_scope("documents:list"))
):
    """Listar documentos del tenant."""
    docs = tdb.list_documents(ctx.tenant_id)
    return {"documents": docs, "count": len(docs)}


@router.delete("/documents/{doc_id}")
async def delete_document(
    doc_id: str,
    background_tasks: BackgroundTasks,
    ctx: TenantContext = Depends(require_scope("documents:delete"))
):
    """Eliminar documento y sus chunks de Qdrant."""
    doc = tdb.get_document(doc_id, ctx.tenant_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Documento no encontrado")

    # Marcar como deleting
    tdb.mark_document_deleting(doc_id, ctx.tenant_id)

    # Borrado en background (Qdrant + registro)
    background_tasks.add_task(_delete_document_background, doc_id, doc["filename"], ctx.tenant_id)

    return {"id": doc_id, "status": "deleting"}


# ─────────────────────────────────────────────
# Executions
# ─────────────────────────────────────────────

@router.get("/executions/{execution_id}")
async def get_execution(
    execution_id: str,
    ctx: TenantContext = Depends(require_scope("executions:read"))
):
    """Detalle de una ejecución."""
    exec_log = tdb.get_execution(execution_id, ctx.tenant_id)
    if not exec_log:
        raise HTTPException(status_code=404, detail="Ejecucion no encontrada")
    return exec_log


# ─────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────

def _log_execution(ctx: TenantContext, question: str, result: dict, latency_ms: int) -> str:
    """Registra ejecución en platform.execution_logs."""
    try:
        return tdb.log_execution(
            tenant_id=ctx.tenant_id,
            app_id=ctx.app_id,
            query_preview=question,
            pipeline_selected=result.get("pipeline", ""),
            pipeline_executed=result.get("pipeline", ""),
            model_used=result.get("model", ""),
            latency_ms=latency_ms,
            sql_executed=result.get("engines", {}).get("sql", False),
            retrieval_executed=result.get("engines", {}).get("retrieval", False),
            forecast_engine_executed=result.get("engines", {}).get("forecast", False),
        )
    except Exception as e:
        print(f"Error logging execution: {e}")
        return None


def _log_execution_error(ctx: TenantContext, question: str, error: str, latency_ms: int):
    try:
        tdb.log_execution(
            tenant_id=ctx.tenant_id,
            app_id=ctx.app_id,
            query_preview=question,
            pipeline_selected="error",
            pipeline_executed="error",
            latency_ms=latency_ms,
            error=error,
        )
    except Exception:
        pass


def _schedule_review(background_tasks: BackgroundTasks, exec_id, ctx, question, result):
    """Programa evaluación AI Judge como background task."""
    if not exec_id:
        return
    try:
        from eval.ai_judge import judge_and_save
        background_tasks.add_task(
            judge_and_save,
            execution_id=str(exec_id),
            tenant_id=ctx.tenant_id,
            app_id=ctx.app_id,
            question=question,
            answer=result.get("answer", ""),
            execution_data={
                "pipeline": result.get("pipeline", ""),
                "pipeline_executed": result.get("pipeline", ""),
                "model": result.get("model", ""),
                "engines": result.get("engines", {}),
                "sources": result.get("sources", []),
                "sql_executed": result.get("engines", {}).get("sql", False),
                "llm_only_response": result.get("engines", {}).get("llm_only", False),
                "degraded_from": result.get("degraded_from"),
                "degraded_to": result.get("degraded_to"),
            },
        )
    except Exception as e:
        print(f"⚠️ AI Judge scheduling failed: {e}")


def _index_document_background(doc_id: str, file_path: str, tenant_id: str):
    """Indexa documento en Qdrant con tenant_id en payload."""
    try:
        tdb.update_document_status(doc_id, "indexing")

        from ingestion.load_pdfs import load_and_chunk_pdf
        from ingestion.index_documents import index_chunks_with_tenant

        chunks = load_and_chunk_pdf(file_path)
        if not chunks:
            tdb.update_document_status(doc_id, "error", error_message="Sin chunks extraidos")
            return

        count = index_chunks_with_tenant(chunks, tenant_id)
        tdb.update_document_status(doc_id, "indexed", chunk_count=count)

    except Exception as e:
        tdb.update_document_status(doc_id, "error", error_message=str(e)[:500])
    finally:
        # Limpiar archivo temporal
        try:
            os.remove(file_path)
        except Exception:
            pass


def _delete_document_background(doc_id: str, filename: str, tenant_id: str):
    """Elimina chunks de Qdrant y luego el registro."""
    try:
        from ingestion.index_documents import delete_chunks_by_source_and_tenant
        delete_chunks_by_source_and_tenant(filename, tenant_id)
        tdb.delete_document_record(doc_id)
    except Exception as e:
        tdb.update_document_status(doc_id, "error", error_message=f"Error borrando: {str(e)[:200]}")
