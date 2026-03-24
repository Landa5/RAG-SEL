"""
db/review_db.py — Capa de datos para AI Judge reviews (schema: platform)
Tabla ai_review_logs + CRUD + queries de análisis + revisión humana + export con niveles.

Ajustes v2:
  - Severidad dual: risk_level + quality_level
  - review_status: raw_ai_review → candidate_for_training → approved_training_candidate
  - requires_human_review flag + human_verdict
  - Índices para revisión humana pendiente
"""
import os
from typing import Optional
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
SCHEMA = "platform"


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


def _execute_one(sql: str, params=None) -> Optional[dict]:
    rows = _execute(sql, params)
    return rows[0] if rows else None


# ─────────────────────────────────────────────
# Tabla
# ─────────────────────────────────────────────

_EMPTY_ARRAY = "'{}'"

def create_review_tables():
    """Crea la tabla ai_review_logs en schema platform (v2)."""
    _execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}", fetch=False)

    _execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.ai_review_logs (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            execution_id UUID NOT NULL,
            tenant_id UUID NOT NULL,
            app_id UUID NOT NULL,
            review_model TEXT NOT NULL,
            pipeline_reviewed TEXT NOT NULL,
            question_preview TEXT,
            answer_preview TEXT,

            -- Scores 0.0-1.0
            grounding_score NUMERIC(3,2) DEFAULT 0,
            hallucination_risk NUMERIC(3,2) DEFAULT 0,
            usefulness_score NUMERIC(3,2) DEFAULT 0,

            -- Checks booleanos
            pipeline_correct BOOLEAN,
            prediction_correct BOOLEAN,
            sql_consistency BOOLEAN,
            rag_evidence_present BOOLEAN,
            degradation_correct BOOLEAN,

            -- Veredicto IA
            verdict TEXT NOT NULL DEFAULT 'pending',
            issues TEXT[] DEFAULT {_EMPTY_ARRAY},
            review_text TEXT NOT NULL DEFAULT '',

            -- Severidad dual (ajuste 5)
            risk_level TEXT DEFAULT 'low',
            quality_level TEXT DEFAULT 'acceptable',

            -- Revisión humana (ajuste 2)
            requires_human_review BOOLEAN DEFAULT FALSE,
            human_review_reason TEXT,
            human_verdict TEXT,
            human_notes TEXT,
            human_reviewed_at TIMESTAMPTZ,
            human_reviewed_by TEXT,

            -- Estado de entrenamiento (ajuste 1)
            review_status TEXT DEFAULT 'raw_ai_review',

            created_at TIMESTAMPTZ DEFAULT now()
        )
    """, fetch=False)

    for idx in [
        f"CREATE INDEX IF NOT EXISTS idx_review_exec ON {SCHEMA}.ai_review_logs(execution_id)",
        f"CREATE INDEX IF NOT EXISTS idx_review_tenant ON {SCHEMA}.ai_review_logs(tenant_id)",
        f"CREATE INDEX IF NOT EXISTS idx_review_verdict ON {SCHEMA}.ai_review_logs(verdict)",
        f"CREATE INDEX IF NOT EXISTS idx_review_created ON {SCHEMA}.ai_review_logs(created_at DESC)",
        f"CREATE INDEX IF NOT EXISTS idx_review_human ON {SCHEMA}.ai_review_logs(requires_human_review) WHERE requires_human_review = TRUE",
        f"CREATE INDEX IF NOT EXISTS idx_review_status ON {SCHEMA}.ai_review_logs(review_status)",
    ]:
        try:
            _execute(idx, fetch=False)
        except Exception:
            pass

    print("✅ ai_review_logs table created/verified (v2)")


# ─────────────────────────────────────────────
# CRUD
# ─────────────────────────────────────────────

def save_review(
    execution_id: str,
    tenant_id: str,
    app_id: str,
    review_model: str,
    pipeline_reviewed: str,
    question_preview: str,
    answer_preview: str,
    grounding_score: float = 0,
    hallucination_risk: float = 0,
    usefulness_score: float = 0,
    pipeline_correct: bool = None,
    prediction_correct: bool = None,
    sql_consistency: bool = None,
    rag_evidence_present: bool = None,
    degradation_correct: bool = None,
    verdict: str = "pending",
    issues: list = None,
    review_text: str = "",
    risk_level: str = "low",
    quality_level: str = "acceptable",
    requires_human_review: bool = False,
    human_review_reason: str = None,
    review_status: str = "raw_ai_review",
) -> dict:
    """Guarda una evaluación del AI Judge (v2)."""
    rows = _execute(f"""
        INSERT INTO {SCHEMA}.ai_review_logs
            (execution_id, tenant_id, app_id, review_model, pipeline_reviewed,
             question_preview, answer_preview,
             grounding_score, hallucination_risk, usefulness_score,
             pipeline_correct, prediction_correct, sql_consistency,
             rag_evidence_present, degradation_correct,
             verdict, issues, review_text,
             risk_level, quality_level,
             requires_human_review, human_review_reason, review_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """, (
        execution_id, tenant_id, app_id, review_model, pipeline_reviewed,
        (question_preview or "")[:500], (answer_preview or "")[:2000],
        grounding_score, hallucination_risk, usefulness_score,
        pipeline_correct, prediction_correct, sql_consistency,
        rag_evidence_present, degradation_correct,
        verdict, issues or [], review_text[:5000] if review_text else "",
        risk_level, quality_level,
        requires_human_review, human_review_reason, review_status,
    ))
    return rows[0] if rows else None


def get_review(review_id: str) -> Optional[dict]:
    return _execute_one(
        f"SELECT * FROM {SCHEMA}.ai_review_logs WHERE id = %s",
        (review_id,)
    )


def get_review_by_execution(execution_id: str) -> Optional[dict]:
    return _execute_one(
        f"SELECT * FROM {SCHEMA}.ai_review_logs WHERE execution_id = %s ORDER BY created_at DESC LIMIT 1",
        (execution_id,)
    )


# ─────────────────────────────────────────────
# Revisión humana (ajuste 2)
# ─────────────────────────────────────────────

def list_pending_human_review(
    tenant_id: str = None,
    limit: int = 50,
) -> list[dict]:
    """Reviews que requieren revisión humana."""
    if tenant_id:
        return _execute(f"""
            SELECT * FROM {SCHEMA}.ai_review_logs
            WHERE requires_human_review = TRUE AND human_verdict IS NULL
              AND tenant_id = %s
            ORDER BY
                CASE risk_level
                    WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2 ELSE 3
                END,
                created_at DESC
            LIMIT %s
        """, (tenant_id, limit))
    return _execute(f"""
        SELECT * FROM {SCHEMA}.ai_review_logs
        WHERE requires_human_review = TRUE AND human_verdict IS NULL
        ORDER BY
            CASE risk_level
                WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                WHEN 'medium' THEN 2 ELSE 3
            END,
            created_at DESC
        LIMIT %s
    """, (limit,))


def submit_human_review(
    review_id: str,
    human_verdict: str,
    human_notes: str = "",
    reviewed_by: str = "admin",
    promote_to_candidate: bool = False,
) -> Optional[dict]:
    """Registra el veredicto humano sobre una review."""
    new_status = "candidate_for_training" if promote_to_candidate else "raw_ai_review"
    return _execute_one(f"""
        UPDATE {SCHEMA}.ai_review_logs
        SET human_verdict = %s,
            human_notes = %s,
            human_reviewed_at = now(),
            human_reviewed_by = %s,
            review_status = %s
        WHERE id = %s
        RETURNING *
    """, (human_verdict, human_notes, reviewed_by, new_status, review_id))


def approve_training_candidate(review_id: str) -> Optional[dict]:
    """Promueve una review a approved_training_candidate."""
    return _execute_one(f"""
        UPDATE {SCHEMA}.ai_review_logs
        SET review_status = 'approved_training_candidate'
        WHERE id = %s AND review_status = 'candidate_for_training'
        RETURNING *
    """, (review_id,))


# ─────────────────────────────────────────────
# Listados y filtros
# ─────────────────────────────────────────────

def list_reviews(
    tenant_id: str = None,
    app_id: str = None,
    pipeline: str = None,
    verdict: str = None,
    risk_level: str = None,
    quality_level: str = None,
    review_status: str = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Lista reviews con filtros opcionales."""
    where = []
    params = []

    if tenant_id:
        where.append("tenant_id = %s")
        params.append(tenant_id)
    if app_id:
        where.append("app_id = %s")
        params.append(app_id)
    if pipeline:
        where.append("pipeline_reviewed = %s")
        params.append(pipeline)
    if verdict:
        where.append("verdict = %s")
        params.append(verdict)
    if risk_level:
        where.append("risk_level = %s")
        params.append(risk_level)
    if quality_level:
        where.append("quality_level = %s")
        params.append(quality_level)
    if review_status:
        where.append("review_status = %s")
        params.append(review_status)

    where_sql = " AND ".join(where) if where else "TRUE"
    params.extend([limit, offset])

    return _execute(f"""
        SELECT * FROM {SCHEMA}.ai_review_logs
        WHERE {where_sql}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """, params)


def get_review_stats(tenant_id: str = None, days: int = 30) -> dict:
    """Estadísticas agregadas de reviews (v2 con risk_level + quality_level)."""
    where = "created_at > now() - interval '%s days'"
    params = [days]
    if tenant_id:
        where += " AND tenant_id = %s"
        params.append(tenant_id)

    return _execute_one(f"""
        SELECT
            COUNT(*) as total_reviews,
            COUNT(*) FILTER (WHERE verdict = 'pass') as passed,
            COUNT(*) FILTER (WHERE verdict = 'fail') as failed,
            COUNT(*) FILTER (WHERE verdict = 'warning') as warnings,
            AVG(grounding_score) as avg_grounding,
            AVG(hallucination_risk) as avg_hallucination_risk,
            AVG(usefulness_score) as avg_usefulness,
            COUNT(*) FILTER (WHERE pipeline_correct = FALSE) as pipeline_errors,
            COUNT(*) FILTER (WHERE sql_consistency = FALSE) as sql_inconsistencies,
            COUNT(*) FILTER (WHERE rag_evidence_present = FALSE) as missing_evidence,
            COUNT(*) FILTER (WHERE risk_level = 'critical') as critical_risks,
            COUNT(*) FILTER (WHERE risk_level = 'high') as high_risks,
            COUNT(*) FILTER (WHERE quality_level = 'poor') as poor_quality,
            COUNT(*) FILTER (WHERE quality_level = 'excellent') as excellent_quality,
            COUNT(*) FILTER (WHERE requires_human_review = TRUE) as pending_human,
            COUNT(*) FILTER (WHERE human_verdict IS NOT NULL) as human_reviewed,
            COUNT(*) FILTER (WHERE review_status = 'approved_training_candidate') as approved_for_training
        FROM {SCHEMA}.ai_review_logs
        WHERE {where}
    """, params)


def get_top_issues(tenant_id: str = None, limit: int = 10) -> list[dict]:
    """Ranking de problemas más frecuentes."""
    where = ""
    params = []
    if tenant_id:
        where = "WHERE tenant_id = %s"
        params.append(tenant_id)
    params.append(limit)

    return _execute(f"""
        SELECT unnest(issues) as issue, COUNT(*) as count
        FROM {SCHEMA}.ai_review_logs
        {where}
        GROUP BY issue ORDER BY count DESC LIMIT %s
    """, params)


# ─────────────────────────────────────────────
# Export con niveles (ajuste 1)
# ─────────────────────────────────────────────

def export_dataset(
    tenant_id: str = None,
    verdict: str = None,
    review_status: str = None,
    limit: int = 500,
) -> list[dict]:
    """
    Exporta dataset diferenciando nivel:
    - review_status='raw_ai_review' → solo telemetría
    - review_status='candidate_for_training' → revisado, pendiente aprobación
    - review_status='approved_training_candidate' → listo para fine-tuning
    """
    where = []
    params = []
    if tenant_id:
        where.append("r.tenant_id = %s")
        params.append(tenant_id)
    if verdict:
        where.append("r.verdict = %s")
        params.append(verdict)
    if review_status:
        where.append("r.review_status = %s")
        params.append(review_status)

    where_sql = " AND ".join(where) if where else "TRUE"
    params.append(limit)

    return _execute(f"""
        SELECT
            r.execution_id,
            r.pipeline_reviewed as pipeline,
            r.question_preview as question,
            r.answer_preview as answer,
            r.grounding_score,
            r.hallucination_risk,
            r.usefulness_score,
            r.pipeline_correct,
            r.sql_consistency,
            r.rag_evidence_present,
            r.verdict,
            r.risk_level,
            r.quality_level,
            r.issues,
            r.review_text,
            r.review_status,
            r.human_verdict,
            r.human_notes,
            e.pipeline_selected,
            e.pipeline_executed,
            e.sql_executed,
            e.retrieval_executed,
            e.forecast_engine_executed,
            e.model_used,
            e.degraded_from,
            e.degraded_to
        FROM {SCHEMA}.ai_review_logs r
        LEFT JOIN {SCHEMA}.execution_logs e ON e.id = r.execution_id
        WHERE {where_sql}
        ORDER BY r.created_at DESC
        LIMIT %s
    """, params)
