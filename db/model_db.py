"""
db/model_db.py — Capa de acceso a datos para rag_engine (PostgreSQL/Supabase)
Incluye caché en memoria con TTL para evitar queries por cada request.
"""
import os
import time
import uuid
import threading
import hashlib
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
SCHEMA = "rag_engine"
CACHE_TTL_SECONDS = 300  # 5 minutos


# ─────────────────────────────────────────────
# Conexión
# ─────────────────────────────────────────────

def _clean_url(url: str) -> str:
    from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params.pop("pgbouncer", None)
    clean_query = urlencode({k: v[0] for k, v in params.items()})
    return urlunparse(parsed._replace(query=clean_query))


def get_conn():
    """Obtiene conexión PostgreSQL."""
    return psycopg2.connect(_clean_url(DATABASE_URL), connect_timeout=10)


def _execute(query: str, params=None, fetch=True):
    """Ejecuta query con manejo de conexión automático."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch:
                result = cur.fetchall()
            else:
                result = cur.rowcount
            conn.commit()
            return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def _execute_one(query: str, params=None):
    """Ejecuta query y devuelve un solo resultado."""
    results = _execute(query, params)
    return dict(results[0]) if results else None


# ─────────────────────────────────────────────
# Caché en memoria con TTL
# ─────────────────────────────────────────────

class _Cache:
    """Caché thread-safe con TTL configurable."""

    def __init__(self, ttl: int = CACHE_TTL_SECONDS):
        self.ttl = ttl
        self._data: dict = {}
        self._timestamps: dict[str, float] = {}
        self._lock = threading.Lock()

    def get(self, key: str):
        with self._lock:
            ts = self._timestamps.get(key, 0)
            if time.time() - ts < self.ttl:
                return self._data.get(key)
            return None

    def set(self, key: str, value):
        with self._lock:
            self._data[key] = value
            self._timestamps[key] = time.time()

    def invalidate(self, key: str = None):
        with self._lock:
            if key:
                self._data.pop(key, None)
                self._timestamps.pop(key, None)
            else:
                self._data.clear()
                self._timestamps.clear()

    def is_stale(self, key: str) -> bool:
        with self._lock:
            ts = self._timestamps.get(key, 0)
            return time.time() - ts >= self.ttl


_cache = _Cache()


def invalidate_cache(key: str = None):
    """Invalida caché (parcial o total). Llamar tras cambios admin o sync."""
    _cache.invalidate(key)


# ─────────────────────────────────────────────
# model_registry
# ─────────────────────────────────────────────

def get_active_models(use_cache: bool = True) -> list[dict]:
    """Devuelve modelos activos (no preliminary, no disabled)."""
    if use_cache:
        cached = _cache.get("active_models")
        if cached is not None:
            return cached

    rows = _execute(f"""
        SELECT * FROM {SCHEMA}.model_registry
        WHERE status = 'active' AND is_preliminary = FALSE
        ORDER BY price_output ASC
    """)
    result = [dict(r) for r in rows]
    _cache.set("active_models", result)
    return result


def get_all_models() -> list[dict]:
    """Todos los modelos (para admin)."""
    rows = _execute(f"SELECT * FROM {SCHEMA}.model_registry ORDER BY status, display_name")
    return [dict(r) for r in rows]


def get_model_by_id(model_id: str) -> Optional[dict]:
    """Busca modelo por model_id (ej: 'gemini-3.1-pro-preview')."""
    return _execute_one(
        f"SELECT * FROM {SCHEMA}.model_registry WHERE model_id = %s",
        (model_id,)
    )


def get_model_by_uuid(uuid_str: str) -> Optional[dict]:
    return _execute_one(
        f"SELECT * FROM {SCHEMA}.model_registry WHERE id = %s",
        (uuid_str,)
    )


def update_model_status(model_uuid: str, status: str):
    _execute(
        f"UPDATE {SCHEMA}.model_registry SET status = %s, updated_at = now() WHERE id = %s",
        (status, model_uuid), fetch=False
    )
    _cache.invalidate("active_models")


def upsert_model(provider: str, model_id: str, display_name: str,
                 arena_scores: dict, arena_ranks: dict, is_preliminary: bool,
                 price_input: float, price_output: float,
                 context_window: int = 128000, **capabilities) -> dict:
    """Inserta o actualiza modelo en el registro."""
    import json
    status = 'preliminary' if is_preliminary else 'active'
    existing = get_model_by_id(model_id)

    if existing:
        _execute(f"""
            UPDATE {SCHEMA}.model_registry SET
                arena_scores = %s::jsonb, arena_ranks = %s::jsonb,
                is_preliminary = %s, price_input = %s, price_output = %s,
                status = CASE WHEN is_preliminary AND NOT %s THEN 'active'
                              WHEN NOT is_preliminary AND %s THEN 'preliminary'
                              ELSE status END,
                last_synced_at = now(), updated_at = now()
            WHERE model_id = %s
        """, (json.dumps(arena_scores), json.dumps(arena_ranks),
              is_preliminary, price_input, price_output,
              is_preliminary, is_preliminary, model_id), fetch=False)
        result = get_model_by_id(model_id)
    else:
        _execute(f"""
            INSERT INTO {SCHEMA}.model_registry
            (provider, model_id, display_name, arena_scores, arena_ranks,
             is_preliminary, context_window, supports_tools, supports_json,
             supports_vision, supports_streaming, price_input, price_output, status, last_synced_at)
            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        """, (provider, model_id, display_name,
              json.dumps(arena_scores), json.dumps(arena_ranks),
              is_preliminary, context_window,
              capabilities.get('supports_tools', True),
              capabilities.get('supports_json', True),
              capabilities.get('supports_vision', False),
              capabilities.get('supports_streaming', True),
              price_input, price_output, status), fetch=False)
        result = get_model_by_id(model_id)

    _cache.invalidate("active_models")
    return result


def update_model_metrics(model_uuid: str, avg_latency_ms: int = None,
                         p95_latency_ms: int = None, success_rate_7d: float = None,
                         avg_feedback_score: float = None, total_requests_7d: int = None):
    """Actualiza métricas operacionales de un modelo."""
    sets = []
    params = []
    if avg_latency_ms is not None:
        sets.append("avg_latency_ms = %s")
        params.append(avg_latency_ms)
    if p95_latency_ms is not None:
        sets.append("p95_latency_ms = %s")
        params.append(p95_latency_ms)
    if success_rate_7d is not None:
        sets.append("success_rate_7d = %s")
        params.append(success_rate_7d)
    if avg_feedback_score is not None:
        sets.append("avg_feedback_score = %s")
        params.append(avg_feedback_score)
    if total_requests_7d is not None:
        sets.append("total_requests_7d = %s")
        params.append(total_requests_7d)
    if not sets:
        return
    sets.append("updated_at = now()")
    params.append(model_uuid)
    _execute(
        f"UPDATE {SCHEMA}.model_registry SET {', '.join(sets)} WHERE id = %s",
        params, fetch=False
    )
    _cache.invalidate("active_models")


# ─────────────────────────────────────────────
# pipelines
# ─────────────────────────────────────────────

def get_pipelines(use_cache: bool = True) -> list[dict]:
    if use_cache:
        cached = _cache.get("pipelines")
        if cached is not None:
            return cached
    rows = _execute(f"SELECT * FROM {SCHEMA}.pipelines WHERE enabled = TRUE ORDER BY id")
    result = [dict(r) for r in rows]
    _cache.set("pipelines", result)
    return result


def get_pipeline(pipeline_id: str) -> Optional[dict]:
    pipelines = get_pipelines()
    return next((p for p in pipelines if p["id"] == pipeline_id), None)


# ─────────────────────────────────────────────
# routing_rules
# ─────────────────────────────────────────────

def get_routing_rules(use_cache: bool = True) -> list[dict]:
    if use_cache:
        cached = _cache.get("routing_rules")
        if cached is not None:
            return cached
    rows = _execute(f"""
        SELECT * FROM {SCHEMA}.routing_rules
        WHERE enabled = TRUE ORDER BY priority DESC
    """)
    result = [dict(r) for r in rows]
    _cache.set("routing_rules", result)
    return result


# ─────────────────────────────────────────────
# fallback_chains
# ─────────────────────────────────────────────

def get_fallback_chains(use_cache: bool = True) -> dict[str, list[dict]]:
    """Devuelve fallback chains agrupadas por pipeline_id."""
    if use_cache:
        cached = _cache.get("fallback_chains")
        if cached is not None:
            return cached
    rows = _execute(f"""
        SELECT fc.*, mr.model_id as model_name, mr.provider
        FROM {SCHEMA}.fallback_chains fc
        JOIN {SCHEMA}.model_registry mr ON fc.model_id = mr.id
        WHERE fc.enabled = TRUE AND mr.status = 'active' AND mr.is_preliminary = FALSE
        ORDER BY fc.pipeline_id, fc.position
    """)
    chains: dict[str, list[dict]] = {}
    for r in rows:
        r = dict(r)
        pid = r["pipeline_id"]
        if pid not in chains:
            chains[pid] = []
        chains[pid].append(r)
    _cache.set("fallback_chains", chains)
    return chains


# ─────────────────────────────────────────────
# model_pipeline_metrics
# ─────────────────────────────────────────────

def get_pipeline_metrics(model_uuid: str, pipeline_id: str) -> Optional[dict]:
    return _execute_one(f"""
        SELECT * FROM {SCHEMA}.model_pipeline_metrics
        WHERE model_id = %s AND pipeline_id = %s
    """, (model_uuid, pipeline_id))


def upsert_pipeline_metrics(model_uuid: str, pipeline_id: str,
                            avg_latency_ms: int, p95_latency_ms: int,
                            success_rate_7d: float, avg_feedback_score: float,
                            avg_cost_actual: float, sample_size: int):
    _execute(f"""
        INSERT INTO {SCHEMA}.model_pipeline_metrics
        (model_id, pipeline_id, avg_latency_ms, p95_latency_ms,
         success_rate_7d, avg_feedback_score, avg_cost_actual, sample_size, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (model_id, pipeline_id) DO UPDATE SET
            avg_latency_ms = EXCLUDED.avg_latency_ms,
            p95_latency_ms = EXCLUDED.p95_latency_ms,
            success_rate_7d = EXCLUDED.success_rate_7d,
            avg_feedback_score = EXCLUDED.avg_feedback_score,
            avg_cost_actual = EXCLUDED.avg_cost_actual,
            sample_size = EXCLUDED.sample_size,
            updated_at = now()
    """, (model_uuid, pipeline_id, avg_latency_ms, p95_latency_ms,
          success_rate_7d, avg_feedback_score, avg_cost_actual, sample_size), fetch=False)


# ─────────────────────────────────────────────
# model_routing_logs
# ─────────────────────────────────────────────

def log_routing_decision(query_preview: str, task_family: str,
                         detected_language: str, detected_features: dict,
                         pipeline_id: str, pipeline_reason: str,
                         selected_model: str, selection_reason: str,
                         composite_score: float, score_breakdown: dict,
                         fallback_chain: list[str]) -> str:
    """Registra decisión de routing. Devuelve el ID del log."""
    import json
    log_id = str(uuid.uuid4())
    _execute(f"""
        INSERT INTO {SCHEMA}.model_routing_logs
        (id, query_preview, task_family, detected_language, detected_features,
         pipeline_id, pipeline_reason, selected_model, selection_reason,
         composite_score, score_breakdown, fallback_chain)
        VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s::jsonb, %s)
    """, (log_id, query_preview[:150], task_family, detected_language,
          json.dumps(detected_features), pipeline_id, pipeline_reason,
          selected_model, selection_reason, composite_score,
          json.dumps(score_breakdown), fallback_chain), fetch=False)
    return log_id


def update_routing_log_execution(log_id: str, tokens_input: int = None,
                                  tokens_output: int = None, cost_estimated: float = None,
                                  cost_actual: float = None, latency_ms: int = None,
                                  error: str = None, fallback_triggered: bool = False,
                                  fallback_model_used: str = None,
                                  fallback_reason: str = None):
    """Actualiza log con datos de ejecución post-respuesta."""
    _execute(f"""
        UPDATE {SCHEMA}.model_routing_logs SET
            tokens_input = %s, tokens_output = %s, cost_estimated = %s,
            cost_actual = %s, latency_ms = %s, error = %s,
            fallback_triggered = %s, fallback_model_used = %s, fallback_reason = %s
        WHERE id = %s
    """, (tokens_input, tokens_output, cost_estimated, cost_actual,
          latency_ms, error, fallback_triggered, fallback_model_used,
          fallback_reason, log_id), fetch=False)


def get_routing_logs(limit: int = 50, pipeline_id: str = None,
                     model: str = None) -> list[dict]:
    """Obtiene logs de routing para admin."""
    where = []
    params = []
    if pipeline_id:
        where.append("pipeline_id = %s")
        params.append(pipeline_id)
    if model:
        where.append("selected_model = %s")
        params.append(model)
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    params.append(limit)
    rows = _execute(f"""
        SELECT * FROM {SCHEMA}.model_routing_logs
        {where_clause} ORDER BY created_at DESC LIMIT %s
    """, params)
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
# response_feedback
# ─────────────────────────────────────────────

def add_feedback(routing_log_id: str, rating: int, feedback_type: str = "auto_quality",
                 feedback_text: str = None, was_hallucination: bool = None,
                 was_incomplete: bool = None, was_wrong_model: bool = None):
    _execute(f"""
        INSERT INTO {SCHEMA}.response_feedback
        (routing_log_id, rating, feedback_type, feedback_text,
         was_hallucination, was_incomplete, was_wrong_model)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (routing_log_id, rating, feedback_type, feedback_text,
          was_hallucination, was_incomplete, was_wrong_model), fetch=False)


# ─────────────────────────────────────────────
# leaderboard_snapshots
# ─────────────────────────────────────────────

def save_leaderboard_snapshot(category: str, raw_data: dict, models_count: int,
                              sync_status: str = "success", error_message: str = None):
    import json
    _execute(f"""
        INSERT INTO {SCHEMA}.leaderboard_snapshots
        (category, raw_data, models_count, sync_status, error_message)
        VALUES (%s, %s::jsonb, %s, %s, %s)
    """, (category, json.dumps(raw_data), models_count, sync_status, error_message), fetch=False)
    # Actualizar timestamp en arena_categories
    _execute(f"""
        UPDATE {SCHEMA}.arena_categories SET last_synced_at = now() WHERE id = %s
    """, (category,), fetch=False)


def get_last_snapshot(category: str) -> Optional[dict]:
    return _execute_one(f"""
        SELECT * FROM {SCHEMA}.leaderboard_snapshots
        WHERE category = %s AND sync_status = 'success'
        ORDER BY synced_at DESC LIMIT 1
    """, (category,))


def get_arena_categories() -> list[dict]:
    rows = _execute(f"SELECT * FROM {SCHEMA}.arena_categories WHERE sync_enabled = TRUE")
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
# prediction_runs
# ─────────────────────────────────────────────

def log_prediction_run(query_preview: str, target_variable: str,
                       method: str, horizon: str, dataset_size: int,
                       prediction_value: float, confidence: float,
                       backtesting: dict = None, warnings: list = None,
                       selected_model: str = None,
                       routing_log_id: str = None) -> str:
    """Registra un prediction run con los datos del forecast engine."""
    import json
    pred_id = str(uuid.uuid4())
    _execute(f"""
        INSERT INTO {SCHEMA}.prediction_runs
        (id, routing_log_id, source_type, dataset_signature, target_variable,
         time_range, feature_set, method, prediction_output, confidence_json)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb, %s::jsonb)
    """, (pred_id, routing_log_id, 'sql', f'{target_variable}_{dataset_size}pts',
          target_variable,
          json.dumps({"horizon": horizon, "dataset_size": dataset_size}),
          json.dumps({"warnings": warnings or []}),
          method,
          json.dumps({"prediction": prediction_value, "horizon": horizon,
                      "model": selected_model}),
          json.dumps({"confidence": confidence, "backtesting": backtesting or {},
                      "method": method})), fetch=False)
    return pred_id


def get_prediction_runs(limit: int = 20) -> list[dict]:
    rows = _execute(f"""
        SELECT pr.*, mrl.query_preview, mrl.selected_model
        FROM {SCHEMA}.prediction_runs pr
        LEFT JOIN {SCHEMA}.model_routing_logs mrl ON pr.routing_log_id = mrl.id
        ORDER BY pr.created_at DESC LIMIT %s
    """, (limit,))
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
# Métricas agregadas (recalculadas periódicamente)
# ─────────────────────────────────────────────

def recalculate_model_metrics():
    """Recalcula métricas de model_registry a partir de logs de últimos 7 días."""
    _execute(f"""
        WITH stats AS (
            SELECT
                selected_model,
                AVG(latency_ms) FILTER (WHERE latency_ms IS NOT NULL) AS avg_lat,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)
                    FILTER (WHERE latency_ms IS NOT NULL) AS p95_lat,
                COUNT(*) FILTER (WHERE error IS NULL)::REAL / NULLIF(COUNT(*), 0) AS success_rate,
                COUNT(*) AS total_req
            FROM {SCHEMA}.model_routing_logs
            WHERE created_at > now() - INTERVAL '7 days'
            GROUP BY selected_model
        ),
        fb AS (
            SELECT
                mrl.selected_model,
                AVG(rf.rating)::REAL AS avg_fb
            FROM {SCHEMA}.response_feedback rf
            JOIN {SCHEMA}.model_routing_logs mrl ON rf.routing_log_id = mrl.id
            WHERE rf.created_at > now() - INTERVAL '7 days'
            GROUP BY mrl.selected_model
        )
        UPDATE {SCHEMA}.model_registry mr SET
            avg_latency_ms = s.avg_lat::INTEGER,
            p95_latency_ms = s.p95_lat::INTEGER,
            success_rate_7d = s.success_rate,
            total_requests_7d = s.total_req,
            avg_feedback_score = COALESCE(fb.avg_fb, mr.avg_feedback_score),
            updated_at = now()
        FROM stats s
        LEFT JOIN fb ON fb.selected_model = s.selected_model
        WHERE mr.model_id = s.selected_model
    """, fetch=False)
    _cache.invalidate("active_models")


def recalculate_pipeline_metrics():
    """Recalcula métricas por cruce modelo+pipeline."""
    _execute(f"""
        WITH stats AS (
            SELECT
                mrl.selected_model,
                mrl.pipeline_id,
                mr.id AS model_uuid,
                AVG(mrl.latency_ms) FILTER (WHERE mrl.latency_ms IS NOT NULL)::INTEGER AS avg_lat,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY mrl.latency_ms)
                    FILTER (WHERE mrl.latency_ms IS NOT NULL)::INTEGER AS p95_lat,
                COUNT(*) FILTER (WHERE mrl.error IS NULL)::REAL / NULLIF(COUNT(*), 0) AS sr,
                AVG(mrl.cost_actual) FILTER (WHERE mrl.cost_actual IS NOT NULL) AS avg_cost,
                COUNT(*) AS cnt
            FROM {SCHEMA}.model_routing_logs mrl
            JOIN {SCHEMA}.model_registry mr ON mr.model_id = mrl.selected_model
            WHERE mrl.created_at > now() - INTERVAL '7 days'
              AND mrl.pipeline_id IS NOT NULL
            GROUP BY mrl.selected_model, mrl.pipeline_id, mr.id
        ),
        fb AS (
            SELECT mrl.selected_model, mrl.pipeline_id, AVG(rf.rating)::REAL AS avg_fb
            FROM {SCHEMA}.response_feedback rf
            JOIN {SCHEMA}.model_routing_logs mrl ON rf.routing_log_id = mrl.id
            WHERE rf.created_at > now() - INTERVAL '7 days' AND mrl.pipeline_id IS NOT NULL
            GROUP BY mrl.selected_model, mrl.pipeline_id
        )
        INSERT INTO {SCHEMA}.model_pipeline_metrics
            (model_id, pipeline_id, avg_latency_ms, p95_latency_ms,
             success_rate_7d, avg_feedback_score, avg_cost_actual, sample_size, updated_at)
        SELECT s.model_uuid, s.pipeline_id, s.avg_lat, s.p95_lat,
               s.sr, COALESCE(fb.avg_fb, 3.0), s.avg_cost, s.cnt, now()
        FROM stats s
        LEFT JOIN fb ON fb.selected_model = s.selected_model AND fb.pipeline_id = s.pipeline_id
        ON CONFLICT (model_id, pipeline_id) DO UPDATE SET
            avg_latency_ms = EXCLUDED.avg_latency_ms,
            p95_latency_ms = EXCLUDED.p95_latency_ms,
            success_rate_7d = EXCLUDED.success_rate_7d,
            avg_feedback_score = EXCLUDED.avg_feedback_score,
            avg_cost_actual = EXCLUDED.avg_cost_actual,
            sample_size = EXCLUDED.sample_size,
            updated_at = now()
    """, fetch=False)


# ─────────────────────────────────────────────
# Preload completo de caché
# ─────────────────────────────────────────────

def log_pipeline_execution(execution_log):
    """
    Registra la ejecución de un pipeline con trazabilidad granular.
    Crea la tabla si no existe. Se ejecuta SIEMPRE (éxito, error, degradación).
    """
    import json as _json
    try:
        _execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.pipeline_execution_logs (
                id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                pipeline_selected TEXT NOT NULL,
                pipeline_executed TEXT NOT NULL,
                forecast_engine_executed BOOLEAN DEFAULT FALSE,
                feasibility_check_executed BOOLEAN DEFAULT FALSE,
                retrieval_executed BOOLEAN DEFAULT FALSE,
                sql_executed BOOLEAN DEFAULT FALSE,
                llm_only_response BOOLEAN DEFAULT FALSE,
                degraded_from TEXT,
                degraded_to TEXT,
                structured_result_type TEXT,
                tool_executions JSONB DEFAULT '[]',
                total_duration_ms INTEGER,
                error TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            )
        """, fetch=False)
    except Exception:
        pass  # tabla ya existe

    tools_json = _json.dumps([
        {
            "tool": t.tool_name,
            "success": t.success,
            "duration_ms": t.duration_ms,
            "summary": t.result_summary,
            "error": t.error,
            "input_preview": t.input_preview,
        }
        for t in execution_log.tool_executions
    ])

    _execute(f"""
        INSERT INTO {SCHEMA}.pipeline_execution_logs
            (pipeline_selected, pipeline_executed,
             forecast_engine_executed, feasibility_check_executed,
             retrieval_executed, sql_executed, llm_only_response,
             degraded_from, degraded_to, structured_result_type,
             tool_executions, total_duration_ms, error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        execution_log.pipeline_selected,
        execution_log.pipeline_executed,
        execution_log.forecast_engine_executed,
        execution_log.feasibility_check_executed,
        execution_log.retrieval_executed,
        execution_log.sql_executed,
        execution_log.llm_only_response,
        execution_log.degraded_from,
        execution_log.degraded_to,
        execution_log.structured_result_type,
        tools_json,
        execution_log.total_duration_ms,
        getattr(execution_log, 'error', None),
    ), fetch=False)


def get_pipeline_execution_logs(limit: int = 50) -> list:
    """Obtiene los últimos logs de ejecución de pipelines."""
    return _execute(f"""
        SELECT * FROM {SCHEMA}.pipeline_execution_logs
        ORDER BY created_at DESC LIMIT %s
    """, (limit,))


def preload_cache():
    """Carga todos los datos necesarios para routing en caché.
    Llamar al arrancar la app y tras sync manual."""
    print("🔄 Precargando caché de rag_engine...")
    get_active_models(use_cache=False)
    get_pipelines(use_cache=False)
    get_routing_rules(use_cache=False)
    get_fallback_chains(use_cache=False)
    print(f"✅ Caché precargada: {len(get_active_models())} modelos, "
          f"{len(get_pipelines())} pipelines, "
          f"{len(get_routing_rules())} reglas")
