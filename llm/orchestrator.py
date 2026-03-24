"""
llm/orchestrator.py — Orquestador central estricto V3.0
Flujo: classify → select_pipeline → plan → execute_pipeline → structured_result → prompt
El LLM nunca calcula — solo explica/redacta el resultado del sistema.
"""
import time
from typing import Optional
from datetime import datetime as dt

from llm.task_classifier import classify as classify_task, TaskClassification
from llm.pipeline_selector import select_pipeline, degrade_pipeline, PipelineSelection
from llm.execution_planner import create_execution_plan, ExecutionPlan
from llm.model_router import route as route_model
from llm.result_contracts import (
    OrchestratedResult, ExecutionLog, ToolExecution,
    DirectChatResult, DocRetrievalResult, AgenticSQLResult,
    AgenticSQLRAGResult, PredictiveInsightResult, PredictiveForecastResult,
    is_llm_only_allowed,
)
from llm.response_renderer import build_constrained_prompt
from llm.prediction_feasibility import (
    check_feasibility, extract_target_variable, FeasibilityResult
)
from llm.forecast_engine import (
    forecast as run_forecast, select_method as select_forecast_method,
    VARIABLE_DISPLAY
)
from retrieval.search import search as retrieval_search
from db import connector as db
from db import model_db as mdb


# SQL templates para datos históricos de forecast
_FORECAST_SQL_TEMPLATES = {
    "litros_gasoil": """
        SELECT DATE_TRUNC('month', jl."fecha") AS periodo,
               SUM(COALESCE(uc."litrosRepostados",0)) AS valor
        FROM "JornadaLaboral" jl
        LEFT JOIN "UsoCamion" uc ON uc."jornadaId" = jl.id
        WHERE jl."fecha" >= NOW() - INTERVAL '24 months'
        GROUP BY periodo ORDER BY periodo
    """,
    "km_totales": """
        SELECT DATE_TRUNC('month', jl."fecha") AS periodo,
               SUM(COALESCE(uc."kmRecorridos",0)) AS valor
        FROM "JornadaLaboral" jl
        LEFT JOIN "UsoCamion" uc ON uc."jornadaId" = jl.id
        WHERE jl."fecha" >= NOW() - INTERVAL '24 months'
        GROUP BY periodo ORDER BY periodo
    """,
    "horas_trabajadas": """
        SELECT DATE_TRUNC('month', "fecha") AS periodo,
               SUM(COALESCE("totalHoras",0)) AS valor
        FROM "JornadaLaboral"
        WHERE "fecha" >= NOW() - INTERVAL '24 months'
        GROUP BY periodo ORDER BY periodo
    """,
    "descargas_totales": """
        SELECT DATE_TRUNC('month', jl."fecha") AS periodo,
               SUM(COALESCE(uc."descargasCount",0)) AS valor
        FROM "JornadaLaboral" jl
        LEFT JOIN "UsoCamion" uc ON uc."jornadaId" = jl.id
        WHERE jl."fecha" >= NOW() - INTERVAL '24 months'
        GROUP BY periodo ORDER BY periodo
    """,
    "coste_mantenimiento": """
        SELECT DATE_TRUNC('month', "fecha") AS periodo,
               SUM(COALESCE("costo",0)) AS valor
        FROM "MantenimientoRealizado"
        WHERE "fecha" >= NOW() - INTERVAL '24 months'
        GROUP BY periodo ORDER BY periodo
    """,
    "dias_ausencia": """
        SELECT DATE_TRUNC('month', "fechaInicio") AS periodo,
               COUNT(*) AS valor
        FROM "Ausencia"
        WHERE "fechaInicio" >= NOW() - INTERVAL '24 months'
          AND "estado" = 'APROBADA'
        GROUP BY periodo ORDER BY periodo
    """,
}

# Aliases
_FORECAST_SQL_TEMPLATES["gasto_total"] = _FORECAST_SQL_TEMPLATES["litros_gasoil"]
_FORECAST_SQL_TEMPLATES["coste_total"] = _FORECAST_SQL_TEMPLATES["coste_mantenimiento"]
_FORECAST_SQL_TEMPLATES["num_averias"] = _FORECAST_SQL_TEMPLATES["coste_mantenimiento"]
_FORECAST_SQL_TEMPLATES["km_por_hora"] = _FORECAST_SQL_TEMPLATES["km_totales"]
_FORECAST_SQL_TEMPLATES["km_por_litro"] = _FORECAST_SQL_TEMPLATES["km_totales"]


# ─────────────────────────────────────────────
# Ejecutores por pipeline
# ─────────────────────────────────────────────

def _run_direct_chat(query: str, plan: ExecutionPlan,
                     log: ExecutionLog, tenant_ctx=None) -> DirectChatResult:
    """Pipeline direct_chat: no ejecuta herramientas."""
    log.llm_only_response = True
    log.structured_result_type = "direct_chat"
    return DirectChatResult(
        notes=["Respuesta conversacional directa"],
        confidence=1.0,
    )


def _run_doc_retrieval(query: str, plan: ExecutionPlan,
                       log: ExecutionLog, tenant_ctx=None) -> DocRetrievalResult:
    """Pipeline doc_retrieval: ejecuta retrieval real ANTES del LLM."""
    start = time.time()
    result = DocRetrievalResult()

    # Reescritura de query para retrieval
    rewritten = _rewrite_query_for_retrieval(query)
    result.rewritten_query = rewritten

    # Extraer tenant_id si hay contexto multi-tenant
    _tid = tenant_ctx.tenant_id if tenant_ctx else None

    try:
        chunks = retrieval_search(rewritten, tenant_id=_tid)
        duration = int((time.time() - start) * 1000)

        log.retrieval_executed = True
        log.add_tool(ToolExecution(
            tool_name="retrieval_search",
            success=True,
            duration_ms=duration,
            result_summary=f"{len(chunks)} chunks recuperados",
            input_preview=rewritten[:200],
        ))

        result.retrieved_chunks = chunks
        result.sources = list({
            (c["source"], c["page"]): {"source": c["source"], "page": c["page"], "score": c.get("score", 0)}
            for c in chunks
        }.values())

        # Construir answer_basis (texto base para el LLM)
        basis_parts = []
        for c in chunks[:10]:  # máx 10 chunks
            basis_parts.append(f"[{c.get('source', '?')}, p{c.get('page', '?')}] {c.get('text', '')}")
        result.answer_basis = "\n---\n".join(basis_parts)
        result.confidence = min(1.0, sum(c.get("score", 0) for c in chunks[:5]) / 5) if chunks else 0

    except Exception as e:
        duration = int((time.time() - start) * 1000)
        log.add_tool(ToolExecution(
            tool_name="retrieval_search",
            success=False,
            duration_ms=duration,
            result_summary=f"Error: {str(e)[:100]}",
            error=str(e),
        ))

    log.structured_result_type = "doc_retrieval"
    return result


def _run_agentic_sql_precheck(query: str, plan: ExecutionPlan,
                              log: ExecutionLog, tenant_ctx=None) -> AgenticSQLResult:
    """
    Pipeline agentic_sql: NO ejecuta SQL aquí (lo hace el LLM vía function calling).
    Prepara el resultado estructurado que se llenará durante la ejecución del LLM.
    Marca que SQL es REQUERIDO.
    """
    log.sql_executed = False  # se actualizará en generate.py cuando se ejecute SQL real
    log.structured_result_type = "agentic_sql"
    return AgenticSQLResult(
        confidence=0.0,  # se actualizará post-ejecución
    )


def _run_agentic_sql_rag_precheck(query: str, plan: ExecutionPlan,
                                   log: ExecutionLog, tenant_ctx=None) -> AgenticSQLRAGResult:
    """
    Pipeline agentic_sql_rag: ejecuta retrieval ANTES del LLM,
    pero SQL se ejecuta durante function calling.
    """
    # Ejecutar RAG primero (con tenant_ctx para Qdrant)
    rag_result = _run_doc_retrieval(query, plan, log, tenant_ctx=tenant_ctx)

    # SQL se ejecutará durante function calling
    sql_result = AgenticSQLResult(confidence=0.0)

    log.structured_result_type = "agentic_sql_rag"
    return AgenticSQLRAGResult(
        sql_result=sql_result,
        rag_result=rag_result,
        confidence=0.0,
    )


def _run_predictive_insight(query: str, plan: ExecutionPlan,
                            log: ExecutionLog,
                            degrade_reason: str = "",
                            tenant_ctx=None) -> PredictiveInsightResult:
    """Pipeline predictive_insight: análisis descriptivo sin predicción cuantitativa."""
    result = PredictiveInsightResult(
        degraded_from_forecast=(degrade_reason != ""),
        degrade_reason=degrade_reason,
    )

    # Extraer database_url del tenant si disponible
    _db_url = tenant_ctx.database_url if tenant_ctx else None

    # Intentar obtener datos históricos para análisis descriptivo
    target_var, horizon_label, horizon_periods = extract_target_variable(query)

    if target_var != "valor_desconocido":
        sql_template = _FORECAST_SQL_TEMPLATES.get(target_var)
        if sql_template:
            start = time.time()
            try:
                rows = db.run_safe_query(sql_template, database_url=_db_url)
                duration = int((time.time() - start) * 1000)
                log.sql_executed = True
                log.add_tool(ToolExecution(
                    tool_name="forecast_sql_query",
                    success=True,
                    duration_ms=duration,
                    result_summary=f"{len(rows)} filas para {target_var}",
                    input_preview=sql_template[:200],
                ))

                if rows:
                    for row in rows:
                        periodo = row.get("periodo")
                        valor = row.get("valor", 0)
                        if periodo:
                            label = periodo.strftime("%Y-%m") if isinstance(periodo, dt) else str(periodo)
                            result.data_points.append({"period": label, "value": float(valor or 0)})

                    values = [dp["value"] for dp in result.data_points]
                    if len(values) >= 2:
                        trend = "ascendente" if values[-1] > values[0] else "descendente" if values[-1] < values[0] else "estable"
                        avg_val = sum(values) / len(values)
                        result.trend_summary = (
                            f"Variable: {VARIABLE_DISPLAY.get(target_var, target_var)}. "
                            f"Tendencia {trend} en {len(values)} periodos. "
                            f"Media: {avg_val:.1f}. "
                            f"Rango: [{min(values):.1f} — {max(values):.1f}]."
                        )
                        result.descriptive_analysis = (
                            f"Se analizaron {len(values)} periodos de {VARIABLE_DISPLAY.get(target_var, target_var)}. "
                            f"El valor más reciente es {values[-1]:.1f}."
                        )
                        result.confidence = 0.6 if len(values) >= 6 else 0.3

            except Exception as e:
                duration = int((time.time() - start) * 1000)
                log.add_tool(ToolExecution(
                    tool_name="forecast_sql_query",
                    success=False,
                    duration_ms=duration,
                    result_summary=f"Error: {str(e)[:100]}",
                    error=str(e),
                ))
                result.warnings.append(f"Error al obtener datos: {str(e)[:100]}")

    if not result.data_points:
        result.warnings.append("No se pudieron obtener datos históricos para análisis descriptivo")
        result.confidence = 0.1

    log.structured_result_type = "predictive_insight"
    return result


def _run_predictive_forecast(query: str, plan: ExecutionPlan,
                             log: ExecutionLog, pipeline_sel: PipelineSelection,
                             tenant_ctx=None
                             ) -> tuple[Optional[PredictiveForecastResult], Optional[PredictiveInsightResult], Optional[str]]:
    """
    Pipeline predictive_forecast: ejecución completa del motor de predicción.
    Returns:
        (ForecastResult, None, None) si exitoso
        (None, InsightResult, degrade_reason) si degradado
    """
    # Extraer database_url del tenant
    _db_url = tenant_ctx.database_url if tenant_ctx else None

    # 1. Extraer variable y horizonte
    start = time.time()
    target_var, horizon_label, horizon_periods = extract_target_variable(query)
    log.add_tool(ToolExecution(
        tool_name="extract_target_variable",
        success=target_var != "valor_desconocido",
        duration_ms=int((time.time() - start) * 1000),
        result_summary=f"target={target_var}, horizon={horizon_label}, periods={horizon_periods}",
        input_preview=query[:200],
    ))

    if target_var == "valor_desconocido":
        reason = "Variable objetivo no identificada"
        insight = _run_predictive_insight(query, plan, log, degrade_reason=reason, tenant_ctx=tenant_ctx)
        return None, insight, reason

    # 2. Obtener datos históricos
    sql_template = _FORECAST_SQL_TEMPLATES.get(target_var)
    if not sql_template:
        reason = f"Sin template SQL para {target_var}"
        insight = _run_predictive_insight(query, plan, log, degrade_reason=reason, tenant_ctx=tenant_ctx)
        return None, insight, reason

    start = time.time()
    try:
        rows = db.run_safe_query(sql_template, database_url=_db_url)
        duration = int((time.time() - start) * 1000)
        log.sql_executed = True
        log.add_tool(ToolExecution(
            tool_name="forecast_sql_query",
            success=True,
            duration_ms=duration,
            result_summary=f"{len(rows)} filas para {target_var}",
            input_preview=sql_template[:200],
        ))
    except Exception as e:
        duration = int((time.time() - start) * 1000)
        log.add_tool(ToolExecution(
            tool_name="forecast_sql_query",
            success=False,
            duration_ms=duration,
            result_summary=f"Error SQL: {str(e)[:100]}",
            error=str(e),
        ))
        reason = f"Error SQL: {str(e)[:100]}"
        insight = _run_predictive_insight(query, plan, log, degrade_reason=reason, tenant_ctx=tenant_ctx)
        return None, insight, reason

    if not rows:
        reason = "La consulta no devolvió datos históricos"
        insight = _run_predictive_insight(query, plan, log, degrade_reason=reason, tenant_ctx=tenant_ctx)
        return None, insight, reason

    # Parsear datos
    values, dates, period_labels = [], [], []
    for row in rows:
        val = row.get("valor", 0)
        periodo = row.get("periodo")
        if val is not None:
            values.append(float(val))
            if isinstance(periodo, dt):
                dates.append(periodo)
                period_labels.append(periodo.strftime("%Y-%m"))
            elif isinstance(periodo, str):
                try:
                    d = dt.fromisoformat(periodo.replace("Z", ""))
                    dates.append(d)
                    period_labels.append(d.strftime("%Y-%m"))
                except Exception:
                    dates.append(None)
                    period_labels.append(str(periodo))
            else:
                dates.append(None)
                period_labels.append(str(periodo) if periodo else "?")

    # 3. Feasibility check
    start = time.time()
    feasibility = check_feasibility(
        values=values,
        dates=[d for d in dates if d is not None],
        target_variable=target_var,
        horizon_periods=horizon_periods,
    )
    duration = int((time.time() - start) * 1000)
    log.feasibility_check_executed = True
    log.add_tool(ToolExecution(
        tool_name="prediction_feasibility_check",
        success=feasibility.feasible,
        duration_ms=duration,
        result_summary=(
            f"{'viable' if feasibility.feasible else 'no viable'}: "
            f"{feasibility.reason}. penalty={feasibility.combined_penalty:.2f}"
        ),
    ))

    if not feasibility.feasible:
        reason = feasibility.reason
        insight = _run_predictive_insight(query, plan, log, degrade_reason=reason)
        # Añadir datos históricos al insight
        for val, label in zip(values, period_labels):
            insight.data_points.append({"period": label, "value": val})
        return None, insight, reason

    # 4. Seleccionar método
    start = time.time()
    method, method_reason = select_forecast_method(values)
    duration = int((time.time() - start) * 1000)
    log.add_tool(ToolExecution(
        tool_name="select_forecast_method",
        success=method is not None,
        duration_ms=duration,
        result_summary=f"método={method or 'ninguno'}: {method_reason}",
    ))

    if method is None:
        reason = method_reason
        insight = _run_predictive_insight(query, plan, log, degrade_reason=reason)
        return None, insight, reason

    # 5. Ejecutar forecast
    start = time.time()
    result = run_forecast(
        values=values,
        target_variable=target_var,
        horizon_label=horizon_label,
        method=method,
        method_reason=method_reason,
        feasibility_penalty=feasibility.combined_penalty,
        period_labels=period_labels,
    )
    duration = int((time.time() - start) * 1000)
    log.forecast_engine_executed = True
    log.add_tool(ToolExecution(
        tool_name="forecast_engine",
        success=result is not None,
        duration_ms=duration,
        result_summary=(
            f"predicción={result.prediction:.2f}, confianza={result.confidence:.0%}" if result
            else "sin resultado"
        ),
    ))

    if result is None:
        reason = "El motor de predicción no produjo resultado"
        insight = _run_predictive_insight(query, plan, log, degrade_reason=reason)
        return None, insight, reason

    # 6. Si confianza < 0.4 → degradar
    if result.confidence < 0.4:
        reason = f"Confianza demasiado baja ({result.confidence:.0%}) para predicción cuantitativa"
        insight = _run_predictive_insight(query, plan, log, degrade_reason=reason)
        for dp in result.data_used:
            insight.data_points.append(dp)
        insight.warnings.append(reason)
        return None, insight, reason

    # 7. Registrar en prediction_runs
    try:
        mdb.log_prediction_run(
            query_preview=query[:150],
            target_variable=target_var,
            method=method,
            horizon=horizon_label,
            dataset_size=len(values),
            prediction_value=result.prediction,
            confidence=result.confidence,
            backtesting=result.backtesting,
            warnings=result.warnings,
        )
    except Exception as e:
        print(f"⚠️ Error logging prediction: {e}")

    log.structured_result_type = "predictive_forecast"
    return result, None, None


# ─────────────────────────────────────────────
# Query rewriting para retrieval
# ─────────────────────────────────────────────

def _rewrite_query_for_retrieval(query: str) -> str:
    """
    Reescribe la query del usuario para mejorar la búsqueda documental.
    Transforma preguntas conversacionales en queries documentales.
    """
    import re

    q = query.lower().strip()

    # Reescrituras comunes
    rewrites = [
        (r"^qué dice el (convenio|documento|contrato) sobre (.+)", r"\2 \1"),
        (r"^según el (convenio|documento), (.+)", r"\2"),
        (r"^cuántos días de (.+) tengo", r"días \1 trabajador"),
        (r"^qué derechos tengo .* sobre (.+)", r"\1 derechos trabajador"),
        (r"^puedo (.+) según el convenio", r"\1 convenio colectivo"),
    ]

    for pattern, replacement in rewrites:
        match = re.match(pattern, q, re.IGNORECASE)
        if match:
            return re.sub(pattern, replacement, q, flags=re.IGNORECASE)

    # Si no hay reescritura, devolver query original limpia
    # Quitar palabras interrogativas que no ayudan al retrieval
    noise_words = ["qué", "cuál", "cómo", "cuándo", "cuánto", "dime", "dame", "explica"]
    words = q.split()
    cleaned = [w for w in words if w not in noise_words]
    return " ".join(cleaned) if cleaned else query


# ─────────────────────────────────────────────
# Orquestador central
# ─────────────────────────────────────────────

def run_orchestrated_pipeline(query: str, force_model: str = None,
                              tenant_ctx=None) -> OrchestratedResult:
    """
    Punto de entrada central. Ejecuta el pipeline completo de forma ESTRICTA:
    classify → pipeline → plan → execute → structured_result → prompt.

    tenant_ctx: TenantContext opcionales para multi-tenant.
    Cuando presente, se propaga a búsquedas Qdrant y consultas SQL.
    """
    start_total = time.time()

    # 1. Clasificar
    classification = classify_task(query)
    print(f"📋 Clasificación: {classification.primary_task_family} "
          f"(caps={classification.secondary_capabilities}, "
          f"complexity={classification.complexity}, conf={classification.confidence:.2f})")

    # 2. Seleccionar pipeline
    pipeline_sel = select_pipeline(classification)
    print(f"🔧 Pipeline: {pipeline_sel.pipeline_id} — {pipeline_sel.reason}")

    # 3. Plan de ejecución
    plan = create_execution_plan(classification, pipeline_sel)

    # 4. Inicializar log
    log = ExecutionLog(
        pipeline_selected=pipeline_sel.pipeline_id,
        pipeline_executed=pipeline_sel.pipeline_id,
    )

    # 5. Ejecutar pipeline (propagando tenant_ctx)
    structured_result = None
    degraded_from = None

    pid = pipeline_sel.pipeline_id

    if pid == "direct_chat":
        structured_result = _run_direct_chat(query, plan, log, tenant_ctx=tenant_ctx)

    elif pid == "doc_retrieval":
        structured_result = _run_doc_retrieval(query, plan, log, tenant_ctx=tenant_ctx)

    elif pid == "agentic_sql":
        structured_result = _run_agentic_sql_precheck(query, plan, log, tenant_ctx=tenant_ctx)

    elif pid == "agentic_sql_rag":
        structured_result = _run_agentic_sql_rag_precheck(query, plan, log, tenant_ctx=tenant_ctx)

    elif pid == "predictive_insight":
        structured_result = _run_predictive_insight(query, plan, log, tenant_ctx=tenant_ctx)

    elif pid == "predictive_forecast":
        forecast_result, insight_fallback, degrade_reason = \
            _run_predictive_forecast(query, plan, log, pipeline_sel, tenant_ctx=tenant_ctx)

        if forecast_result:
            structured_result = forecast_result
        elif insight_fallback:
            structured_result = insight_fallback
            degraded_from = "predictive_forecast"
            log.degraded_from = "predictive_forecast"
            log.degraded_to = "predictive_insight"
            log.pipeline_executed = "predictive_insight"
            print(f"⚠️ Degradado a predictive_insight: {degrade_reason}")

    # 6. Construir resultado orquestado
    allow_llm = is_llm_only_allowed(log.pipeline_executed)

    orch = OrchestratedResult(
        pipeline_executed=log.pipeline_executed,
        structured_result=structured_result,
        execution_log=log,
        allow_llm_only=allow_llm,
        degraded_from=degraded_from,
        classification_info={
            "family": classification.primary_task_family,
            "caps": classification.secondary_capabilities,
            "complexity": classification.complexity,
            "predictive_intent": classification.predictive_intent,
        },
    )

    # 7. Validar contrato
    valid, validation_msg = orch.validate()
    if not valid:
        print(f"⚠️ Contrato no cumplido: {validation_msg}")
        # No bloquear, pero registrar

    # 8. Construir prompt restringido para el LLM
    orch.prompt_for_llm = build_constrained_prompt(orch, query)

    log.total_duration_ms = int((time.time() - start_total) * 1000)

    return orch
