"""
llm/response_renderer.py — Construye prompts restringidos y renderiza respuestas V3.0
Cada pipeline tiene una plantilla FORZADA. El LLM solo explica/redacta — nunca calcula.
"""
from llm.result_contracts import (
    OrchestratedResult, DirectChatResult, DocRetrievalResult,
    AgenticSQLResult, AgenticSQLRAGResult, PredictiveInsightResult,
    PredictiveForecastResult, is_llm_only_allowed,
)

# ─────────────────────────────────────────────
# Reglas anti-alucinación por pipeline
# ─────────────────────────────────────────────

_GLOBAL_CONSTRAINTS = """REGLAS OBLIGATORIAS:
- Responde SIEMPRE en español.
- No calcules predicciones por tu cuenta.
- No inventes confianza ni porcentajes.
- No deduzcas resultados que no estén presentes en los datos proporcionados.
- Si falta un dato, explica la limitación — no lo inventes.
- Sé conciso y directo. Usa listas o tablas cuando haya muchos datos.
- NUNCA hagas preguntas al usuario ni termines con "¿Te gustaría...?"."""

_PIPELINE_CONSTRAINTS = {
    "direct_chat": (
        "Eres un asistente conversacional. Responde de forma directa y útil. "
        "No tienes acceso a herramientas en este modo."
    ),
    "doc_retrieval": (
        "RESTRICCIÓN: Solo puedes responder usando los fragmentos documentales proporcionados abajo.\n"
        "PROHIBIDO: Inventar información que no esté en los fragmentos.\n"
        "OBLIGATORIO: Cita el nombre del archivo y la página de cada dato que uses.\n"
        "Si ningún fragmento es relevante, di claramente que no se encontró evidencia documental."
    ),
    "agentic_sql": (
        "RESTRICCIÓN: Solo puedes responder usando los resultados SQL proporcionados abajo.\n"
        "PROHIBIDO: Inventar cifras que no estén en los resultados de las consultas.\n"
        "OBLIGATORIO: Basa tu respuesta exclusivamente en los datos devueltos por SQL.\n"
        "Si la consulta no devolvió resultados, explica que no hay datos disponibles."
    ),
    "agentic_sql_rag": (
        "RESTRICCIÓN: Solo puedes responder usando los resultados SQL Y los fragmentos documentales proporcionados.\n"
        "PROHIBIDO: Inventar cifras o información no presentes en los datos.\n"
        "OBLIGATORIO: Distingue entre datos de la BD y datos de documentos.\n"
        "Si una fuente no tiene datos, menciónalo."
    ),
    "predictive_insight": (
        "RESTRICCIÓN: Este es un análisis DESCRIPTIVO, NO una predicción cuantitativa.\n"
        "PROHIBIDO: Dar cifras de predicción, rangos futuros o porcentajes de probabilidad.\n"
        "OBLIGATORIO: Describe tendencias, patrones y anomalías basándote en los datos proporcionados.\n"
        "Si los datos son insuficientes, di claramente que no hay base suficiente para conclusiones fuertes."
    ),
    "predictive_forecast": (
        "RESTRICCIÓN: Los datos de predicción han sido calculados por el motor de forecast del sistema.\n"
        "PROHIBIDO: Recalcular, ajustar o inventar una predicción diferente.\n"
        "PROHIBIDO: Modificar el porcentaje de confianza.\n"
        "OBLIGATORIO: Presenta la predicción, el método, la confianza, los datos usados y las advertencias "
        "exactamente como los proporciona el sistema.\n"
        "Tu único trabajo es EXPLICAR estos datos al usuario de forma clara y profesional."
    ),
}


# ─────────────────────────────────────────────
# Plantillas de render por contrato
# ─────────────────────────────────────────────

def _render_direct_chat(result: DirectChatResult) -> str:
    """Direct chat: prompt mínimo, el LLM genera libremente."""
    return ""  # sin datos previos — respuesta libre


def _render_doc_retrieval(result: DocRetrievalResult) -> str:
    """Inyecta chunks documentales en el prompt."""
    if not result.has_evidence:
        return (
            "\n\n--- RESULTADO DE BÚSQUEDA DOCUMENTAL ---\n"
            "No se encontraron documentos relevantes para esta consulta.\n"
            "Informa al usuario de que no hay evidencia documental disponible.\n"
            "--- FIN RESULTADO DOCUMENTAL ---"
        )

    lines = ["\n\n--- FRAGMENTOS DOCUMENTALES RECUPERADOS ---"]
    for i, chunk in enumerate(result.retrieved_chunks, 1):
        source = chunk.get("source", "?")
        page = chunk.get("page", "?")
        score = chunk.get("score", 0)
        text = chunk.get("text", "")
        lines.append(f"\n[Fragmento {i} — {source}, pág {page}, relevancia {score:.2f}]")
        lines.append(text)

    lines.append("\n--- FIN FRAGMENTOS DOCUMENTALES ---")
    lines.append("\nINSTRUCCIÓN: Responde usando SOLO estos fragmentos. Cita archivo y página.")
    return "\n".join(lines)


def _render_agentic_sql(result: AgenticSQLResult) -> str:
    """Inyecta resultados SQL ejecutados en el prompt."""
    if not result.has_data:
        return (
            "\n\n--- RESULTADOS SQL ---\n"
            "Las consultas SQL no devolvieron datos.\n"
            "Informa al usuario de que no hay datos disponibles para su consulta.\n"
            "--- FIN RESULTADOS SQL ---"
        )

    lines = ["\n\n--- RESULTADOS DE CONSULTAS SQL EJECUTADAS ---"]
    for q in result.sql_queries:
        lines.append(f"\nConsulta: {q.get('description', 'N/A')}")
        lines.append(f"SQL: {q.get('sql', 'N/A')}")
        lines.append(f"Filas: {q.get('rows_count', 0)}")

    if result.structured_findings:
        lines.append(f"\nDatos obtenidos:\n{result.structured_findings}")

    lines.append("\n--- FIN RESULTADOS SQL ---")
    lines.append("\nINSTRUCCIÓN: Explica estos resultados al usuario. NO inventes cifras adicionales.")
    return "\n".join(lines)


def _render_agentic_sql_rag(result: AgenticSQLRAGResult) -> str:
    """Combina resultados SQL y RAG."""
    parts = ["\n\n--- RESULTADOS COMBINADOS (SQL + DOCUMENTOS) ---"]

    if result.sql_result:
        parts.append("\n=== DATOS DE BASE DE DATOS ===")
        parts.append(_render_agentic_sql(result.sql_result))

    if result.rag_result:
        parts.append("\n=== DATOS DE DOCUMENTOS ===")
        parts.append(_render_doc_retrieval(result.rag_result))

    if result.merged_findings:
        parts.append(f"\nHallazgos combinados:\n{result.merged_findings}")

    parts.append("\n--- FIN RESULTADOS COMBINADOS ---")
    parts.append("\nINSTRUCCIÓN: Integra ambas fuentes en tu respuesta. Distingue datos SQL de documentales.")
    return "\n".join(parts)


def _render_predictive_insight(result: PredictiveInsightResult) -> str:
    """Análisis descriptivo sin predicción cuantitativa."""
    lines = ["\n\n--- ANÁLISIS DESCRIPTIVO DEL SISTEMA ---"]

    if result.degraded_from_forecast:
        lines.append(f"NOTA: Se degradó desde predictive_forecast: {result.degrade_reason}")

    if result.descriptive_analysis:
        lines.append(f"\nAnálisis:\n{result.descriptive_analysis}")

    if result.trend_summary:
        lines.append(f"\nTendencias:\n{result.trend_summary}")

    if result.data_points:
        lines.append("\nDatos históricos:")
        for dp in result.data_points:
            lines.append(f"  {dp.get('period', '?')}: {dp.get('value', '?')}")

    if result.warnings:
        lines.append("\nAdvertencias:")
        for w in result.warnings:
            lines.append(f"  - {w}")

    lines.append("\n--- FIN ANÁLISIS DESCRIPTIVO ---")
    lines.append(
        "\nINSTRUCCIÓN: Presenta este análisis descriptivo. "
        "NO hagas predicciones cuantitativas. Solo describe tendencias y patrones."
    )
    return "\n".join(lines)


def _render_predictive_forecast(result: PredictiveForecastResult) -> str:
    """Predicción cuantitativa del forecast engine."""
    lines = [
        "\n\n--- DATOS DE PREDICCIÓN CUANTITATIVA (calculados por el sistema) ---",
        f"Variable: {result.target_display}",
        f"Horizonte: {result.horizon}",
        f"Método: {result.method_display}",
        f"Razón de elección: {result.method_selection_reason}",
        f"PREDICCIÓN: {result.prediction:.2f}",
        f"Confianza: {result.confidence:.0%}",
        f"Dataset: {result.dataset_size} periodos",
        f"Backtesting MAPE: {result.backtesting.get('MAPE', 'N/A')}",
        f"Tipo validación: {result.backtesting.get('validation_type', 'N/A')}",
        f"\nDatos históricos usados:",
    ]
    for dp in result.data_used:
        lines.append(f"  {dp['period']}: {dp['value']}")

    if result.warnings:
        lines.append("\nAdvertencias:")
        for w in result.warnings:
            lines.append(f"  - {w}")

    lines.append("\n--- FIN DATOS DE PREDICCIÓN ---")
    lines.append(
        "\nINSTRUCCIÓN: Explica esta predicción al usuario de forma clara y profesional.\n"
        "Incluye OBLIGATORIAMENTE: predicción, confianza, método, datos usados, y advertencias.\n"
        "NO inventes datos adicionales. Usa SOLO los datos proporcionados arriba.\n"
        "NO recalcules la confianza ni modifiques la predicción."
    )
    return "\n".join(lines)


# Mapeo de tipo de resultado → función de render
_RENDER_MAP = {
    "direct_chat": _render_direct_chat,
    "doc_retrieval": _render_doc_retrieval,
    "agentic_sql": _render_agentic_sql,
    "agentic_sql_rag": _render_agentic_sql_rag,
    "predictive_insight": _render_predictive_insight,
    "predictive_forecast": _render_predictive_forecast,
}


# ─────────────────────────────────────────────
# API pública
# ─────────────────────────────────────────────

def build_constrained_prompt(orch_result: OrchestratedResult, original_query: str) -> str:
    """
    Construye el prompt completo para el LLM:
    query original + restricciones del pipeline + datos estructurados.
    """
    pipeline = orch_result.pipeline_executed
    constraints = _PIPELINE_CONSTRAINTS.get(pipeline, "")

    # Renderizar datos del resultado
    sr = orch_result.structured_result
    render_fn = _RENDER_MAP.get(orch_result.result_type, lambda _: "")
    rendered_data = render_fn(sr) if sr else ""

    # Construir prompt final
    parts = [original_query]

    if rendered_data:
        parts.append(rendered_data)

    parts.append(f"\n\n{_GLOBAL_CONSTRAINTS}")
    parts.append(f"\n{constraints}")

    return "\n".join(parts)


def render_structured_metadata(orch_result: OrchestratedResult) -> dict:
    """
    Genera metadata estructurada para el frontend/logs.
    """
    log = orch_result.execution_log
    meta = {
        "pipeline_selected": log.pipeline_selected,
        "pipeline_executed": log.pipeline_executed,
        "result_type": orch_result.result_type,
        "allow_llm_only": orch_result.allow_llm_only,
        "is_critical": orch_result.is_critical,
        "degraded_from": log.degraded_from,
        "degraded_to": log.degraded_to,
        "engines_used": {
            "forecast": log.forecast_engine_executed,
            "feasibility": log.feasibility_check_executed,
            "retrieval": log.retrieval_executed,
            "sql": log.sql_executed,
            "llm_only": log.llm_only_response,
        },
        "tool_executions": [
            {
                "tool": t.tool_name,
                "success": t.success,
                "duration_ms": t.duration_ms,
                "summary": t.result_summary,
            }
            for t in log.tool_executions
        ],
        "total_duration_ms": log.total_duration_ms,
    }

    # Añadir datos específicos del resultado
    sr = orch_result.structured_result
    if isinstance(sr, PredictiveForecastResult):
        meta["forecast"] = {
            "prediction": sr.prediction,
            "confidence": sr.confidence,
            "method": sr.method_display,
            "dataset_size": sr.dataset_size,
        }
    elif isinstance(sr, DocRetrievalResult):
        meta["retrieval"] = {
            "chunks_count": len(sr.retrieved_chunks),
            "sources_count": len(sr.sources),
            "has_evidence": sr.has_evidence,
        }
    elif isinstance(sr, AgenticSQLResult):
        meta["sql"] = {
            "queries_count": len(sr.sql_queries),
            "rows_returned": sr.rows_returned,
            "has_data": sr.has_data,
        }

    return meta
