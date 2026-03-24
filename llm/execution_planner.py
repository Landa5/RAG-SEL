"""
llm/execution_planner.py — Planes de ejecución PRESCRIPTIVOS V3.0
Define qué DEBE ejecutarse, qué herramientas son obligatorias,
qué contrato de salida se espera, y si se permite solo-LLM.
"""
from dataclasses import dataclass, field
from typing import Optional

from llm.task_classifier import TaskClassification
from llm.pipeline_selector import PipelineSelection, degrade_pipeline


@dataclass
class DataSource:
    """Una fuente de datos para el plan de ejecución."""
    source_type: str  # 'sql', 'rag', 'forecast', 'context'
    description: str
    required: bool = True


@dataclass
class ExecutionPlan:
    """Plan ejecutable PRESCRIPTIVO generado por el planner."""
    pipeline_id: str
    data_sources: list[DataSource]
    needs_sql_query: bool
    needs_doc_retrieval: bool
    needs_forecast_engine: bool
    output_format: str              # 'text', 'json', 'structured'
    pre_validations: list[str]      # validaciones antes de ejecutar
    can_degrade: bool               # si puede degradar
    degrade_to: Optional[str]       # pipeline de degradación
    degraded_from: Optional[str]
    execution_notes: list[str]      # notas para el executor

    # ── Campos prescriptivos V3.0 ──
    required_tools: list[str] = field(default_factory=list)
    required_steps: list[str] = field(default_factory=list)
    fallback_pipeline: Optional[str] = None
    result_contract: str = ""       # tipo de resultado esperado
    allow_llm_only: bool = False    # si puede responder sin resultado estructurado


def create_execution_plan(classification: TaskClassification,
                          pipeline: PipelineSelection) -> ExecutionPlan:
    """Genera un plan de ejecución concreto y prescriptivo."""
    pid = pipeline.pipeline_id

    if pid == "direct_chat":
        return _plan_direct_chat(classification, pipeline)
    elif pid == "doc_retrieval":
        return _plan_doc_retrieval(classification, pipeline)
    elif pid == "agentic_sql":
        return _plan_agentic_sql(classification, pipeline)
    elif pid == "agentic_sql_rag":
        return _plan_agentic_sql_rag(classification, pipeline)
    elif pid == "predictive_insight":
        return _plan_predictive_insight(classification, pipeline)
    elif pid == "predictive_forecast":
        return _plan_predictive_forecast(classification, pipeline)
    else:
        return _plan_direct_chat(classification, pipeline)


def _plan_direct_chat(tc: TaskClassification, ps: PipelineSelection) -> ExecutionPlan:
    return ExecutionPlan(
        pipeline_id="direct_chat",
        data_sources=[],
        needs_sql_query=False,
        needs_doc_retrieval=False,
        needs_forecast_engine=False,
        output_format="text",
        pre_validations=[],
        can_degrade=False,
        degrade_to=None,
        degraded_from=ps.degraded_from,
        execution_notes=["Respuesta directa del LLM sin herramientas"],
        required_tools=[],
        required_steps=[],
        fallback_pipeline=None,
        result_contract="DirectChatResult",
        allow_llm_only=True,   # ÚNICO pipeline que lo permite
    )


def _plan_doc_retrieval(tc: TaskClassification, ps: PipelineSelection) -> ExecutionPlan:
    return ExecutionPlan(
        pipeline_id="doc_retrieval",
        data_sources=[
            DataSource("rag", "Búsqueda semántica + rerank en documentos indexados"),
        ],
        needs_sql_query=False,
        needs_doc_retrieval=True,
        needs_forecast_engine=False,
        output_format="text",
        pre_validations=[
            "Verificar que hay documentos indexados en Qdrant",
        ],
        can_degrade=True,
        degrade_to="direct_chat",
        degraded_from=ps.degraded_from,
        execution_notes=[
            "Recuperar documentos relevantes ANTES del LLM",
            "Incluir fuentes (archivo, página, score) en la respuesta",
            "Si el score de relevancia es bajo, advertir al usuario",
        ],
        required_tools=["retrieval_search"],
        required_steps=["rewrite_query", "execute_retrieval", "build_doc_result"],
        fallback_pipeline="direct_chat",
        result_contract="DocRetrievalResult",
        allow_llm_only=False,
    )


def _plan_agentic_sql(tc: TaskClassification, ps: PipelineSelection) -> ExecutionPlan:
    return ExecutionPlan(
        pipeline_id="agentic_sql",
        data_sources=[
            DataSource("sql", "Consultas dinámicas a PostgreSQL vía agente SQL"),
        ],
        needs_sql_query=True,
        needs_doc_retrieval=False,
        needs_forecast_engine=False,
        output_format="json" if tc.needs_json else "text",
        pre_validations=[
            "Verificar conectividad con la BD",
        ],
        can_degrade=True,
        degrade_to="direct_chat",
        degraded_from=ps.degraded_from,
        execution_notes=[
            "El LLM genera consultas SQL vía function calling",
            "Post-validar que SQL real se ejecutó (no inventado)",
            "Limitar a consultas SELECT (solo lectura)",
        ],
        required_tools=["ejecutar_consulta_sql"],
        required_steps=["llm_sql_generation", "sql_execution", "build_sql_result"],
        fallback_pipeline="direct_chat",
        result_contract="AgenticSQLResult",
        allow_llm_only=False,
    )


def _plan_agentic_sql_rag(tc: TaskClassification, ps: PipelineSelection) -> ExecutionPlan:
    return ExecutionPlan(
        pipeline_id="agentic_sql_rag",
        data_sources=[
            DataSource("sql", "Consultas dinámicas a PostgreSQL"),
            DataSource("rag", "Búsqueda semántica en documentos indexados"),
        ],
        needs_sql_query=True,
        needs_doc_retrieval=True,
        needs_forecast_engine=False,
        output_format="json" if tc.needs_json else "text",
        pre_validations=[
            "Verificar conectividad con la BD",
            "Verificar documentos indexados en Qdrant",
        ],
        can_degrade=True,
        degrade_to="agentic_sql",
        degraded_from=ps.degraded_from,
        execution_notes=[
            "Ejecutar retrieval ANTES del LLM",
            "El LLM genera SQL vía function calling",
            "Combinar datos SQL + documentos en resultado",
        ],
        required_tools=["retrieval_search", "ejecutar_consulta_sql"],
        required_steps=["execute_retrieval", "llm_sql_generation", "sql_execution", "merge_results"],
        fallback_pipeline="agentic_sql",
        result_contract="AgenticSQLRAGResult",
        allow_llm_only=False,
    )


def _plan_predictive_insight(tc: TaskClassification, ps: PipelineSelection) -> ExecutionPlan:
    return ExecutionPlan(
        pipeline_id="predictive_insight",
        data_sources=[
            DataSource("sql", "Datos históricos para análisis de tendencias"),
            DataSource("context", "Contexto del dominio para interpretación", required=False),
        ],
        needs_sql_query=True,
        needs_doc_retrieval=False,
        needs_forecast_engine=False,
        output_format="text",
        pre_validations=[
            "Verificar que hay datos históricos suficientes para análisis",
        ],
        can_degrade=True,
        degrade_to="agentic_sql",
        degraded_from=ps.degraded_from,
        execution_notes=[
            "Análisis descriptivo SOLAMENTE — NO predicciones cuantitativas",
            "Describir tendencias, patrones y anomalías",
            "Explicar la base de datos usada y el periodo",
        ],
        required_tools=["forecast_sql_query"],
        required_steps=["extract_variable", "sql_historical_data", "build_insight"],
        fallback_pipeline="agentic_sql",
        result_contract="PredictiveInsightResult",
        allow_llm_only=False,
    )


def _plan_predictive_forecast(tc: TaskClassification, ps: PipelineSelection) -> ExecutionPlan:
    return ExecutionPlan(
        pipeline_id="predictive_forecast",
        data_sources=[
            DataSource("sql", "Dataset estructurado para forecasting"),
            DataSource("forecast", "Motor de predicción cuantitativa"),
        ],
        needs_sql_query=True,
        needs_doc_retrieval=False,
        needs_forecast_engine=True,
        output_format="json",
        pre_validations=[
            "Verificar dataset mínimo: ≥6 puntos temporales",
            "Verificar varianza no-nula en variable objetivo",
            "Si datos insuficientes, DEGRADAR a predictive_insight",
        ],
        can_degrade=True,
        degrade_to="predictive_insight",
        degraded_from=ps.degraded_from,
        execution_notes=[
            "OBLIGATORIO: ejecutar forecast_engine real",
            "OBLIGATORIO: feasibility_check antes del forecast",
            "PROHIBIDO: inventar predicción sin ForecastResult",
            "Si confianza < 0.4, degradar a predictive_insight",
        ],
        required_tools=["extract_target_variable", "forecast_sql_query",
                        "prediction_feasibility_check", "select_forecast_method",
                        "forecast_engine"],
        required_steps=[
            "extract_variable", "sql_historical_data",
            "feasibility_check", "method_selection",
            "forecast_execution", "backtesting", "log_prediction",
        ],
        fallback_pipeline="predictive_insight",
        result_contract="PredictiveForecastResult",
        allow_llm_only=False,
    )


def validate_plan(plan: ExecutionPlan) -> tuple[bool, list[str]]:
    """
    Ejecuta validaciones previas del plan.
    Devuelve (ok, lista de problemas).
    """
    problems = []

    for validation in plan.pre_validations:
        if "conectividad con la BD" in validation:
            try:
                from db.connector import _get_conn
                conn = _get_conn()
                conn.close()
            except Exception as e:
                problems.append(f"BD no disponible: {e}")

        elif "documentos indexados" in validation:
            try:
                from retrieval.search import list_indexed_documents
                docs = list_indexed_documents()
                if not docs:
                    problems.append("No hay documentos indexados en Qdrant")
            except Exception as e:
                problems.append(f"Qdrant no disponible: {e}")

    return len(problems) == 0, problems
