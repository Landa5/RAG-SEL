"""
llm/result_contracts.py — Contratos de salida tipados por pipeline V3.0
Cada pipeline DEBE devolver una estructura específica antes de invocar al LLM.
El LLM solo puede explicar/redactar el resultado — nunca calcularlo.
"""
from dataclasses import dataclass, field
from typing import Optional, Any
from datetime import datetime


# ─────────────────────────────────────────────
# Registro de ejecución de herramientas
# ─────────────────────────────────────────────

@dataclass
class ToolExecution:
    """Registro granular de una herramienta ejecutada."""
    tool_name: str
    success: bool
    duration_ms: int
    result_summary: str          # resumen corto del resultado
    error: Optional[str] = None
    input_preview: Optional[str] = None   # primeros 200 chars del input


@dataclass
class ExecutionLog:
    """Trazabilidad completa de la ejecución de un pipeline."""
    pipeline_selected: str
    pipeline_executed: str
    forecast_engine_executed: bool = False
    feasibility_check_executed: bool = False
    retrieval_executed: bool = False
    sql_executed: bool = False
    llm_only_response: bool = False
    degraded_from: Optional[str] = None
    degraded_to: Optional[str] = None
    structured_result_type: Optional[str] = None
    tool_executions: list[ToolExecution] = field(default_factory=list)
    total_duration_ms: int = 0
    error: Optional[str] = None           # error global de ejecución
    warnings: list[str] = field(default_factory=list)  # advertencias

    def add_tool(self, tool: ToolExecution):
        self.tool_executions.append(tool)


# ─────────────────────────────────────────────
# Resultados por pipeline
# ─────────────────────────────────────────────

@dataclass
class DirectChatResult:
    """Pipeline direct_chat: respuesta conversacional sin herramientas."""
    answer_text: str = ""
    notes: list[str] = field(default_factory=list)
    confidence: float = 1.0

    result_type: str = "direct_chat"


@dataclass
class DocRetrievalResult:
    """Pipeline doc_retrieval: respuesta basada en documentos indexados."""
    retrieved_chunks: list[dict] = field(default_factory=list)
    sources: list[dict] = field(default_factory=list)
    answer_basis: str = ""       # texto base construido con los chunks
    rewritten_query: str = ""    # query reescrita para retrieval
    confidence: float = 0.0

    result_type: str = "doc_retrieval"

    @property
    def has_evidence(self) -> bool:
        return len(self.retrieved_chunks) > 0 and len(self.sources) > 0


@dataclass
class AgenticSQLResult:
    """Pipeline agentic_sql: resultado de consultas SQL ejecutadas."""
    sql_queries: list[dict] = field(default_factory=list)   # [{sql, description, rows_count}]
    rows_returned: int = 0
    structured_findings: str = ""   # resumen de hallazgos
    raw_results: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)   # advertencias post-ejecución
    confidence: float = 0.0

    result_type: str = "agentic_sql"

    @property
    def has_data(self) -> bool:
        return self.rows_returned > 0


@dataclass
class AgenticSQLRAGResult:
    """Pipeline agentic_sql_rag: combinación de SQL + documentos."""
    sql_result: Optional[AgenticSQLResult] = None
    rag_result: Optional[DocRetrievalResult] = None
    merged_findings: str = ""
    sources: list[dict] = field(default_factory=list)
    confidence: float = 0.0

    result_type: str = "agentic_sql_rag"

    @property
    def has_evidence(self) -> bool:
        sql_ok = self.sql_result and self.sql_result.has_data
        rag_ok = self.rag_result and self.rag_result.has_evidence
        return sql_ok or rag_ok


@dataclass
class PredictiveInsightResult:
    """Pipeline predictive_insight: análisis descriptivo sin predicción cuantitativa."""
    descriptive_analysis: str = ""
    trend_summary: str = ""
    data_points: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    confidence: float = 0.0
    degraded_from_forecast: bool = False
    degrade_reason: str = ""

    result_type: str = "predictive_insight"


# PredictiveForecastResult se importa directamente de forecast_engine.py
# Re-export para conveniencia
from llm.forecast_engine import ForecastResult as PredictiveForecastResult


# ─────────────────────────────────────────────
# Resultado orquestado (envolvente)
# ─────────────────────────────────────────────

# Política de allow_llm_only por pipeline
_LLM_ONLY_POLICY = {
    "direct_chat": True,       # único pipeline donde LLM puede actuar solo
    "doc_retrieval": False,
    "agentic_sql": False,
    "agentic_sql_rag": False,
    "predictive_insight": False,
    "predictive_forecast": False,
}


def is_llm_only_allowed(pipeline_id: str) -> bool:
    """Determina si el pipeline permite respuestas solo-LLM."""
    return _LLM_ONLY_POLICY.get(pipeline_id, False)


# Tipos de resultado que se consideran "críticos" (requieren evidencia)
CRITICAL_PIPELINES = {"predictive_forecast", "predictive_insight",
                      "agentic_sql", "agentic_sql_rag", "doc_retrieval"}


@dataclass
class OrchestratedResult:
    """Resultado completo de la orquestación — se pasa al LLM para redacción."""
    pipeline_executed: str
    structured_result: Any       # uno de los *Result de arriba
    execution_log: ExecutionLog
    prompt_for_llm: str = ""     # prompt construido por response_renderer
    allow_llm_only: bool = False
    degraded_from: Optional[str] = None
    classification_info: dict = field(default_factory=dict)
    model_name: str = ""

    @property
    def result_type(self) -> str:
        if self.structured_result and hasattr(self.structured_result, 'result_type'):
            return self.structured_result.result_type
        return "unknown"

    @property
    def is_critical(self) -> bool:
        return self.pipeline_executed in CRITICAL_PIPELINES

    def validate(self) -> tuple[bool, str]:
        """Valida que el resultado cumple el contrato del pipeline."""
        if self.allow_llm_only:
            return True, "LLM-only permitido"

        sr = self.structured_result
        if sr is None:
            return False, f"Pipeline {self.pipeline_executed} requiere resultado estructurado"

        if self.pipeline_executed == "doc_retrieval":
            if not isinstance(sr, DocRetrievalResult) or not sr.has_evidence:
                return False, "doc_retrieval requiere chunks y fuentes reales"

        elif self.pipeline_executed == "agentic_sql":
            if not isinstance(sr, AgenticSQLResult):
                return False, "agentic_sql requiere AgenticSQLResult"

        elif self.pipeline_executed == "agentic_sql_rag":
            if not isinstance(sr, AgenticSQLRAGResult):
                return False, "agentic_sql_rag requiere AgenticSQLRAGResult"

        elif self.pipeline_executed == "predictive_forecast":
            if not isinstance(sr, PredictiveForecastResult):
                return False, "predictive_forecast requiere PredictiveForecastResult"

        elif self.pipeline_executed == "predictive_insight":
            if not isinstance(sr, PredictiveInsightResult):
                return False, "predictive_insight requiere PredictiveInsightResult"

        return True, "OK"
