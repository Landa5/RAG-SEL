"""
llm/pipeline_selector.py — Selección de pipeline según clasificación de tarea V2.1
Pipeline = estrategia de resolución (no el modelo concreto).
"""
from dataclasses import dataclass
from typing import Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from llm.task_classifier import TaskClassification


@dataclass
class PipelineSelection:
    """Resultado de la selección de pipeline."""
    pipeline_id: str
    reason: str
    requires_retrieval: bool
    requires_sql: bool
    requires_forecasting: bool
    degraded_from: Optional[str] = None  # si se degradó desde otro pipeline


# ─────────────────────────────────────────────
# Reglas de mapeo task_family → pipeline
# ─────────────────────────────────────────────

# Reglas ordenadas por prioridad (la primera que matchee gana)
# Cada regla: (condición, pipeline_id, razón)

def select_pipeline(classification: TaskClassification) -> PipelineSelection:
    """
    Selecciona el pipeline óptimo basándose en la clasificación de tarea.
    Usa reglas heurísticas + capacidades secundarias para composición.
    """
    tf = classification.primary_task_family
    caps = set(classification.secondary_capabilities)
    
    # ─── Predicción ───
    if tf == "prediction" or classification.predictive_intent:
        has_sql = "sql" in caps or _has_structured_data_context(classification)
        
        if has_sql and classification.complexity >= 4:
            return PipelineSelection(
                pipeline_id="predictive_forecast",
                reason=f"Intención predictiva con datos estructurados (complexity={classification.complexity})",
                requires_retrieval="rag" in caps,
                requires_sql=True,
                requires_forecasting=True,
            )
        else:
            return PipelineSelection(
                pipeline_id="predictive_insight",
                reason="Intención predictiva — análisis descriptivo/tendencias",
                requires_retrieval="rag" in caps,
                requires_sql=has_sql,
                requires_forecasting=False,
            )
    
    # ─── Chat trivial ───
    if tf == "cheap_chat":
        return PipelineSelection(
            pipeline_id="direct_chat",
            reason="Conversación trivial, sin herramientas necesarias",
            requires_retrieval=False,
            requires_sql=False,
            requires_forecasting=False,
        )
    
    # ─── Razonamiento complejo ───
    if tf == "reasoning_hard":
        has_sql = "sql" in caps
        has_rag = "rag" in caps
        
        if has_sql and has_rag:
            return PipelineSelection(
                pipeline_id="agentic_sql_rag",
                reason="Razonamiento complejo requiriendo SQL + documentos",
                requires_retrieval=True,
                requires_sql=True,
                requires_forecasting=False,
            )
        elif has_sql:
            return PipelineSelection(
                pipeline_id="agentic_sql",
                reason="Razonamiento complejo con datos estructurados (SQL)",
                requires_retrieval=False,
                requires_sql=True,
                requires_forecasting=False,
            )
        elif has_rag:
            return PipelineSelection(
                pipeline_id="doc_retrieval",
                reason="Razonamiento sobre documentos",
                requires_retrieval=True,
                requires_sql=False,
                requires_forecasting=False,
            )
        else:
            # Si no hay SQL ni RAG explícito, asumimos SQL por defecto
            # (la mayoría de preguntas complejas en este contexto requieren BD)
            return PipelineSelection(
                pipeline_id="agentic_sql",
                reason="Razonamiento complejo — asumiendo SQL por contexto del dominio",
                requires_retrieval=False,
                requires_sql=True,
                requires_forecasting=False,
            )
    
    # ─── RAG / Documentos ───
    if tf == "rag_qa":
        has_sql = "sql" in caps
        if has_sql:
            return PipelineSelection(
                pipeline_id="agentic_sql_rag",
                reason="Pregunta documental + datos estructurados",
                requires_retrieval=True,
                requires_sql=True,
                requires_forecasting=False,
            )
        return PipelineSelection(
            pipeline_id="doc_retrieval",
            reason="Pregunta sobre documentos indexados",
            requires_retrieval=True,
            requires_sql=False,
            requires_forecasting=False,
        )
    
    # ─── Extracción ───
    if tf == "extraction":
        has_rag = "rag" in caps
        has_sql = "sql" in caps
        if has_sql and has_rag:
            return PipelineSelection(
                pipeline_id="agentic_sql_rag",
                reason="Extracción de datos de BD + documentos",
                requires_retrieval=True,
                requires_sql=True,
                requires_forecasting=False,
            )
        elif has_rag:
            return PipelineSelection(
                pipeline_id="doc_retrieval",
                reason="Extracción de información documental",
                requires_retrieval=True,
                requires_sql=False,
                requires_forecasting=False,
            )
        elif has_sql:
            return PipelineSelection(
                pipeline_id="agentic_sql",
                reason="Extracción de datos estructurados",
                requires_retrieval=False,
                requires_sql=True,
                requires_forecasting=False,
            )
        return PipelineSelection(
            pipeline_id="direct_chat",
            reason="Extracción sin fuente de datos identificada",
            requires_retrieval=False,
            requires_sql=False,
            requires_forecasting=False,
        )
    
    # ─── Instruction following ───
    if tf == "instruction_following":
        has_sql = "sql" in caps
        if has_sql:
            return PipelineSelection(
                pipeline_id="agentic_sql",
                reason="Instrucción que requiere consulta SQL",
                requires_retrieval=False,
                requires_sql=True,
                requires_forecasting=False,
            )
        return PipelineSelection(
            pipeline_id="direct_chat",
            reason="Instrucción directa sin datos",
            requires_retrieval=False,
            requires_sql=False,
            requires_forecasting=False,
        )
    
    # ─── Creative writing / Coding ───
    if tf in ("creative_writing", "coding"):
        return PipelineSelection(
            pipeline_id="direct_chat",
            reason=f"Tarea de {tf}, respuesta directa del LLM",
            requires_retrieval=False,
            requires_sql=False,
            requires_forecasting=False,
        )
    
    # ─── Default ───
    return PipelineSelection(
        pipeline_id="direct_chat",
        reason="Familia no reconocida, usando pipeline por defecto",
        requires_retrieval=False,
        requires_sql=False,
        requires_forecasting=False,
    )


def degrade_pipeline(current: PipelineSelection, reason: str) -> PipelineSelection:
    """
    Degrada un pipeline a uno más simple.
    Usado cuando no hay datos suficientes para predicción, etc.
    """
    degradation_map = {
        "predictive_forecast": "predictive_insight",
        "predictive_insight": "agentic_sql_rag",
        "agentic_sql_rag": "agentic_sql",
        "agentic_sql": "doc_retrieval",
        "doc_retrieval": "direct_chat",
    }
    
    degraded_to = degradation_map.get(current.pipeline_id, "direct_chat")
    
    return PipelineSelection(
        pipeline_id=degraded_to,
        reason=f"Degradado desde {current.pipeline_id}: {reason}",
        requires_retrieval=degraded_to in ("doc_retrieval", "agentic_sql_rag"),
        requires_sql=degraded_to in ("agentic_sql", "agentic_sql_rag", "predictive_insight"),
        requires_forecasting=False,
        degraded_from=current.pipeline_id,
    )


def _has_structured_data_context(classification: TaskClassification) -> bool:
    """Detecta si la pregunta implica datos estructurados aunque no diga 'SQL'."""
    # Keywords que implican datos tabulares/estructurados
    structured_hints = [
        "gasto", "coste", "km", "horas", "litros", "descargas",
        "productividad", "rendimiento", "mensual", "trimestral",
        "por mes", "por conductor", "por camión",
    ]
    # Este check es redundante con el clasificador pero sirve como safety net
    return "sql" in classification.secondary_capabilities
