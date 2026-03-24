"""
llm/model_router.py — Router de modelos con composite scoring V2.1
Selecciona el modelo óptimo usando 5 dimensiones: calidad, coste, latencia, fiabilidad, feedback.
Usa métricas por pipeline cuando están disponibles.
Excluye siempre modelos Preliminary.
"""
from dataclasses import dataclass
from typing import Optional
import json

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import model_db as db


@dataclass
class ModelInfo:
    """Info de un modelo seleccionado."""
    uuid: str
    model_id: str        # 'gemini-3.1-pro-preview'
    display_name: str
    provider: str
    price_input: float
    price_output: float


@dataclass
class RoutingDecision:
    """Resultado completo de la selección de modelo."""
    primary: ModelInfo
    fallback_chain: list[ModelInfo]
    reason: str
    composite_score: float
    score_breakdown: dict  # {quality, cost, latency, reliability, feedback}
    all_candidates_scores: list[dict]  # para logging/debug


def route(query: str, pipeline_id: str, force_model: str = None) -> RoutingDecision:
    """
    Selecciona el mejor modelo para el pipeline dado.
    
    Args:
        query: la pregunta del usuario
        pipeline_id: pipeline seleccionado (determina pesos y categoría arena)
        force_model: forzar un modelo específico (override admin)
    """
    # ── Obtener pipeline config ──
    pipeline = db.get_pipeline(pipeline_id)
    if not pipeline:
        # Fallback a direct_chat si pipeline no encontrado
        pipeline = db.get_pipeline("direct_chat") or _default_pipeline()
    
    # ── Force model override ──
    if force_model:
        model = db.get_model_by_id(force_model)
        if model:
            mi = _to_model_info(model)
            return RoutingDecision(
                primary=mi,
                fallback_chain=[],
                reason=f"Modelo forzado: {force_model}",
                composite_score=1.0,
                score_breakdown={},
                all_candidates_scores=[],
            )
    
    # ── Obtener modelos candidatos ──
    all_models = db.get_active_models()
    candidates = _filter_candidates(all_models, pipeline)
    
    if not candidates:
        # Si no hay candidatos tras filtrar, usar todos los activos
        candidates = all_models
        if not candidates:
            raise RuntimeError("No hay modelos activos disponibles")
    
    # ── Calcular scores ──
    scored = []
    for model in candidates:
        breakdown = _calculate_scores(model, pipeline)
        final = _composite_score(breakdown, pipeline)
        scored.append({
            "model": model,
            "breakdown": breakdown,
            "final": final,
        })
    
    # Ordenar por score compuesto descendente
    scored.sort(key=lambda x: x["final"], reverse=True)
    
    # ── Seleccionar primario y fallbacks ──
    primary_entry = scored[0]
    primary_model = _to_model_info(primary_entry["model"])
    
    # Fallback: siguientes candidatos (distinto proveedor preferido)
    fallback_chain = _build_fallback_chain(scored[1:], primary_entry["model"])
    
    # ── Razón de selección ──
    arena_cat = pipeline.get("arena_category", "text")
    arena_score = _get_arena_score(primary_entry["model"], arena_cat)
    reason = (
        f"Mejor composite score ({primary_entry['final']:.3f}) para pipeline "
        f"'{pipeline_id}' [arena_{arena_cat}={arena_score}, "
        f"coste_esp=${_expected_cost(primary_entry['model'], pipeline):.4f}/1Ktok]"
    )
    
    return RoutingDecision(
        primary=primary_model,
        fallback_chain=fallback_chain,
        reason=reason,
        composite_score=primary_entry["final"],
        score_breakdown=primary_entry["breakdown"],
        all_candidates_scores=[
            {"model_id": s["model"]["model_id"], "score": round(s["final"], 4),
             "breakdown": {k: round(v, 4) for k, v in s["breakdown"].items()}}
            for s in scored
        ],
    )


# ─────────────────────────────────────────────
# Filtrado de candidatos
# ─────────────────────────────────────────────

def _filter_candidates(models: list[dict], pipeline: dict) -> list[dict]:
    """Filtra modelos por capacidades requeridas del pipeline."""
    filtered = []
    for m in models:
        # NUNCA incluir preliminary
        if m.get("is_preliminary"):
            continue
        if m.get("status") != "active":
            continue
        # Capacidades
        if pipeline.get("requires_tools") and not m.get("supports_tools"):
            continue
        if pipeline.get("requires_json") and not m.get("supports_json"):
            continue
        if pipeline.get("requires_vision") and not m.get("supports_vision"):
            continue
        # Context window
        if m.get("context_window", 0) < pipeline.get("min_context_window", 0):
            continue
        # Arena score mínimo
        arena_cat = pipeline.get("arena_category", "text")
        arena_score = _get_arena_score(m, arena_cat)
        if arena_score and pipeline.get("min_arena_score", 0) > 0:
            if arena_score < pipeline["min_arena_score"]:
                continue
        filtered.append(m)
    return filtered


# ─────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────

def _calculate_scores(model: dict, pipeline: dict) -> dict:
    """Calcula las 5 dimensiones de scoring para un modelo."""
    all_models = db.get_active_models()
    arena_cat = pipeline.get("arena_category", "text")
    
    # 1. Calidad (arena score normalizado)
    s_quality = _score_quality(model, all_models, arena_cat)
    
    # 2. Coste esperado (con input + output weights del pipeline)
    s_cost = _score_cost(model, all_models, pipeline)
    
    # 3. Latencia (histórica, por pipeline si disponible)
    s_latency = _score_latency(model, all_models, pipeline)
    
    # 4. Fiabilidad (tasa de éxito 7d)
    s_reliability = _score_reliability(model)
    
    # 5. Feedback (promedio 1-5 normalizado)
    s_feedback = _score_feedback(model, pipeline)
    
    return {
        "quality": s_quality,
        "cost": s_cost,
        "latency": s_latency,
        "reliability": s_reliability,
        "feedback": s_feedback,
    }


def _composite_score(breakdown: dict, pipeline: dict) -> float:
    """Calcula score final ponderado según pesos del pipeline."""
    return (
        pipeline.get("weight_quality", 0.5) * breakdown["quality"] +
        pipeline.get("weight_cost", 0.3) * breakdown["cost"] +
        pipeline.get("weight_latency", 0.1) * breakdown["latency"] +
        pipeline.get("weight_reliability", 0.05) * breakdown["reliability"] +
        pipeline.get("weight_feedback", 0.05) * breakdown["feedback"]
    )


def _score_quality(model: dict, all_models: list[dict], arena_cat: str) -> float:
    """Arena score normalizado 0-1."""
    score = _get_arena_score(model, arena_cat)
    if not score:
        return 0.5  # default medio
    max_score = max(
        (_get_arena_score(m, arena_cat) or 0) for m in all_models
    )
    return score / max_score if max_score > 0 else 0.5


def _score_cost(model: dict, all_models: list[dict], pipeline: dict) -> float:
    """Coste ponderado con input+output weights, invertido (barato=alto score)."""
    cost = _expected_cost(model, pipeline)
    max_cost = max(_expected_cost(m, pipeline) for m in all_models)
    if max_cost == 0:
        return 1.0
    return 1.0 - (cost / max_cost)


def _expected_cost(model: dict, pipeline: dict) -> float:
    """Coste esperado por 1K tokens usando pesos del pipeline."""
    w_in = pipeline.get("expected_input_weight", 0.3)
    w_out = pipeline.get("expected_output_weight", 0.7)
    return (w_in * model["price_input"] + w_out * model["price_output"]) / 1000


def _score_latency(model: dict, all_models: list[dict], pipeline: dict) -> float:
    """Latencia normalizada (menor=mejor). Usa métricas por pipeline si existen."""
    # Intentar métricas por pipeline
    pipeline_metrics = db.get_pipeline_metrics(str(model["id"]), pipeline["id"])
    if pipeline_metrics and pipeline_metrics.get("avg_latency_ms"):
        lat = pipeline_metrics["avg_latency_ms"]
    else:
        lat = model.get("avg_latency_ms") or 2000  # default 2s
    
    max_lat = max(
        (m.get("avg_latency_ms") or 2000) for m in all_models
    )
    if max_lat == 0:
        return 1.0
    return 1.0 - (lat / max_lat)


def _score_reliability(model: dict) -> float:
    """Tasa de éxito 7d. Mínimas muestras = default 0.95."""
    total = model.get("total_requests_7d") or 0
    if total < 10:
        return 0.95  # default optimista
    return model.get("success_rate_7d") or 0.95


def _score_feedback(model: dict, pipeline: dict) -> float:
    """Feedback promedio normalizado 0-1. Usa métricas por pipeline si existen."""
    pipeline_metrics = db.get_pipeline_metrics(str(model["id"]), pipeline["id"])
    if pipeline_metrics and pipeline_metrics.get("avg_feedback_score"):
        fb = pipeline_metrics["avg_feedback_score"]
    else:
        fb = model.get("avg_feedback_score") or 3.0
    
    # Normalizar 1-5 → 0-1
    return max(0, min(1, (fb - 1) / 4))


# ─────────────────────────────────────────────
# Fallback chain
# ─────────────────────────────────────────────

def _build_fallback_chain(remaining_scored: list[dict],
                          primary_model: dict) -> list[ModelInfo]:
    """
    Construye cadena de fallback inteligente:
    - Preferir distinto proveedor al primario
    - Máximo 3 fallbacks
    """
    chain = []
    primary_provider = primary_model.get("provider", "")
    
    # Priorizar otro proveedor
    other_provider = [
        s for s in remaining_scored
        if s["model"].get("provider") != primary_provider
    ]
    same_provider = [
        s for s in remaining_scored
        if s["model"].get("provider") == primary_provider
    ]
    
    # Intercalar: otro proveedor primero, luego mismo
    ordered = other_provider + same_provider
    
    for entry in ordered[:3]:
        chain.append(_to_model_info(entry["model"]))
    
    return chain


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _get_arena_score(model: dict, category: str) -> int:
    """Extrae arena score de una categoría."""
    scores = model.get("arena_scores") or {}
    if isinstance(scores, str):
        scores = json.loads(scores)
    return scores.get(category, 0)


def _to_model_info(model: dict) -> ModelInfo:
    return ModelInfo(
        uuid=str(model["id"]),
        model_id=model["model_id"],
        display_name=model["display_name"],
        provider=model["provider"],
        price_input=model["price_input"],
        price_output=model["price_output"],
    )


def _default_pipeline() -> dict:
    return {
        "id": "direct_chat",
        "arena_category": "text",
        "min_arena_score": 0,
        "min_context_window": 4000,
        "requires_tools": False,
        "requires_json": False,
        "requires_vision": False,
        "weight_quality": 0.3,
        "weight_cost": 0.4,
        "weight_latency": 0.2,
        "weight_reliability": 0.05,
        "weight_feedback": 0.05,
        "expected_input_weight": 0.4,
        "expected_output_weight": 0.6,
    }
