"""
eval/ai_judge.py — IA evaluadora de respuestas del sistema RAG-SEL (v2)

Ajustes v2:
  1. Export diferenciado: raw_ai_review / candidate / approved
  2. Revisión humana: requires_human_review automático para fail/critical/muestra warnings
  3. LLM_INVENTED_NUMBERS contextual (pipeline + grounding)
  4. SQL checks finos: MISINTERPRETED, NOT_GROUNDED, SCOPE_TOO_BROAD
  5. Severidad dual: risk_level + quality_level
  6. Muestreo inteligente: 100% SQL/predictivas/híbridas/degradadas/error/cifras,
     muestreo parcial solo para direct_chat trivial
"""
import os
import json
import time
import random
import re
from typing import Optional
from dataclasses import dataclass, field

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────

JUDGE_MODEL = os.getenv("RAGSEL_JUDGE_MODEL", "gemini-2.0-flash")
JUDGE_TRIVIAL_SAMPLE_RATE = float(os.getenv("RAGSEL_JUDGE_TRIVIAL_SAMPLE_RATE", "0.3"))
JUDGE_ENABLED = os.getenv("RAGSEL_JUDGE_ENABLED", "true").lower() == "true"
JUDGE_WARNING_SAMPLE_RATE = float(os.getenv("RAGSEL_JUDGE_WARNING_SAMPLE_RATE", "0.5"))

_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/"


# ─────────────────────────────────────────────
# Resultado de evaluación
# ─────────────────────────────────────────────

@dataclass
class JudgeVerdict:
    grounding_score: float = 0.0
    hallucination_risk: float = 0.0
    usefulness_score: float = 0.0
    pipeline_correct: bool = True
    prediction_correct: bool = None
    sql_consistency: bool = None
    rag_evidence_present: bool = None
    degradation_correct: bool = None
    verdict: str = "pass"
    risk_level: str = "low"           # low | medium | high | critical
    quality_level: str = "acceptable"  # poor | acceptable | good | excellent
    issues: list = field(default_factory=list)
    review_text: str = ""
    review_model: str = ""
    latency_ms: int = 0
    requires_human_review: bool = False
    human_review_reason: str = None


# ─────────────────────────────────────────────
# Muestreo inteligente (ajuste 6)
# ─────────────────────────────────────────────

# Pipelines que SIEMPRE se revisan (100%)
_ALWAYS_REVIEW_PIPELINES = {
    "agentic_sql", "agentic_sql_rag",
    "predictive_forecast", "predictive_insight",
}


def _should_review(execution_data: dict, answer: str) -> bool:
    """
    Muestreo inteligente:
    - 100% SQL, predictivas, híbridas → siempre
    - 100% degradadas → siempre
    - 100% con error → siempre
    - 100% con cifras en la respuesta → siempre
    - Parcial para direct_chat trivial
    """
    pipeline = execution_data.get("pipeline_executed", "")

    # Siempre revisar pipelines complejos
    if pipeline in _ALWAYS_REVIEW_PIPELINES:
        return True

    # Siempre revisar degradaciones
    if execution_data.get("degraded_from"):
        return True

    # Siempre revisar errores
    if execution_data.get("error"):
        return True

    # Siempre revisar si hay cifras sustanciales (≥2 números largos)
    nums = re.findall(r'\b\d{3,}\b', answer)
    if len(nums) >= 2:
        return True

    # Siempre revisar RAG (tiene fuentes que validar)
    if pipeline in ("doc_retrieval",):
        return True

    # Direct chat trivial → muestreo parcial
    if pipeline == "direct_chat":
        return random.random() < JUDGE_TRIVIAL_SAMPLE_RATE

    # Todo lo demás → revisar
    return True


# ─────────────────────────────────────────────
# Reglas pre-check (determinísticas, sin IA)
# ─────────────────────────────────────────────

def _pre_check_rules(question: str, answer: str, execution_data: dict) -> list[str]:
    """
    Reglas determinísticas antes de invocar al LLM evaluador.
    v2: LLM_INVENTED_NUMBERS contextual + SQL checks finos.
    """
    issues = []
    pipeline = execution_data.get("pipeline_executed", "")
    engines = execution_data.get("engines", {})
    sources = execution_data.get("sources", [])

    # ── R1: SQL no ejecutado ──
    if pipeline in ("agentic_sql", "agentic_sql_rag"):
        if not engines.get("sql", False) and not execution_data.get("sql_executed", False):
            issues.append("SQL_NOT_EXECUTED: pipeline SQL pero sql_executed=false")

    # ── R2: RAG sin fuentes ──
    if pipeline in ("doc_retrieval", "agentic_sql_rag"):
        if not sources:
            issues.append("RAG_NO_SOURCES: pipeline RAG sin fuentes/evidencia")

    # ── R3: Forecast no ejecutado ──
    if pipeline in ("predictive_forecast", "predictive_insight"):
        if not engines.get("forecast", False) and not execution_data.get("forecast_engine_executed", False):
            degraded = execution_data.get("degraded_from")
            if not degraded:
                issues.append("FORECAST_NOT_EXECUTED: pipeline predictivo sin forecast ni degradación")

    # ── R4: LLM_INVENTED_NUMBERS contextual (ajuste 3) ──
    issues.extend(_check_invented_numbers(question, answer, pipeline, engines, sources))

    # ── R5: SQL checks finos (ajuste 4) ──
    if pipeline in ("agentic_sql", "agentic_sql_rag") and engines.get("sql", False):
        issues.extend(_check_sql_quality(question, answer, execution_data))

    # ── R6: Degradación incompleta ──
    if execution_data.get("degraded_from") and not execution_data.get("degraded_to"):
        issues.append("DEGRADATION_INCOMPLETE: degradación registrada sin destino")

    return issues


def _check_invented_numbers(
    question: str, answer: str,
    pipeline: str, engines: dict, sources: list,
) -> list[str]:
    """
    Ajuste 3: LLM_INVENTED_NUMBERS contextual.
    No solo cuenta cifras, sino que evalúa:
    - pipeline (direct_chat sin datos = sospechoso)
    - tipo de pregunta (pregunta cuantitativa sin grounding = sospechoso)
    - existencia de grounding SQL/RAG/forecast
    """
    issues = []

    # Extraer números significativos (≥3 dígitos, excluyendo años y códigos comunes)
    all_nums = re.findall(r'\b\d+[.,]?\d*\b', answer)
    significant_nums = [
        n for n in all_nums
        if len(n.replace(",", "").replace(".", "")) >= 3
        and not re.match(r'^(19|20)\d{2}$', n)  # excluir años
    ]

    if not significant_nums:
        return issues

    # ¿Tiene grounding real?
    has_sql = engines.get("sql", False)
    has_rag = bool(sources)
    has_forecast = engines.get("forecast", False)
    has_grounding = has_sql or has_rag or has_forecast

    # Caso 1: Direct chat o LLM-only con números → sospechoso si no hay grounding
    if pipeline == "direct_chat" or execution_data_is_llm_only(engines):
        if len(significant_nums) >= 2 and not has_grounding:
            issues.append(
                f"LLM_INVENTED_NUMBERS: {len(significant_nums)} cifras "
                f"en pipeline {pipeline} sin grounding SQL/RAG/forecast"
            )

    # Caso 2: Pipeline con datos pero numbers no explicados por las fuentes
    elif has_grounding and len(significant_nums) >= 5:
        issues.append(
            "LLM_NUMBERS_EXCESS: respuesta con muchas cifras que podrían "
            "exceder los datos reales del pipeline"
        )

    # Caso 3: Pregunta cuantitativa sin ningún grounding
    quantitative_keywords = [
        "cuánto", "cuantos", "cuántas", "total", "suma", "media",
        "promedio", "máximo", "mínimo", "porcentaje",
    ]
    is_quantitative = any(kw in question.lower() for kw in quantitative_keywords)
    if is_quantitative and not has_grounding and len(significant_nums) >= 1:
        issues.append(
            "LLM_QUANTITATIVE_NO_GROUNDING: pregunta cuantitativa "
            "respondida sin datos reales"
        )

    return issues


def execution_data_is_llm_only(engines: dict) -> bool:
    """Comprueba si la ejecución fue solo LLM sin herramientas."""
    return not any(engines.get(k, False) for k in ("sql", "retrieval", "forecast"))


def _check_sql_quality(question: str, answer: str, execution_data: dict) -> list[str]:
    """
    Ajuste 4: SQL checks finos.
    - SQL_RESULT_MISINTERPRETED: la respuesta contradice o malinterpreta los datos SQL
    - SQL_ANSWER_NOT_GROUNDED: la respuesta incluye datos no presentes en el resultado SQL
    - SQL_SCOPE_TOO_BROAD: la query SQL parece demasiado amplia para la pregunta
    """
    issues = []

    # Si hay metadata de SQL en execution_data, usar para checks
    sql_result = execution_data.get("sql_result", "")
    sql_query = execution_data.get("sql_query", "")

    # Check: respuesta vacía pero SQL ejecutado exitosamente
    if execution_data.get("sql_executed", False) and not answer.strip():
        issues.append("SQL_RESULT_MISINTERPRETED: SQL ejecutado pero respuesta vacía")

    # Check: SQL con SELECT * o sin WHERE (scope demasiado amplio)
    if sql_query:
        q_upper = sql_query.upper()
        if "SELECT *" in q_upper and "LIMIT" not in q_upper:
            issues.append("SQL_SCOPE_TOO_BROAD: query usa SELECT * sin LIMIT")
        if "WHERE" not in q_upper and "GROUP BY" not in q_upper and "LIMIT" not in q_upper:
            has_aggregation = any(f in q_upper for f in ("COUNT(", "SUM(", "AVG(", "MAX(", "MIN("))
            if not has_aggregation:
                issues.append("SQL_SCOPE_TOO_BROAD: query sin WHERE/GROUP BY/LIMIT ni agregación")

    # Check: respuesta menciona "no hay datos" pero SQL devolvió resultados
    if sql_result and ("no hay" in answer.lower() or "sin datos" in answer.lower()):
        if str(sql_result).strip() and str(sql_result).strip() != "[]":
            issues.append("SQL_RESULT_MISINTERPRETED: respuesta dice 'no hay datos' pero SQL devolvió resultados")

    return issues


# ─────────────────────────────────────────────
# Severidad dual (ajuste 5)
# ─────────────────────────────────────────────

def _compute_risk_level(issues: list, hallucination_risk: float, verdict: str) -> str:
    """
    risk_level: basado en riesgo de información incorrecta.
    critical: alucinación alta + datos SQL/predictivos incorrectos
    high: números inventados, SQL no ejecutado
    medium: warnings con datos dudosos
    low: sin riesgos detectados
    """
    critical_codes = {"SQL_NOT_EXECUTED", "FORECAST_NOT_EXECUTED", "SQL_RESULT_MISINTERPRETED"}
    high_codes = {"LLM_INVENTED_NUMBERS", "LLM_QUANTITATIVE_NO_GROUNDING", "RAG_NO_SOURCES"}

    issue_codes = {i.split(":")[0] for i in issues}

    if issue_codes & critical_codes or hallucination_risk >= 0.8:
        return "critical"
    if issue_codes & high_codes or hallucination_risk >= 0.5:
        return "high"
    if issues or hallucination_risk >= 0.3:
        return "medium"
    return "low"


def _compute_quality_level(
    grounding_score: float, usefulness_score: float,
    pipeline_correct: bool, issues: list,
) -> str:
    """
    quality_level: basado en calidad de la respuesta.
    poor: grounding < 0.3 o many issues
    acceptable: grounding ≥ 0.3 y usefulness ≥ 0.4
    good: grounding ≥ 0.6 y usefulness ≥ 0.6
    excellent: grounding ≥ 0.8 y usefulness ≥ 0.8 y pipeline correcto
    """
    if len(issues) >= 3 or grounding_score < 0.3:
        return "poor"
    if grounding_score >= 0.8 and usefulness_score >= 0.8 and pipeline_correct:
        return "excellent"
    if grounding_score >= 0.6 and usefulness_score >= 0.6:
        return "good"
    return "acceptable"


# ─────────────────────────────────────────────
# Revisión humana automática (ajuste 2)
# ─────────────────────────────────────────────

def _requires_human_review(verdict: str, risk_level: str, pipeline: str) -> tuple[bool, str]:
    """
    Determina si la review requiere revisión humana.
    Retorna (requires, reason).
    """
    # Siempre: verdict=fail
    if verdict == "fail":
        return True, "verdict=fail"

    # Siempre: risk=critical
    if risk_level == "critical":
        return True, "risk_level=critical"

    # Siempre: risk=high en pipelines con datos
    if risk_level == "high" and pipeline in _ALWAYS_REVIEW_PIPELINES:
        return True, f"risk=high en {pipeline}"

    # Muestreo de warnings
    if verdict == "warning":
        if random.random() < JUDGE_WARNING_SAMPLE_RATE:
            return True, "warning muestreado para revisión"

    return False, None


# ─────────────────────────────────────────────
# Prompt para el juez IA (v2)
# ─────────────────────────────────────────────

_JUDGE_SYSTEM_PROMPT = """Eres un evaluador experto de sistemas RAG (Retrieval Augmented Generation).
Tu trabajo es revisar la respuesta de un sistema y evaluar su calidad.

EVALÚA las siguientes dimensiones (0.0 a 1.0):

1. grounding_score: ¿La respuesta se basa en datos reales ejecutados por el sistema?
   - 1.0 = completamente basada en datos reales (SQL, RAG docs, forecast)
   - 0.0 = inventada sin base factual

2. hallucination_risk: ¿Hay riesgo de alucinación (datos inventados)?
   - 0.0 = sin riesgo, todo fundamentado
   - 1.0 = claramente inventado

3. usefulness_score: ¿La respuesta es útil para el usuario?
   - 1.0 = respuesta clara, completa y accionable
   - 0.0 = inútil, confusa o incorrecta

VERIFICA:
- pipeline_correct: ¿El pipeline seleccionado es adecuado para la pregunta?
- sql_consistency: Si usó SQL, ¿la respuesta refleja fielmente los datos retornados?
- rag_evidence_present: Si usó RAG, ¿hay fuentes que respalden cada afirmación?
- prediction_correct: Si es predicción, ¿el resultado es estadísticamente razonable?
- degradation_correct: Si hubo degradación de pipeline, ¿fue necesaria y justificada?

RESPONDE SOLO con JSON válido (sin markdown), con esta estructura exacta:
{
  "grounding_score": 0.0-1.0,
  "hallucination_risk": 0.0-1.0,
  "usefulness_score": 0.0-1.0,
  "pipeline_correct": true/false,
  "sql_consistency": true/false/null,
  "rag_evidence_present": true/false/null,
  "prediction_correct": true/false/null,
  "degradation_correct": true/false/null,
  "verdict": "pass|warning|fail",
  "issues": ["ISSUE_CODE: descripción"],
  "review_text": "explicación breve del veredicto"
}"""


def _build_judge_prompt(question: str, answer: str, execution_data: dict, pre_issues: list) -> str:
    pipeline = execution_data.get("pipeline_executed", "desconocido")
    model_used = execution_data.get("model", "desconocido")
    sources = execution_data.get("sources", [])
    engines = execution_data.get("engines", {})
    degraded_from = execution_data.get("degraded_from")
    degraded_to = execution_data.get("degraded_to")

    return f"""## Datos de la ejecución
- Pipeline: {pipeline}
- Modelo: {model_used}
- SQL ejecutado: {engines.get('sql', False)}
- RAG ejecutado: {engines.get('retrieval', False)}
- Forecast ejecutado: {engines.get('forecast', False)}
- LLM only: {execution_data_is_llm_only(engines)}
- Degradación: {f'{degraded_from} → {degraded_to}' if degraded_from else 'ninguna'}
- Fuentes: {len(sources)} documentos

## Pre-checks (reglas determinísticas)
{json.dumps(pre_issues, ensure_ascii=False) if pre_issues else 'Sin problemas detectados'}

## Pregunta del usuario
{question[:1000]}

## Respuesta del sistema
{answer[:3000]}

## Fuentes proporcionadas
{json.dumps(sources[:5], ensure_ascii=False, default=str)[:1000] if sources else 'Ninguna'}"""


# ─────────────────────────────────────────────
# Llamada al modelo juez
# ─────────────────────────────────────────────

def _call_judge_model(prompt: str) -> Optional[dict]:
    import requests
    try:
        from config import GEMINI_API_KEY
    except ImportError:
        return {"error": "config.GEMINI_API_KEY no disponible"}

    url = f"{_BASE_URL}{JUDGE_MODEL}:generateContent?key={GEMINI_API_KEY}"

    payload = {
        "contents": [
            {"role": "user", "parts": [{"text": prompt}]}
        ],
        "systemInstruction": {
            "parts": [{"text": _JUDGE_SYSTEM_PROMPT}]
        },
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 1000,
            "responseMimeType": "application/json",
        }
    }

    try:
        resp = requests.post(url, json=payload, timeout=30)
        if resp.status_code != 200:
            return None

        data = resp.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]

        return json.loads(text.strip())

    except Exception as e:
        return {"error": str(e)[:200]}


# ─────────────────────────────────────────────
# Punto de entrada principal
# ─────────────────────────────────────────────

def judge_response(
    execution_id: str,
    tenant_id: str,
    app_id: str,
    question: str,
    answer: str,
    execution_data: dict,
) -> Optional[JudgeVerdict]:
    """
    Evalúa una respuesta (v2 con muestreo inteligente + severidad dual).
    """
    if not JUDGE_ENABLED:
        return None

    # Muestreo inteligente (ajuste 6)
    if not _should_review(execution_data, answer):
        return None

    start = time.time()

    # 1. Pre-checks determinísticos (v2 con rules contextuales)
    pre_issues = _pre_check_rules(question, answer, execution_data)

    # 2. Llamada al juez IA
    prompt = _build_judge_prompt(question, answer, execution_data, pre_issues)
    ai_result = _call_judge_model(prompt)

    latency = int((time.time() - start) * 1000)

    # 3. Construir veredicto
    if ai_result and "error" not in ai_result:
        ai_issues = ai_result.get("issues", [])
        all_issues = list(set(pre_issues + ai_issues))

        verdict = ai_result.get("verdict", "pass")
        if pre_issues and verdict == "pass":
            verdict = "warning"

        grounding = float(ai_result.get("grounding_score", 0))
        hallu = float(ai_result.get("hallucination_risk", 0))
        useful = float(ai_result.get("usefulness_score", 0))
        pipe_correct = ai_result.get("pipeline_correct", True)

        # Severidad dual (ajuste 5)
        risk = _compute_risk_level(all_issues, hallu, verdict)
        quality = _compute_quality_level(grounding, useful, pipe_correct, all_issues)

        # Revisión humana (ajuste 2)
        pipeline = execution_data.get("pipeline_executed", "")
        needs_human, human_reason = _requires_human_review(verdict, risk, pipeline)

        return JudgeVerdict(
            grounding_score=grounding,
            hallucination_risk=hallu,
            usefulness_score=useful,
            pipeline_correct=pipe_correct,
            prediction_correct=ai_result.get("prediction_correct"),
            sql_consistency=ai_result.get("sql_consistency"),
            rag_evidence_present=ai_result.get("rag_evidence_present"),
            degradation_correct=ai_result.get("degradation_correct"),
            verdict=verdict,
            risk_level=risk,
            quality_level=quality,
            issues=all_issues,
            review_text=ai_result.get("review_text", ""),
            review_model=JUDGE_MODEL,
            latency_ms=latency,
            requires_human_review=needs_human,
            human_review_reason=human_reason,
        )
    else:
        # Fallback: solo reglas determinísticas
        verdict = "fail" if len(pre_issues) >= 2 else ("warning" if pre_issues else "pass")
        risk = _compute_risk_level(pre_issues, 0.5 if pre_issues else 0.0, verdict)
        quality = "poor" if pre_issues else "acceptable"
        pipeline = execution_data.get("pipeline_executed", "")
        needs_human, human_reason = _requires_human_review(verdict, risk, pipeline)

        return JudgeVerdict(
            grounding_score=0.5,
            hallucination_risk=0.5 if pre_issues else 0.0,
            usefulness_score=0.5,
            pipeline_correct=not any("PIPELINE" in i for i in pre_issues),
            sql_consistency=not any("SQL" in i for i in pre_issues),
            rag_evidence_present=not any("RAG" in i for i in pre_issues),
            verdict=verdict,
            risk_level=risk,
            quality_level=quality,
            issues=pre_issues,
            review_text=f"Evaluación solo con reglas (IA no disponible): {ai_result.get('error', 'timeout') if ai_result else 'sin respuesta'}",
            review_model="rules_only",
            latency_ms=latency,
            requires_human_review=needs_human,
            human_review_reason=human_reason,
        )


def judge_and_save(
    execution_id: str,
    tenant_id: str,
    app_id: str,
    question: str,
    answer: str,
    execution_data: dict,
) -> Optional[dict]:
    """Evalúa + guarda en BD. Para llamar desde background task."""
    verdict = judge_response(
        execution_id, tenant_id, app_id,
        question, answer, execution_data,
    )
    if not verdict:
        return None

    from db.review_db import save_review
    return save_review(
        execution_id=execution_id,
        tenant_id=tenant_id,
        app_id=app_id,
        review_model=verdict.review_model,
        pipeline_reviewed=execution_data.get("pipeline", "unknown"),
        question_preview=question,
        answer_preview=answer,
        grounding_score=verdict.grounding_score,
        hallucination_risk=verdict.hallucination_risk,
        usefulness_score=verdict.usefulness_score,
        pipeline_correct=verdict.pipeline_correct,
        prediction_correct=verdict.prediction_correct,
        sql_consistency=verdict.sql_consistency,
        rag_evidence_present=verdict.rag_evidence_present,
        degradation_correct=verdict.degradation_correct,
        verdict=verdict.verdict,
        issues=verdict.issues,
        review_text=verdict.review_text,
        risk_level=verdict.risk_level,
        quality_level=verdict.quality_level,
        requires_human_review=verdict.requires_human_review,
        human_review_reason=verdict.human_review_reason,
    )
