"""
llm/task_classifier.py — Clasificador de tareas multi-dimensión V2.1
Detecta: familia principal, capacidades secundarias, idioma, complejidad, intención predictiva.
Clasificación por reglas heurísticas (sin llamada LLM), preparada para refinamiento.
"""
from dataclasses import dataclass, field


@dataclass
class TaskClassification:
    """Resultado completo de la clasificación de una query."""
    primary_task_family: str          # cheap_chat, rag_qa, creative_writing, etc.
    secondary_capabilities: list[str] # ['rag', 'sql', 'prediction', 'json', 'tools', 'long_context']
    language: str                     # 'es', 'en', 'ca', ...
    needs_tools: bool
    needs_json: bool
    needs_long_context: bool
    needs_low_cost: bool
    predictive_intent: bool
    complexity: int                   # 1-5
    confidence: float                 # 0.0 - 1.0


# ─────────────────────────────────────────────
# Detección de idioma por stopwords
# ─────────────────────────────────────────────

_STOPWORDS_ES = {
    "de", "la", "el", "en", "los", "las", "un", "una", "que", "por", "con",
    "para", "del", "al", "es", "se", "lo", "como", "su", "más", "pero",
    "fue", "son", "está", "hay", "ser", "tiene", "este", "ya", "todo",
    "esta", "sin", "sobre", "entre", "también", "desde", "hasta", "cuando",
    "nos", "muy", "puede", "todos", "así", "nos", "le", "me", "donde",
    "cuánto", "cuántos", "cuántas", "dime", "quiero", "puedes", "hola",
}

_STOPWORDS_EN = {
    "the", "is", "at", "which", "on", "a", "an", "and", "or", "but",
    "in", "with", "to", "for", "of", "not", "you", "it", "be", "are",
    "was", "were", "been", "being", "have", "has", "had", "do", "does",
    "did", "will", "would", "shall", "should", "may", "might", "can",
    "could", "this", "that", "these", "those", "from", "how", "what",
}

_STOPWORDS_CA = {
    "de", "la", "el", "en", "els", "les", "un", "una", "que", "per",
    "amb", "del", "al", "és", "està", "hi", "ha", "ser", "com", "però",
}


def _detect_language(text: str) -> str:
    words = set(text.lower().split())
    scores = {
        "es": len(words & _STOPWORDS_ES),
        "en": len(words & _STOPWORDS_EN),
        "ca": len(words & _STOPWORDS_CA),
    }
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "es"  # default español


# ─────────────────────────────────────────────
# Keywords por familia de tarea
# ─────────────────────────────────────────────

_KEYWORDS = {
    "cheap_chat": [
        "hola", "buenos días", "buenas tardes", "buenas noches", "gracias",
        "adiós", "hasta luego", "qué tal", "cómo estás", "ok", "vale",
        "de acuerdo", "perfecto", "entendido", "genial",
    ],
    "rag_qa": [
        "factura", "facturas", "albarán", "documento", "documentos", "pdf",
        "convenio", "contrato", "proveedor", "expediente", "archivo",
        "según el documento", "dice el convenio", "certificado",
    ],
    "reasoning_hard": [
        "comparar", "comparativa", "analizar", "análisis", "tendencia",
        "evolución", "estadística", "promedio", "media", "máximo", "mínimo",
        "ranking", "porcentaje", "incremento", "reducción", "variación",
        "desglose", "detallado", "resumen completo", "coste total",
        "rentabilidad", "productividad", "eficiencia", "por qué",
        "cómo mejorar", "qué recomiendas", "mejor opción", "anomalía",
    ],
    "prediction": [
        "predecir", "predicción", "predice", "pronóstico", "forecast",
        "prever", "proyección", "estimar", "estimación", "estima",
        "futuro", "próximo mes", "próximo trimestre", "próximo año",
        "siguiente mes", "siguiente trimestre", "mes que viene",
        "va a pasar", "esperado", "previsto",
        "cuánto gastaremos", "cuánto costará", "cuánto será",
        "tendencia futura", "avería probable",
        "riesgo de", "riesgo", "probabilidad",
        "previsión", "cuántos litros se gastarán", "cuántos km se harán",
    ],
    "extraction": [
        "extrae", "extraer", "lista de", "tabla de", "dame una lista",
        "exportar", "resumen de todos", "todos los", "inventario",
    ],
    "creative_writing": [
        "escribe", "redacta", "genera un email", "genera un informe",
        "borrador", "carta", "formato formal", "redacción",
    ],
    "coding": [
        "código", "script", "función", "programa", "sql", "query",
        "python", "javascript", "api", "endpoint", "bug", "error de código",
    ],
    "instruction_following": [
        "paso a paso", "instrucciones", "cómo se hace", "tutorial",
        "explícame", "guía", "procedimiento", "protocolo",
    ],
}

# Keywords que indican necesidad de SQL
_SQL_KEYWORDS = [
    "empleado", "empleados", "conductor", "conductores", "jornada", "jornadas",
    "horas", "kilómetros", "km", "descarga", "descargas", "litros",
    "camión", "camiones", "flota", "matrícula", "itv", "seguro",
    "tacógrafo", "vehículo", "nómina", "ausencia", "vacaciones",
    "tarea", "tareas", "avería", "mantenimiento", "gasóleo", "gasoil",
    "gasto", "coste", "gasoil",
]

# Keywords que indican necesidad de RAG documental
_RAG_KEYWORDS = [
    "factura", "facturas", "albarán", "documento", "pdf", "convenio",
    "contrato", "proveedor", "según el documento", "certificado",
    "expediente", "archivo", "adjunto",
]


def classify(query: str) -> TaskClassification:
    """
    Clasifica una query de usuario en familia de tarea + capacidades.
    No hace llamadas LLM — usa heurísticas por reglas.
    """
    q = query.lower().strip()
    words = q.split()
    language = _detect_language(q)

    # ─── Contar matches por familia ───
    family_scores: dict[str, int] = {}
    for family, keywords in _KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in q)
        family_scores[family] = score

    # ─── Heurísticas adicionales de complejidad ───
    len_bonus = 0
    if len(q) > 200:
        len_bonus = 2
    elif len(q) > 100:
        len_bonus = 1
    family_scores["reasoning_hard"] = family_scores.get("reasoning_hard", 0) + len_bonus

    if q.count(" y ") >= 2:
        family_scores["reasoning_hard"] = family_scores.get("reasoning_hard", 0) + 1
    if q.count("?") > 1:
        family_scores["reasoning_hard"] = family_scores.get("reasoning_hard", 0) + 1

    # ─── PRIORIDAD EXPLÍCITA: prediction gana si tiene ≥1 match ───
    prediction_score = family_scores.get("prediction", 0)
    if prediction_score >= 1:
        primary = "prediction"
        primary_score = prediction_score
    else:
        # ─── Elegir familia primaria por score máximo ───
        primary = max(family_scores, key=family_scores.get)
        primary_score = family_scores[primary]

    # Si nada matchea, default a cheap_chat o rag_qa
    if primary_score == 0:
        if any(kw in q for kw in _SQL_KEYWORDS):
            primary = "reasoning_hard"
        elif any(kw in q for kw in _RAG_KEYWORDS):
            primary = "rag_qa"
        elif len(q) < 30:
            primary = "cheap_chat"
        else:
            primary = "instruction_following"

    # ─── Capacidades secundarias ───
    secondary: list[str] = []
    needs_sql = any(kw in q for kw in _SQL_KEYWORDS)
    needs_rag = any(kw in q for kw in _RAG_KEYWORDS)
    needs_tools = needs_sql or primary in ("reasoning_hard", "coding", "prediction")
    needs_json_out = any(kw in q for kw in ["json", "tabla", "exportar", "formato json"])
    needs_long_ctx = len(q) > 500 or "todos los" in q or "completo" in q
    predictive_intent = family_scores.get("prediction", 0) > 0

    if needs_sql:
        secondary.append("sql")
    if needs_rag:
        secondary.append("rag")
    if predictive_intent:
        secondary.append("prediction")
    if needs_json_out:
        secondary.append("json")
    if needs_tools:
        secondary.append("tools")
    if needs_long_ctx:
        secondary.append("long_context")

    # ─── Complejidad (1-5) ───
    complexity = 1
    if primary in ("reasoning_hard", "prediction"):
        complexity = 4
    elif primary in ("agentic_sql", "coding"):
        complexity = 3
    elif primary in ("rag_qa", "extraction", "creative_writing"):
        complexity = 2

    if len(secondary) > 2:
        complexity = min(5, complexity + 1)
    if len_bonus > 0:
        complexity = min(5, complexity + 1)

    # ─── Confianza ───
    total_matches = sum(family_scores.values())
    if total_matches == 0:
        confidence = 0.4
    elif primary_score >= 3:
        confidence = 0.9
    elif primary_score >= 2:
        confidence = 0.75
    elif primary_score >= 1:
        confidence = 0.6
    else:
        confidence = 0.5

    # ─── Low cost ───
    needs_low_cost = primary in ("cheap_chat", "extraction") or (
        complexity <= 2 and not predictive_intent
    )

    return TaskClassification(
        primary_task_family=primary,
        secondary_capabilities=secondary,
        language=language,
        needs_tools=needs_tools,
        needs_json=needs_json_out,
        needs_long_context=needs_long_ctx,
        needs_low_cost=needs_low_cost,
        predictive_intent=predictive_intent,
        complexity=complexity,
        confidence=confidence,
    )
