"""
llm/router.py — DESACTIVADO en producción (V3.0)

╔══════════════════════════════════════════════════════════╗
║  ESTE MÓDULO ESTÁ DESACTIVADO EN PRODUCCIÓN.            ║
║                                                          ║
║  La lógica de routing está ahora en:                     ║
║    - llm/task_classifier.py (clasificación)              ║
║    - llm/pipeline_selector.py (selección de pipeline)    ║
║    - llm/model_router.py (selección de modelo)           ║
║    - llm/orchestrator.py (orquestación central)          ║
║                                                          ║
║  NO IMPORTAR ESTE MÓDULO EN RUNTIME PRODUCTIVO.          ║
║  Solo mantenido para compatibilidad con scripts offline. ║
╚══════════════════════════════════════════════════════════╝
"""

import warnings as _warnings
_warnings.warn(
    "llm.router está DESACTIVADO en producción. "
    "Usa llm.orchestrator.run_orchestrated_pipeline() en su lugar.",
    DeprecationWarning,
    stacklevel=2,
)


def extract_mes_anio(question: str):
    """LEGACY — NO usar en producción. Solo para scripts offline."""
    import re
    from datetime import datetime

    meses = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    }

    q = question.lower()
    mes, anio = None, None

    for nombre, num in meses.items():
        if nombre in q:
            mes = num
            break

    match = re.search(r"20\d{2}", q)
    if match:
        anio = int(match.group())

    if "este mes" in q or "mes actual" in q:
        now = datetime.now()
        mes, anio = now.month, now.year

    return mes, anio


def extract_nombre_empleado(question: str):
    """LEGACY — NO usar en producción. Solo para scripts offline."""
    import re

    q = question.lower()
    patterns = [
        r"(?:de|para|del?)\s+(?:el\s+)?(?:empleado|conductor|trabajador|mecánico)\s+(\w+(?:\s+\w+)?)",
        r"(?:cómo|que|qué)\s+(?:ha|tiene)\s+(?:trabajado|hecho)\s+(\w+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, q)
        if match:
            name = match.group(1).strip()
            excludes = {"el", "la", "un", "una", "los", "las", "este", "esta", "hoy"}
            if name not in excludes:
                return name
    return None


def classify_query(*args, **kwargs):
    """ELIMINADO — Usar llm.task_classifier.classify() en su lugar."""
    raise NotImplementedError(
        "classify_query ha sido eliminado. "
        "Usa llm.task_classifier.classify() + llm.pipeline_selector.select_pipeline()."
    )


def route_query(*args, **kwargs):
    """ELIMINADO — Usar llm.orchestrator.run_orchestrated_pipeline() en su lugar."""
    raise NotImplementedError(
        "route_query ha sido eliminado. "
        "Usa llm.orchestrator.run_orchestrated_pipeline()."
    )
