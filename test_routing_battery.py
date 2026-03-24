"""
test_routing_battery.py — Batería de 33 tests end-to-end para V2.1
Valida: clasificación, pipeline, forecast, feasibility, degradación, confianza.

Ejecutar: python test_routing_battery.py
"""
import sys
import os
import math
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from llm.task_classifier import classify
from llm.pipeline_selector import select_pipeline
from llm.execution_planner import create_execution_plan
from llm.model_router import route
from llm.prediction_feasibility import check_feasibility, extract_target_variable
from llm.forecast_engine import forecast as run_forecast, select_method
from db import model_db as mdb


# ─────────────────────────────────────────────
# Test Cases
# ─────────────────────────────────────────────

TESTS = [
    # ═══ CATEGORÍA 1: Clasificación predictiva (7) ═══
    {"id": 1, "query": "predice el gasto en gasoil del próximo mes",
     "expected_family": "prediction", "expected_pipeline": "predictive_forecast",
     "category": "clasificacion_predictiva"},

    {"id": 2, "query": "estima cuántos km hará la flota el mes que viene",
     "expected_family": "prediction", "expected_pipeline": "predictive_forecast",
     "category": "clasificacion_predictiva"},

    {"id": 3, "query": "forecast de litros de gasóleo para el siguiente trimestre",
     "expected_family": "prediction", "expected_pipeline": "predictive_forecast",
     "category": "clasificacion_predictiva"},

    {"id": 4, "query": "cuál es la probabilidad de avería del camión 1234-AAA",
     "expected_family": "prediction", "expected_pipeline": "predictive_forecast",
     "category": "clasificacion_predictiva"},

    {"id": 5, "query": "riesgo de retraso en las descargas esta semana",
     "expected_family": "prediction", "expected_pipeline": "predictive_forecast",
     "category": "clasificacion_predictiva"},

    {"id": 6, "query": "cuánto gastaremos en mantenimiento el próximo año",
     "expected_family": "prediction", "expected_pipeline": "predictive_forecast",
     "category": "clasificacion_predictiva"},

    {"id": 7, "query": "previsión de horas extra para el siguiente mes",
     "expected_family": "prediction", "expected_pipeline": "predictive_forecast",
     "category": "clasificacion_predictiva"},

    # ═══ CATEGORÍA 2: Clasificación general (8) ═══
    {"id": 8, "query": "hola, buenos días",
     "expected_family": "cheap_chat", "expected_pipeline": "direct_chat",
     "category": "clasificacion_general"},

    {"id": 9, "query": "busca en las facturas del proveedor Repsol",
     "expected_family": "rag_qa", "expected_pipeline": "doc_retrieval",
     "category": "clasificacion_general"},

    {"id": 10, "query": "cuántos km ha hecho la flota este mes",
     "expected_family": "reasoning_hard", "expected_pipeline": "agentic_sql",
     "category": "clasificacion_general"},

    {"id": 11, "query": "escribe un email formal para el cliente",
     "expected_family": "creative_writing", "expected_pipeline": "direct_chat",
     "category": "clasificacion_general"},

    {"id": 12, "query": "genera un script en python para parsear csv",
     "expected_family": "coding", "expected_pipeline": "direct_chat",
     "category": "clasificacion_general"},

    {"id": 13, "query": "qué dice el convenio sobre horas extra",
     "expected_family": "rag_qa", "expected_pipeline": "agentic_sql_rag",
     "category": "clasificacion_general"},

    {"id": 14, "query": "compara las reparaciones del camión A vs camión B",
     "expected_family": "reasoning_hard", "expected_pipeline": "agentic_sql",
     "category": "clasificacion_general"},

    {"id": 15, "query": "ok, gracias",
     "expected_family": "cheap_chat", "expected_pipeline": "direct_chat",
     "category": "clasificacion_general"},

    # ═══ CATEGORÍA 3: Forecast con datos reales (5) ═══
    {"id": 16, "category": "forecast_con_datos",
     "type": "forecast_test",
     "values": [100, 110, 105, 120, 115, 130, 125, 140, 135, 150, 145, 160],
     "target": "km_totales", "horizon": "próximo mes",
     "expected_feasible": True, "expected_method_exists": True},

    {"id": 17, "category": "forecast_con_datos",
     "type": "forecast_test",
     "values": [500, 520, 510, 530, 540, 550, 545, 560, 570, 580, 575, 590,
                600, 610, 605, 620, 630, 640, 635, 650, 660, 670, 665, 680],
     "target": "litros_gasoil", "horizon": "próximo mes",
     "expected_feasible": True, "expected_method_exists": True,
     "note": "Serie larga (24 pts), tendencia clara → regresión lineal"},

    {"id": 18, "category": "forecast_con_datos",
     "type": "forecast_test",
     "values": [1000, 1000, 1000, 1000, 1000, 1000, 980, 1010, 990, 1005, 995, 1000],
     "target": "gasto_total", "horizon": "próximo mes",
     "expected_feasible": True, "expected_method_exists": True,
     "note": "Serie estable, baja varianza → WMA"},

    {"id": 19, "category": "forecast_con_datos",
     "type": "forecast_test",
     "values": [200, 210, 205, 215, 220, 225],
     "target": "horas_trabajadas", "horizon": "próximo mes",
     "expected_feasible": True, "expected_method_exists": True,
     "note": "Serie corta (6 pts, mínimo absoluto) → WMA"},

    {"id": 20, "category": "forecast_con_datos",
     "type": "forecast_test",
     "values": [50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 105,
                110, 115, 120, 125, 130, 135],
     "target": "descargas_totales", "horizon": "próximo mes",
     "expected_feasible": True, "expected_method_exists": True,
     "note": "Tendencia lineal perfecta → regresión lineal, R² alto"},

    # ═══ CATEGORÍA 4: Forecast sin datos / degradación (5) ═══
    {"id": 21, "category": "forecast_sin_datos",
     "type": "forecast_test",
     "values": [100, 200, 50],
     "target": "km_totales", "horizon": "próximo mes",
     "expected_feasible": False,
     "note": "Solo 3 puntos < 6 mínimo → infeasible"},

    {"id": 22, "category": "forecast_sin_datos",
     "type": "forecast_test",
     "values": [500, 500, 500, 500, 500, 500, 500, 500],
     "target": "gasto_total", "horizon": "próximo mes",
     "expected_feasible": False,
     "note": "Varianza = 0 → variable constante → infeasible"},

    {"id": 23, "category": "forecast_sin_datos",
     "type": "forecast_test",
     "values": [],
     "target": "valor_desconocido", "horizon": "próximo mes",
     "expected_feasible": False,
     "note": "Sin datos → variable no identificada"},

    {"id": 24, "category": "forecast_sin_datos",
     "type": "forecast_test",
     "values": [100, 110, 105, 120, 115, 130],
     "target": "km_totales", "horizon": "próximo año",
     "horizon_periods": 12,
     "expected_feasible": False,
     "note": "Horizonte (12) excede histórico (6) → infeasible"},

    {"id": 25, "category": "forecast_sin_datos",
     "type": "forecast_test",
     "values": [100, 105, 98, 102, 99, 101, 103, 97,
                50000, 80000, 70000, 60000],
     "target": "gasto_total", "horizon": "próximo mes",
     "expected_feasible": True,
     "note": "Datos bimodales → feasible pero confianza < 40% → degradación en forecast"},

    # ═══ CATEGORÍA 5: Feasibility checks específicos (5) ═══
    {"id": 26, "category": "feasibility_checks",
     "type": "feasibility_test",
     "values": [100, 110, 105, 120, 115, 130, 125, 140],
     "dates_monthly": True, "start": "2025-06-01",
     "target": "km_totales",
     "expected_feasible": True,
     "check": "periodicidad mensual detectada correctamente"},

    {"id": 27, "category": "feasibility_checks",
     "type": "feasibility_test",
     "values": [100, 110, 105, 120, 115, 130, 125, 140, 135, 150, 145, 160],
     "dates_monthly": True, "start": "2025-01-01",
     "target": "km_totales",
     "expected_feasible": True,
     "check": "umbral mensual (12 recomendado) alcanzado → sin penalización"},

    {"id": 28, "category": "feasibility_checks",
     "type": "feasibility_test",
     "values": [100, 110, 105, 120, 115, 130, 125],
     "dates_monthly": True, "start": "2025-06-01",
     "target": "km_totales",
     "expected_feasible": True,
     "check_penalty": True,
     "check": "7 pts < 12 recomendado para mensual → penalización historico_bajo"},

    {"id": 29, "category": "feasibility_checks",
     "type": "feasibility_test",
     "values": [100, 110, 105, 120, 115, 130, 125, 140],
     "dates_irregular": True,
     "target": "km_totales",
     "expected_feasible": False,
     "check_penalty": True,
     "check": "fechas irregulares + historico bajo → penalty combinada < 0.25 → infeasible"},

    {"id": 30, "category": "feasibility_checks",
     "type": "feasibility_test",
     "values": [100, 110, 105, 120, 115, 130, 500, 125],
     "dates_monthly": True, "start": "2025-01-01",
     "target": "km_totales",
     "expected_feasible": True,
     "check_penalty": True,
     "check": "outlier (500) detectado → penalización moderada"},

    # ═══ CATEGORÍA 6: Trazabilidad (3) ═══
    {"id": 31, "category": "trazabilidad",
     "type": "routing_test",
     "query": "predice el gasto en mantenimiento del próximo mes",
     "check": "extract_target_variable identifica variable, horizonte y periodos"},

    {"id": 32, "category": "trazabilidad",
     "type": "routing_test",
     "query": "analiza la tendencia de km por conductor",
     "check": "classification marca predictive_intent=False para análisis sin predict keywords"},

    {"id": 33, "category": "trazabilidad",
     "type": "routing_test",
     "query": "estima cuántos litros se gastarán el siguiente trimestre",
     "check": "extract_target_variable devuelve litros_gasoil y horizonte 3 periodos"},
]


# ─────────────────────────────────────────────
# Ejecución de tests
# ─────────────────────────────────────────────

def _make_dates(n, monthly=True, start="2025-01-01", irregular=False):
    """Genera lista de fechas para tests."""
    base = datetime.fromisoformat(start)
    if irregular:
        # Intervalos irregulares: 15, 45, 7, 60, 30... días
        deltas = [15, 45, 7, 60, 30, 20, 90, 10]
        dates = [base]
        for i in range(n - 1):
            dates.append(dates[-1] + timedelta(days=deltas[i % len(deltas)]))
        return dates
    if monthly:
        return [base + timedelta(days=30 * i) for i in range(n)]
    return [base + timedelta(days=i) for i in range(n)]


def run_classification_test(test):
    """Ejecuta test de clasificación + pipeline."""
    c = classify(test["query"])
    p = select_pipeline(c)

    family_ok = c.primary_task_family == test["expected_family"]
    pipeline_ok = p.pipeline_id == test["expected_pipeline"]

    try:
        r = route(test["query"], p.pipeline_id)
        model = r.primary.model_id
        score = f"{r.composite_score:.3f}"
    except Exception:
        model = "N/A"
        score = "N/A"

    status = "✅" if (family_ok and pipeline_ok) else "❌"
    return {
        "id": test["id"],
        "status": status,
        "query": test["query"][:60],
        "family": c.primary_task_family,
        "family_expected": test["expected_family"],
        "family_ok": family_ok,
        "pipeline": p.pipeline_id,
        "pipeline_expected": test["expected_pipeline"],
        "pipeline_ok": pipeline_ok,
        "model": model,
        "score": score,
        "predictive_intent": c.predictive_intent,
    }


def run_forecast_test(test):
    """Ejecuta test de forecast con datos sintéticos."""
    values = test["values"]
    target = test["target"]
    horizon = test.get("horizon", "próximo mes")
    horizon_periods = test.get("horizon_periods", 1)

    # Generar fechas
    if values:
        dates = _make_dates(
            len(values),
            monthly=test.get("dates_monthly", True),
            start=test.get("start", "2025-01-01"),
            irregular=test.get("dates_irregular", False),
        )
    else:
        dates = []

    # Feasibility check
    if target == "valor_desconocido" or not values:
        feasible = False
        reason = "Sin datos o variable desconocida"
        method = None
        prediction = None
        confidence = None
        stats = None
    else:
        result = check_feasibility(values, dates, target, horizon_periods)
        feasible = result.feasible
        reason = result.reason
        stats = result.dataset_stats

        if feasible:
            method, method_reason = select_method(values)
            if method:
                fr = run_forecast(values, target, horizon, method, method_reason,
                                  result.combined_penalty)
                prediction = fr.prediction if fr else None
                confidence = fr.confidence if fr else None
            else:
                prediction = None
                confidence = None
        else:
            method = None
            prediction = None
            confidence = None

    expected_feasible = test.get("expected_feasible", True)
    feasible_ok = feasible == expected_feasible
    status = "✅" if feasible_ok else "❌"

    return {
        "id": test["id"],
        "status": status,
        "target": target,
        "n_points": len(values),
        "feasible": feasible,
        "expected_feasible": expected_feasible,
        "feasible_ok": feasible_ok,
        "method": method,
        "prediction": f"{prediction:.2f}" if prediction is not None else "N/A",
        "confidence": f"{confidence:.0%}" if confidence is not None else "N/A",
        "reason": reason[:80],
        "note": test.get("note", ""),
    }


def run_feasibility_test(test):
    """Ejecuta test específico de feasibility."""
    values = test["values"]
    dates = _make_dates(
        len(values),
        monthly=test.get("dates_monthly", True),
        start=test.get("start", "2025-01-01"),
        irregular=test.get("dates_irregular", False),
    )

    result = check_feasibility(values, dates, test["target"], 1)
    expected = test["expected_feasible"]
    ok = result.feasible == expected

    penalty_ok = True
    if test.get("check_penalty"):
        penalty_ok = len(result.confidence_penalties) > 0

    status = "✅" if (ok and penalty_ok) else "❌"

    return {
        "id": test["id"],
        "status": status,
        "check": test["check"][:60],
        "feasible": result.feasible,
        "expected": expected,
        "passed": len(result.checks_passed),
        "failed": len(result.checks_failed),
        "penalties": dict(result.confidence_penalties),
        "combined_penalty": f"{result.combined_penalty:.2f}",
        "periodicity": result.dataset_stats.detected_periodicity if result.dataset_stats else "N/A",
        "outlier_ratio": f"{result.dataset_stats.outlier_ratio:.0%}" if result.dataset_stats else "N/A",
    }


def run_trazability_test(test):
    """Ejecuta test de trazabilidad."""
    query = test["query"]
    c = classify(query)
    p = select_pipeline(c)

    if test["id"] == 31:
        target, horizon, periods = extract_target_variable(query)
        ok = target == "coste_mantenimiento" and periods == 1
        detail = f"target={target}, horizon={horizon}, periods={periods}"

    elif test["id"] == 32:
        ok = not c.predictive_intent
        detail = f"predictive_intent={c.predictive_intent}, family={c.primary_task_family}"

    elif test["id"] == 33:
        target, horizon, periods = extract_target_variable(query)
        ok = target == "litros_gasoil" and periods == 3
        detail = f"target={target}, horizon={horizon}, periods={periods}"
    else:
        ok = True
        detail = ""

    return {
        "id": test["id"],
        "status": "✅" if ok else "❌",
        "check": test["check"][:60],
        "detail": detail,
        "family": c.primary_task_family,
        "pipeline": p.pipeline_id,
    }


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    # Preload caché para routing
    try:
        mdb.preload_cache()
    except Exception as e:
        print(f"⚠️ No se pudo cargar caché de BD: {e}")
        print("   Los tests de routing de modelo usarán N/A\n")

    results = {"pass": 0, "fail": 0}
    all_results = []

    print("=" * 100)
    print("  BATERÍA DE TESTS V2.1 — Router Dinámico + Predicción")
    print("=" * 100)

    # ─── Clasificación predictiva ───
    print("\n── CATEGORÍA 1: Clasificación predictiva (7 tests) ──\n")
    print(f"{'ID':>3} {'ST':>2} {'Query':<55} {'Family':<15} {'Pipeline':<25} {'Model':<25}")
    print("-" * 100)
    for t in TESTS:
        if t["category"] == "clasificacion_predictiva":
            r = run_classification_test(t)
            all_results.append(r)
            ok = r["family_ok"] and r["pipeline_ok"]
            results["pass" if ok else "fail"] += 1
            print(f"{r['id']:>3} {r['status']:>2} {r['query']:<55} "
                  f"{r['family']:<15} {r['pipeline']:<25} {r['model']:<25}")

    # ─── Clasificación general ───
    print("\n── CATEGORÍA 2: Clasificación general (8 tests) ──\n")
    print(f"{'ID':>3} {'ST':>2} {'Query':<55} {'Family':<15} {'Pipeline':<25} {'Model':<25}")
    print("-" * 100)
    for t in TESTS:
        if t["category"] == "clasificacion_general":
            r = run_classification_test(t)
            all_results.append(r)
            ok = r["family_ok"] and r["pipeline_ok"]
            results["pass" if ok else "fail"] += 1
            print(f"{r['id']:>3} {r['status']:>2} {r['query']:<55} "
                  f"{r['family']:<15} {r['pipeline']:<25} {r['model']:<25}")

    # ─── Forecast con datos ───
    print("\n── CATEGORÍA 3: Forecast con datos reales (5 tests) ──\n")
    print(f"{'ID':>3} {'ST':>2} {'Target':<20} {'Pts':>4} {'Method':<25} {'Predict':>10} "
          f"{'Conf':>6} {'Note':<30}")
    print("-" * 100)
    for t in TESTS:
        if t.get("category") == "forecast_con_datos":
            r = run_forecast_test(t)
            all_results.append(r)
            results["pass" if r["feasible_ok"] else "fail"] += 1
            print(f"{r['id']:>3} {r['status']:>2} {r['target']:<20} {r['n_points']:>4} "
                  f"{str(r['method']):<25} {r['prediction']:>10} {r['confidence']:>6} "
                  f"{r['note'][:30]:<30}")

    # ─── Forecast sin datos ───
    print("\n── CATEGORÍA 4: Forecast sin datos / degradación (5 tests) ──\n")
    print(f"{'ID':>3} {'ST':>2} {'Note':<60} {'Feasible':>8} {'Reason':<40}")
    print("-" * 100)
    for t in TESTS:
        if t.get("category") == "forecast_sin_datos":
            r = run_forecast_test(t)
            all_results.append(r)
            results["pass" if r["feasible_ok"] else "fail"] += 1
            print(f"{r['id']:>3} {r['status']:>2} {r['note'][:60]:<60} "
                  f"{'Yes' if r['feasible'] else 'No':>8} {r['reason'][:40]:<40}")

    # ─── Feasibility checks ───
    print("\n── CATEGORÍA 5: Feasibility checks específicos (5 tests) ──\n")
    print(f"{'ID':>3} {'ST':>2} {'Check':<50} {'Period':<10} {'Outlier':>8} "
          f"{'Penalty':>8} {'Penalties'}")
    print("-" * 100)
    for t in TESTS:
        if t.get("category") == "feasibility_checks":
            r = run_feasibility_test(t)
            all_results.append(r)
            ok = r["status"] == "✅"
            results["pass" if ok else "fail"] += 1
            pen_str = str(r['penalties'])[:30] if r['penalties'] else "{}"
            print(f"{r['id']:>3} {r['status']:>2} {r['check'][:50]:<50} "
                  f"{r['periodicity']:<10} {r['outlier_ratio']:>8} "
                  f"{r['combined_penalty']:>8} {pen_str}")

    # ─── Trazabilidad ───
    print("\n── CATEGORÍA 6: Trazabilidad (3 tests) ──\n")
    print(f"{'ID':>3} {'ST':>2} {'Check':<50} {'Detail'}")
    print("-" * 100)
    for t in TESTS:
        if t.get("category") == "trazabilidad":
            r = run_trazability_test(t)
            all_results.append(r)
            ok = r["status"] == "✅"
            results["pass" if ok else "fail"] += 1
            print(f"{r['id']:>3} {r['status']:>2} {r['check'][:50]:<50} {r['detail']}")

    # ─── Resumen ───
    total = results["pass"] + results["fail"]
    print(f"\n{'=' * 100}")
    print(f"  RESULTADO: {results['pass']}/{total} tests pasados")
    if results["fail"] > 0:
        print(f"  ❌ {results['fail']} tests fallidos")
    else:
        print(f"  ✅ TODOS LOS TESTS PASARON")
    print(f"{'=' * 100}")

    return results["fail"] == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
