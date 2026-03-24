"""
llm/prediction_feasibility.py — Validación de viabilidad predictiva V2.1
9 checks obligatorios antes de ejecutar forecast.
Si falla algún check crítico, degrada a predictive_insight o agentic_sql.
"""
import math
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timedelta


@dataclass
class DatasetStats:
    """Estadísticas del dataset analizado."""
    size: int = 0
    min_val: float = 0.0
    max_val: float = 0.0
    mean: float = 0.0
    std: float = 0.0
    cv: float = 0.0  # coeficiente de variación
    variance: float = 0.0
    detected_periodicity: str = "unknown"  # daily, weekly, monthly, irregular
    gap_ratio: float = 0.0  # % de gaps en la serie
    outlier_ratio: float = 0.0  # % de outliers (IQR)
    granularity_cv: float = 0.0  # CV de deltas entre periodos
    null_ratio: float = 0.0


@dataclass
class FeasibilityResult:
    """Resultado de la validación de viabilidad."""
    feasible: bool
    checks_passed: list[str] = field(default_factory=list)
    checks_failed: list[str] = field(default_factory=list)
    reason: str = ""
    degraded_to: Optional[str] = None  # pipeline al que degradar
    dataset_stats: Optional[DatasetStats] = None
    confidence_penalties: dict = field(default_factory=dict)  # penalty_name → factor
    combined_penalty: float = 1.0  # multiplicador final sobre confidence


# ─────────────────────────────────────────────
# Umbrales por periodicidad
# ─────────────────────────────────────────────

PERIODICITY_THRESHOLDS = {
    "daily":   {"abs_min": 6, "recommended": 30, "expected_delta_days": 1},
    "weekly":  {"abs_min": 6, "recommended": 16, "expected_delta_days": 7},
    "monthly": {"abs_min": 6, "recommended": 12, "expected_delta_days": 30},
    "unknown": {"abs_min": 6, "recommended": 12, "expected_delta_days": 30},
}


# ─────────────────────────────────────────────
# Detección de periodicidad
# ─────────────────────────────────────────────

def detect_periodicity(dates: list[datetime]) -> str:
    """Detecta la periodicidad de una serie temporal por los deltas entre puntos."""
    if len(dates) < 3:
        return "unknown"

    sorted_dates = sorted(dates)
    deltas = [(sorted_dates[i+1] - sorted_dates[i]).days for i in range(len(sorted_dates)-1)]
    deltas = [d for d in deltas if d > 0]  # ignorar duplicados

    if not deltas:
        return "unknown"

    mean_delta = sum(deltas) / len(deltas)

    if mean_delta <= 2:
        return "daily"
    elif 5 <= mean_delta <= 10:
        return "weekly"
    elif 25 <= mean_delta <= 35:
        return "monthly"
    else:
        return "irregular"


def _compute_cv(values: list[float]) -> float:
    """Coeficiente de variación."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std = math.sqrt(variance)
    return std / abs(mean)


def _detect_outliers_iqr(values: list[float]) -> tuple[int, float]:
    """Detecta outliers por IQR. Devuelve (count, ratio).
    Si IQR=0 pero hay valores distintos, usa fallback por desviación estándar."""
    if len(values) < 4:
        return 0, 0.0

    sorted_vals = sorted(values)
    n = len(sorted_vals)
    q1 = sorted_vals[n // 4]
    q3 = sorted_vals[(3 * n) // 4]
    iqr = q3 - q1

    if iqr == 0:
        # Fallback: si todos iguales → 0 outliers
        # Si hay valores distintos → usar 2 desviaciones estándar
        if min(sorted_vals) == max(sorted_vals):
            return 0, 0.0
        mean = sum(sorted_vals) / n
        variance = sum((v - mean) ** 2 for v in sorted_vals) / n
        std = math.sqrt(variance)
        if std == 0:
            return 0, 0.0
        lower = mean - 2 * std
        upper = mean + 2 * std
        outliers = sum(1 for v in sorted_vals if v < lower or v > upper)
        return outliers, outliers / n

    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    outliers = sum(1 for v in sorted_vals if v < lower or v > upper)
    return outliers, outliers / n


# ─────────────────────────────────────────────
# Validación principal
# ─────────────────────────────────────────────

def check_feasibility(
    values: list[float],
    dates: list[datetime] = None,
    target_variable: str = None,
    horizon_periods: int = 1,
) -> FeasibilityResult:
    """
    Ejecuta 9 validaciones sobre un dataset para predicción.
    
    Args:
        values: lista de valores numéricos de la variable objetivo
        dates: lista de fechas correspondientes (misma longitud que values)
        target_variable: nombre de la variable a predecir
        horizon_periods: cuántos periodos hacia el futuro predecir
    
    Returns:
        FeasibilityResult con feasible=True/False y detalle de cada check
    """
    stats = DatasetStats()
    passed = []
    failed = []
    penalties = {}

    # Filtrar nulls
    valid_pairs = []
    null_count = 0
    for i, v in enumerate(values):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            null_count += 1
        else:
            d = dates[i] if dates and i < len(dates) else None
            valid_pairs.append((d, float(v)))

    stats.null_ratio = null_count / len(values) if values else 1.0
    clean_values = [p[1] for p in valid_pairs]
    clean_dates = [p[0] for p in valid_pairs if p[0] is not None]
    stats.size = len(clean_values)

    # ─── CHECK 1: Variable objetivo identificable ───
    if target_variable and len(target_variable.strip()) > 0:
        passed.append("variable_objetivo: identificada")
    else:
        failed.append("variable_objetivo: no se pudo identificar la variable a predecir")
        return FeasibilityResult(
            feasible=False,
            checks_passed=passed,
            checks_failed=failed,
            reason="Variable objetivo no identificada",
            degraded_to="agentic_sql",
            dataset_stats=stats,
        )

    # ─── CHECK 2: Histórico mínimo absoluto (6 puntos) ───
    if stats.size >= 6:
        passed.append(f"historico_minimo: {stats.size} puntos ≥ 6")
    else:
        failed.append(f"historico_minimo: {stats.size} puntos < 6 (mínimo absoluto)")
        return FeasibilityResult(
            feasible=False,
            checks_passed=passed,
            checks_failed=failed,
            reason=f"Dataset insuficiente ({stats.size} puntos, mínimo 6)",
            degraded_to="predictive_insight",
            dataset_stats=stats,
        )

    # Estadísticas básicas
    stats.min_val = min(clean_values)
    stats.max_val = max(clean_values)
    stats.mean = sum(clean_values) / len(clean_values)
    stats.variance = sum((v - stats.mean) ** 2 for v in clean_values) / len(clean_values)
    stats.std = math.sqrt(stats.variance)
    stats.cv = stats.std / abs(stats.mean) if stats.mean != 0 else 0.0

    # ─── CHECK 3: Varianza > 0 ───
    if stats.variance > 0:
        passed.append(f"varianza: {stats.variance:.4f} > 0")
    else:
        failed.append("varianza: variable constante (varianza = 0)")
        return FeasibilityResult(
            feasible=False,
            checks_passed=passed,
            checks_failed=failed,
            reason="Variable constante — sin variación para predecir",
            degraded_to="predictive_insight",
            dataset_stats=stats,
        )

    # ─── CHECK 4: Periodicidad detectada ───
    if clean_dates and len(clean_dates) >= 3:
        periodicity = detect_periodicity(clean_dates)
    else:
        periodicity = "unknown"
    stats.detected_periodicity = periodicity

    if periodicity != "irregular":
        passed.append(f"periodicidad: {periodicity}")
    else:
        failed.append("periodicidad: irregular — serie no periódica")
        penalties["periodicidad_irregular"] = 0.5
        # No degradar inmediatamente, pero penalizar fuerte

    # ─── CHECK 5: Umbral por periodicidad (recomendable) ───
    thresholds = PERIODICITY_THRESHOLDS.get(periodicity, PERIODICITY_THRESHOLDS["unknown"])
    recommended = thresholds["recommended"]
    if stats.size >= recommended:
        passed.append(f"umbral_periodicidad: {stats.size} ≥ {recommended} (recomendado para {periodicity})")
    else:
        penalty = 0.6  # penalización fuerte
        failed.append(f"umbral_periodicidad: {stats.size} < {recommended} (recomendado para {periodicity})")
        penalties["historico_bajo"] = penalty

    # ─── CHECK 6: Continuidad temporal ───
    if clean_dates and len(clean_dates) >= 3:
        sorted_dates = sorted(clean_dates)
        deltas_days = [(sorted_dates[i+1] - sorted_dates[i]).days
                       for i in range(len(sorted_dates)-1)]
        expected_delta = thresholds["expected_delta_days"]
        expected_count = 0
        for d in deltas_days:
            if d <= expected_delta * 1.5:  # tolerancia 50%
                expected_count += 1
        total_deltas = len(deltas_days)
        gap_ratio = 1.0 - (expected_count / total_deltas) if total_deltas > 0 else 1.0
        stats.gap_ratio = gap_ratio

        if gap_ratio <= 0.20:
            passed.append(f"continuidad: {gap_ratio:.0%} gaps ≤ 20%")
        else:
            failed.append(f"continuidad: {gap_ratio:.0%} gaps > 20%")
            if gap_ratio > 0.40:
                penalties["gaps_severos"] = 0.4
                return FeasibilityResult(
                    feasible=False,
                    checks_passed=passed,
                    checks_failed=failed,
                    reason=f"Serie con {gap_ratio:.0%} de gaps — demasiados huecos",
                    degraded_to="predictive_insight",
                    dataset_stats=stats,
                    confidence_penalties=penalties,
                )
            else:
                penalties["gaps_moderados"] = 0.7
    else:
        passed.append("continuidad: sin fechas para validar (asumiendo OK)")

    # ─── CHECK 7: Granularidad consistente ───
    if clean_dates and len(clean_dates) >= 3:
        sorted_dates = sorted(clean_dates)
        deltas = [(sorted_dates[i+1] - sorted_dates[i]).days
                  for i in range(len(sorted_dates)-1)]
        deltas_f = [float(d) for d in deltas if d > 0]
        granularity_cv = _compute_cv(deltas_f) if deltas_f else 0.0
        stats.granularity_cv = granularity_cv

        if granularity_cv < 0.5:
            passed.append(f"granularidad: CV={granularity_cv:.2f} < 0.5")
        else:
            failed.append(f"granularidad: CV={granularity_cv:.2f} ≥ 0.5 — inconsistente")
            penalties["granularidad_inconsistente"] = 0.6
    else:
        passed.append("granularidad: sin fechas para validar")

    # ─── CHECK 8: Outliers ───
    outlier_count, outlier_ratio = _detect_outliers_iqr(clean_values)
    stats.outlier_ratio = outlier_ratio

    if outlier_ratio < 0.15:
        passed.append(f"outliers: {outlier_ratio:.0%} < 15%")
    elif outlier_ratio < 0.30:
        failed.append(f"outliers: {outlier_ratio:.0%} — entre 15% y 30%")
        penalties["outliers_moderados"] = 0.7
    else:
        failed.append(f"outliers: {outlier_ratio:.0%} > 30% — demasiados outliers")
        return FeasibilityResult(
            feasible=False,
            checks_passed=passed,
            checks_failed=failed,
            reason=f"Demasiados outliers ({outlier_ratio:.0%}) — datos no fiables",
            degraded_to="predictive_insight",
            dataset_stats=stats,
            confidence_penalties=penalties,
        )

    # ─── CHECK 9: Horizonte ≤ periodos disponibles ───
    if horizon_periods <= stats.size:
        passed.append(f"horizonte: {horizon_periods} ≤ {stats.size} periodos")
    else:
        failed.append(f"horizonte: {horizon_periods} > {stats.size} periodos — excede histórico")
        return FeasibilityResult(
            feasible=False,
            checks_passed=passed,
            checks_failed=failed,
            reason=f"Horizonte ({horizon_periods}) excede el histórico ({stats.size})",
            degraded_to="predictive_insight",
            dataset_stats=stats,
        )

    # ─── Calcular penalización combinada ───
    combined = 1.0
    for name, factor in penalties.items():
        combined *= factor

    # Si la penalización combinada baja demasiado, no es viable
    if combined < 0.25:
        return FeasibilityResult(
            feasible=False,
            checks_passed=passed,
            checks_failed=failed,
            reason=f"Penalización acumulada ({combined:.2f}) demasiado alta — datos no fiables",
            degraded_to="predictive_insight",
            dataset_stats=stats,
            confidence_penalties=penalties,
            combined_penalty=combined,
        )

    return FeasibilityResult(
        feasible=True,
        checks_passed=passed,
        checks_failed=failed,
        reason=f"Dataset viable ({stats.size} puntos, {stats.detected_periodicity}, "
               f"penalty={combined:.2f})",
        degraded_to=None,
        dataset_stats=stats,
        confidence_penalties=penalties,
        combined_penalty=combined,
    )


def extract_target_variable(query: str) -> tuple[str, str, int]:
    """
    Extrae variable objetivo, horizonte y periodos del query.
    Devuelve (target_variable, horizon_label, horizon_periods).
    """
    q = query.lower()

    # Ordenado de más específico a más genérico
    # (las keywords más específicas se evalúan primero)
    variable_map_ordered = [
        ("mantenimiento", "coste_mantenimiento"),
        ("reparaciones", "coste_mantenimiento"),
        ("gasoil", "litros_gasoil"),
        ("gasóleo", "litros_gasoil"),
        ("litros", "litros_gasoil"),
        ("kilómetros", "km_totales"),
        ("km", "km_totales"),
        ("descargas", "descargas_totales"),
        ("averías", "num_averias"),
        ("ausencias", "dias_ausencia"),
        ("vacaciones", "dias_vacaciones"),
        ("productividad", "km_por_hora"),
        ("rendimiento", "km_por_litro"),
        ("horas", "horas_trabajadas"),
        # Genéricos al final
        ("gasto", "gasto_total"),
        ("coste", "coste_total"),
    ]

    target = None
    for keyword, var in variable_map_ordered:
        if keyword in q:
            target = var
            break

    if not target:
        target = "valor_desconocido"

    # ─── Horizonte ───
    horizon_periods = 1
    horizon_label = "próximo periodo"

    if any(x in q for x in ["próximo mes", "siguiente mes", "mes que viene"]):
        horizon_label = "próximo mes"
        horizon_periods = 1
    elif any(x in q for x in ["próximo trimestre", "siguiente trimestre"]):
        horizon_label = "próximo trimestre"
        horizon_periods = 3
    elif any(x in q for x in ["próximo año", "siguiente año"]):
        horizon_label = "próximo año"
        horizon_periods = 12
    elif "3 meses" in q or "tres meses" in q:
        horizon_label = "próximos 3 meses"
        horizon_periods = 3

    return target, horizon_label, horizon_periods
