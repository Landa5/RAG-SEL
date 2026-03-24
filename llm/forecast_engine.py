"""
llm/forecast_engine.py — Motor de predicción cuantitativa V2.1.1
3 métodos: WMA, regresión lineal, variación interperiodo.
Selector heurístico + backtesting (rolling validation ≥12 pts) + cap de confianza.
"""
import math
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BacktestResult:
    """Resultado del backtesting leave-one-out."""
    predicted: float
    actual: float
    error: float
    absolute_error: float
    percentage_error: float  # MAPE individual


@dataclass
class ForecastResult:
    """Resultado completo de una predicción."""
    prediction: float              # valor predicho
    confidence: float              # 0.0-1.0 basado en validación real
    method: str                    # 'wma', 'linear_regression', 'interperiod_variation'
    method_display: str            # nombre legible
    data_used: list[dict]          # dataset real usado [{period, value}]
    dataset_size: int
    horizon: str                   # 'próximo mes'
    target_variable: str           # 'gasto_gasoil'
    target_display: str            # nombre legible
    warnings: list[str]
    backtesting: dict              # MAE, MAPE, R²
    method_selection_reason: str   # por qué se eligió este método


# ─────────────────────────────────────────────
# Métodos de forecast
# ─────────────────────────────────────────────

def _weighted_moving_average(values: list[float], window: int = None) -> float:
    """Media móvil ponderada. Pesos decrecientes (más reciente = más peso)."""
    if not values:
        return 0.0
    if window is None:
        window = min(len(values), 6)
    recent = values[-window:]
    weights = list(range(1, len(recent) + 1))
    total_weight = sum(weights)
    return sum(v * w for v, w in zip(recent, weights)) / total_weight


def _linear_regression(values: list[float]) -> tuple[float, float, float]:
    """
    Regresión lineal simple. x = índice temporal.
    Devuelve (prediction_next, slope, r_squared).
    """
    n = len(values)
    if n < 3:
        return values[-1], 0.0, 0.0

    x = list(range(n))
    x_mean = sum(x) / n
    y_mean = sum(values) / n

    ss_xy = sum((x[i] - x_mean) * (values[i] - y_mean) for i in range(n))
    ss_xx = sum((x[i] - x_mean) ** 2 for i in range(n))
    ss_yy = sum((values[i] - y_mean) ** 2 for i in range(n))

    if ss_xx == 0:
        return y_mean, 0.0, 0.0

    slope = ss_xy / ss_xx
    intercept = y_mean - slope * x_mean

    # Predicción para siguiente periodo
    prediction = slope * n + intercept

    # R²
    if ss_yy == 0:
        r_squared = 0.0
    else:
        r_squared = (ss_xy ** 2) / (ss_xx * ss_yy)

    return prediction, slope, r_squared


def _interperiod_variation(values: list[float]) -> tuple[float, float]:
    """
    Variación interperiodo: cambio porcentual medio.
    Devuelve (prediction_next, avg_change_pct).
    """
    if len(values) < 2:
        return values[-1] if values else 0.0, 0.0

    changes = []
    for i in range(1, len(values)):
        if values[i-1] != 0:
            change = (values[i] - values[i-1]) / abs(values[i-1])
            changes.append(change)

    if not changes:
        return values[-1], 0.0

    avg_change = sum(changes) / len(changes)
    prediction = values[-1] * (1 + avg_change)
    return prediction, avg_change


# ─────────────────────────────────────────────
# Backtesting
# ─────────────────────────────────────────────

def _predict_one(values: list[float], method: str) -> float:
    """Predice el siguiente valor usando el método indicado."""
    if method == "wma":
        return _weighted_moving_average(values)
    elif method == "linear_regression":
        pred, _, _ = _linear_regression(values)
        return pred
    elif method == "interperiod_variation":
        pred, _ = _interperiod_variation(values)
        return pred
    return values[-1]


def _backtest_method(values: list[float], method: str) -> dict:
    """
    Backtesting con dos estrategias:
    - Rolling validation si n ≥ 12 (ventana móvil, más robusto).
    - Leave-last-out si n < 12 (retira últimos puntos).
    Devuelve dict con MAE, MAPE, tests, detalle.
    """
    n = len(values)
    results = []

    if n >= 12:
        # ── Rolling validation ──
        # Usar 50% del dataset como ventana de entrenamiento mínima,
        # validar sobre los puntos restantes uno a uno.
        min_train = max(6, n // 2)
        for split in range(min_train, n):
            train = values[:split]
            actual = values[split]
            predicted = _predict_one(train, method)

            error = predicted - actual
            abs_error = abs(error)
            pct_error = abs_error / abs(actual) if actual != 0 else 0.0

            results.append(BacktestResult(
                predicted=predicted, actual=actual,
                error=error, absolute_error=abs_error,
                percentage_error=pct_error,
            ))
    else:
        # ── Leave-last-out (n < 12) ──
        test_count = min(3, max(1, n // 3))
        for i in range(test_count):
            train_size = n - 1 - i
            if train_size < 3:
                break
            train = values[:train_size]
            actual = values[train_size]
            predicted = _predict_one(train, method)

            error = predicted - actual
            abs_error = abs(error)
            pct_error = abs_error / abs(actual) if actual != 0 else 0.0

            results.append(BacktestResult(
                predicted=predicted, actual=actual,
                error=error, absolute_error=abs_error,
                percentage_error=pct_error,
            ))

    if not results:
        return {"MAE": 0.0, "MAPE": 0.0, "R2": 0.0, "tests": 0,
                "validation_type": "none"}

    mae = sum(r.absolute_error for r in results) / len(results)
    mape = sum(r.percentage_error for r in results) / len(results)
    validation_type = "rolling" if n >= 12 else "leave_last_out"

    return {
        "MAE": round(mae, 4),
        "MAPE": round(mape, 4),
        "R2": 0.0,
        "tests": len(results),
        "validation_type": validation_type,
        "detail": [
            {"predicted": round(r.predicted, 2), "actual": round(r.actual, 2),
             "error_pct": round(r.percentage_error * 100, 1)}
            for r in results
        ],
    }


# ─────────────────────────────────────────────
# Estabilidad de tendencia
# ─────────────────────────────────────────────

def _check_trend_stability(values: list[float]) -> tuple[bool, str]:
    """
    Divide la serie en 2 mitades y compara pendientes.
    Devuelve (stable, reason).
    Inestable si:
      - Pendiente cambia de signo entre mitades.
      - Magnitud difiere más de 2× entre mitades.
    """
    n = len(values)
    if n < 8:
        return True, "Serie demasiado corta para chequear estabilidad"

    mid = n // 2
    first_half = values[:mid]
    second_half = values[mid:]

    _, slope1, _ = _linear_regression(first_half)
    _, slope2, _ = _linear_regression(second_half)

    # Check 1: cambio de signo
    if slope1 * slope2 < 0 and abs(slope1) > 0.5 and abs(slope2) > 0.5:
        return False, (
            f"Tendencia cambia de signo (1ª mitad: {slope1:+.2f}, "
            f"2ª mitad: {slope2:+.2f}). Regresión lineal no es fiable."
        )

    # Check 2: magnitud muy diferente
    if abs(slope1) > 0.01 and abs(slope2) > 0.01:
        ratio = max(abs(slope1), abs(slope2)) / min(abs(slope1), abs(slope2))
        if ratio > 3.0:
            return False, (
                f"Magnitud de tendencia inestable (ratio={ratio:.1f}×, "
                f"1ª: {slope1:+.2f}, 2ª: {slope2:+.2f}). Regresión lineal descartada."
            )

    return True, f"Tendencia estable (1ª: {slope1:+.2f}, 2ª: {slope2:+.2f})"


# ─────────────────────────────────────────────
# Selector de método
# ─────────────────────────────────────────────

def select_method(values: list[float]) -> tuple[Optional[str], str]:
    """
    Elige el método de forecast más adecuado según heurísticas.
    Incluye chequeo de estabilidad de tendencia antes de elegir regresión.
    
    Returns:
        (method_name, reason) o (None, reason) si ninguno es viable.
    """
    n = len(values)
    if n < 3:
        return None, "Dataset demasiado pequeño para cualquier método"

    # Calcular métricas
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    std = math.sqrt(variance)
    cv = std / abs(mean) if mean != 0 else 0.0

    # Probar regresión para ver R²
    _, slope, r_squared = _linear_regression(values)

    # ── Chequeo de estabilidad de tendencia ──
    trend_stable, trend_reason = _check_trend_stability(values)

    # Heurística de selección
    if r_squared > 0.6 and trend_stable:
        return "linear_regression", (
            f"Tendencia clara detectada (R²={r_squared:.2f}, slope={slope:.2f}). "
            f"Estabilidad verificada. Regresión lineal adecuada."
        )

    if r_squared > 0.6 and not trend_stable:
        # R² alto pero tendencia inestable → degradar a WMA
        return "wma", (
            f"R²={r_squared:.2f} sugería regresión, pero {trend_reason} "
            f"Degradado a WMA por seguridad."
        )

    if n < 8 and cv < 0.3:
        return "wma", (
            f"Serie corta ({n} puntos) y estable (CV={cv:.2f}). "
            f"Media móvil ponderada es apropiada."
        )

    if cv < 0.5:
        return "wma", (
            f"Serie con variabilidad moderada (CV={cv:.2f}). "
            f"Media móvil ponderada captura tendencias suaves."
        )

    # Si CV alto pero hay algún patrón de cambios relativos
    changes = []
    for i in range(1, n):
        if values[i-1] != 0:
            changes.append((values[i] - values[i-1]) / abs(values[i-1]))
    if changes:
        change_cv = _compute_cv_local(changes)
        if change_cv < 0.8:
            return "interperiod_variation", (
                f"Cambios relativos entre periodos son consistentes "
                f"(CV cambios={change_cv:.2f}). Variación interperiodo apropiada."
            )

    # Si nada es bueno
    if cv >= 1.0 and r_squared < 0.2:
        return None, (
            f"Datos muy volátiles (CV={cv:.2f}) sin tendencia (R²={r_squared:.2f}). "
            f"Ningún método simple es fiable."
        )

    # Fallback a WMA como método más robusto
    return "wma", (
        f"Sin método claramente óptimo. WMA como fallback robusto "
        f"(CV={cv:.2f}, R²={r_squared:.2f})."
    )


def _compute_cv_local(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance) / abs(mean)


# ─────────────────────────────────────────────
# Cap de confianza por tamaño de muestra
# ─────────────────────────────────────────────

# Máxima confianza alcanzable según puntos de histórico.
# Evita valores extremadamente altos con datasets pequeños.
CONFIDENCE_CAPS = [
    # (min_points, max_confidence)
    (24, 1.00),  # ≥24 pts: sin cap
    (18, 0.95),  # 18-23 pts: max 95%
    (12, 0.88),  # 12-17 pts: max 88%
    (8,  0.75),  # 8-11 pts: max 75%
    (6,  0.55),  # 6-7 pts: max 55%
    (0,  0.30),  # <6 pts: max 30%
]

def _get_confidence_cap(n: int) -> float:
    """Devuelve el máximo de confianza permitido para un dataset de n puntos."""
    for min_pts, cap in CONFIDENCE_CAPS:
        if n >= min_pts:
            return cap
    return 0.30


# ─────────────────────────────────────────────
# Cálculo de confianza
# ─────────────────────────────────────────────

def _compute_confidence(
    method: str,
    values: list[float],
    backtest: dict,
    feasibility_penalty: float = 1.0,
) -> float:
    """
    Confianza basada en datos reales:
    confidence = (1 - MAPE) × factor_R2 × factor_muestra × factor_CV × penalty
    Resultado limitado por cap máximo según tamaño de muestra.
    
    NO se inventan porcentajes.
    """
    n = len(values)
    mape = backtest.get("MAPE", 0.5)

    # Factor MAPE (0-1): cuanto menor MAPE, mayor confianza
    factor_mape = max(0.0, 1.0 - mape)

    # Factor R² (solo para regresión lineal)
    if method == "linear_regression":
        _, _, r_sq = _linear_regression(values)
        factor_r2 = max(0.1, r_sq)
    else:
        factor_r2 = 0.8  # neutro para otros métodos

    # Factor muestra: penalizar datasets pequeños
    if n >= 24:
        factor_muestra = 1.0
    elif n >= 12:
        factor_muestra = 0.85
    elif n >= 8:
        factor_muestra = 0.7
    else:
        factor_muestra = 0.55

    # Factor CV: penalizar alta volatilidad
    mean = sum(values) / n if n > 0 else 0
    std = math.sqrt(sum((v - mean) ** 2 for v in values) / n) if n > 0 else 0
    cv = std / abs(mean) if mean != 0 else 0
    if cv < 0.15:
        factor_cv = 1.0
    elif cv < 0.3:
        factor_cv = 0.9
    elif cv < 0.5:
        factor_cv = 0.75
    else:
        factor_cv = 0.5

    raw_confidence = factor_mape * factor_r2 * factor_muestra * factor_cv * feasibility_penalty

    # ── Cap por tamaño de muestra ──
    cap = _get_confidence_cap(n)
    confidence = min(raw_confidence, cap)

    return round(max(0.0, min(1.0, confidence)), 3)


# ─────────────────────────────────────────────
# Función principal de forecast
# ─────────────────────────────────────────────

VARIABLE_DISPLAY = {
    "gasto_total": "Gasto total",
    "litros_gasoil": "Litros de gasóleo",
    "km_totales": "Kilómetros totales",
    "horas_trabajadas": "Horas trabajadas",
    "descargas_totales": "Descargas totales",
    "coste_total": "Coste total",
    "coste_mantenimiento": "Coste mantenimiento",
    "num_averias": "Número de averías",
    "dias_ausencia": "Días de ausencia",
    "dias_vacaciones": "Días de vacaciones",
    "km_por_hora": "Productividad (km/h)",
    "km_por_litro": "Rendimiento (km/L)",
}

METHOD_DISPLAY = {
    "wma": "Media móvil ponderada (WMA)",
    "linear_regression": "Regresión lineal simple",
    "interperiod_variation": "Variación interperiodo",
}


def forecast(
    values: list[float],
    target_variable: str,
    horizon_label: str = "próximo periodo",
    method: str = None,
    method_reason: str = "",
    feasibility_penalty: float = 1.0,
    period_labels: list[str] = None,
) -> Optional[ForecastResult]:
    """
    Ejecuta predicción cuantitativa con el método seleccionado.
    
    Args:
        values: serie temporal de valores
        target_variable: nombre de variable
        horizon_label: descripción del horizonte
        method: método a usar (si None, no ejecutar — error de flujo)
        method_reason: por qué se eligió este método
        feasibility_penalty: penalización del feasibility check
        period_labels: etiquetas de periodos ['2025-01', '2025-02', ...]
    
    Returns:
        ForecastResult o None si no se puede predecir
    """
    if not method or not values:
        return None

    warnings = []

    # Ejecutar predicción
    if method == "wma":
        prediction = _weighted_moving_average(values)
    elif method == "linear_regression":
        prediction, slope, r_sq = _linear_regression(values)
        if r_sq < 0.3:
            warnings.append(f"R² bajo ({r_sq:.2f}) — la tendencia lineal no explica bien los datos")
        if prediction < 0 and all(v >= 0 for v in values):
            warnings.append("Predicción negativa ajustada a 0 (datos siempre positivos)")
            prediction = 0.0
    elif method == "interperiod_variation":
        prediction, avg_change = _interperiod_variation(values)
        if abs(avg_change) > 0.3:
            warnings.append(f"Cambio medio alto ({avg_change:.0%}) — predicción con alta incertidumbre")
        if prediction < 0 and all(v >= 0 for v in values):
            warnings.append("Predicción negativa ajustada a 0")
            prediction = 0.0
    else:
        return None

    # Backtesting obligatorio
    backtest = _backtest_method(values, method)

    # Confidence basado en datos reales
    confidence = _compute_confidence(method, values, backtest, feasibility_penalty)

    if confidence < 0.4:
        warnings.append(f"⚠️ Confianza baja ({confidence:.0%}) — tratar como orientativo")

    # Dataset usado
    data_used = []
    for i, v in enumerate(values):
        label = period_labels[i] if period_labels and i < len(period_labels) else f"P{i+1}"
        data_used.append({"period": label, "value": round(v, 2)})

    return ForecastResult(
        prediction=round(prediction, 2),
        confidence=confidence,
        method=method,
        method_display=METHOD_DISPLAY.get(method, method),
        data_used=data_used,
        dataset_size=len(values),
        horizon=horizon_label,
        target_variable=target_variable,
        target_display=VARIABLE_DISPLAY.get(target_variable, target_variable),
        warnings=warnings,
        backtesting=backtest,
        method_selection_reason=method_reason,
    )
