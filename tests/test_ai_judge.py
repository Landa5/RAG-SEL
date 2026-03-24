"""
tests/test_ai_judge.py — Tests del AI Judge v2
Cubre: pre-check rules v2 (contextual, SQL finos), severidad dual,
       muestreo inteligente, revisión humana, JudgeVerdict v2.
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ═══════════════════════════════════════════════
# Pre-check rules (determinísticas, v2)
# ═══════════════════════════════════════════════

class TestPreCheckRules(unittest.TestCase):

    def test_sql_not_executed(self):
        from eval.ai_judge import _pre_check_rules
        issues = _pre_check_rules(
            "¿Cuántos empleados?", "50 empleados",
            {"pipeline_executed": "agentic_sql", "engines": {"sql": False}, "sql_executed": False}
        )
        self.assertTrue(any("SQL_NOT_EXECUTED" in i for i in issues))

    def test_sql_executed_ok(self):
        from eval.ai_judge import _pre_check_rules
        issues = _pre_check_rules(
            "¿Cuántos empleados?", "50 empleados",
            {"pipeline_executed": "agentic_sql", "engines": {"sql": True}, "sql_executed": True}
        )
        self.assertFalse(any("SQL_NOT_EXECUTED" in i for i in issues))

    def test_rag_no_sources(self):
        from eval.ai_judge import _pre_check_rules
        issues = _pre_check_rules(
            "¿convenio?", "El convenio dice...",
            {"pipeline_executed": "doc_retrieval", "engines": {}, "sources": []}
        )
        self.assertTrue(any("RAG_NO_SOURCES" in i for i in issues))

    def test_rag_with_sources_ok(self):
        from eval.ai_judge import _pre_check_rules
        issues = _pre_check_rules(
            "¿convenio?", "Artículo 5...",
            {"pipeline_executed": "doc_retrieval", "engines": {}, "sources": [{"doc": "c.pdf"}]}
        )
        self.assertFalse(any("RAG_NO_SOURCES" in i for i in issues))

    def test_forecast_not_executed(self):
        from eval.ai_judge import _pre_check_rules
        issues = _pre_check_rules(
            "¿gasoil?", "500 litros",
            {"pipeline_executed": "predictive_forecast", "engines": {"forecast": False}}
        )
        self.assertTrue(any("FORECAST_NOT_EXECUTED" in i for i in issues))

    def test_degradation_incomplete(self):
        from eval.ai_judge import _pre_check_rules
        issues = _pre_check_rules(
            "test", "resp",
            {"pipeline_executed": "direct_chat", "degraded_from": "sql", "degraded_to": None}
        )
        self.assertTrue(any("DEGRADATION_INCOMPLETE" in i for i in issues))

    def test_clean_response_no_issues(self):
        from eval.ai_judge import _pre_check_rules
        issues = _pre_check_rules(
            "Hola", "Hola, ¿en qué te ayudo?",
            {"pipeline_executed": "direct_chat", "engines": {}}
        )
        self.assertEqual(len(issues), 0)


# ═══════════════════════════════════════════════
# Ajuste 3: LLM_INVENTED_NUMBERS contextual
# ═══════════════════════════════════════════════

class TestInventedNumbersContextual(unittest.TestCase):

    def test_direct_chat_with_numbers_no_grounding_suspicious(self):
        """Direct chat + cifras + sin grounding = sospechoso."""
        from eval.ai_judge import _check_invented_numbers
        issues = _check_invented_numbers(
            "Hola", "Hay 1500 empleados y 3200 jornadas registradas.",
            "direct_chat", {}, []
        )
        self.assertTrue(any("LLM_INVENTED_NUMBERS" in i for i in issues))

    def test_sql_pipeline_with_numbers_ok(self):
        """SQL pipeline + cifras + grounding SQL = OK."""
        from eval.ai_judge import _check_invented_numbers
        issues = _check_invented_numbers(
            "¿cuántos empleados?", "Hay 1500 empleados y 3200 jornadas.",
            "agentic_sql", {"sql": True}, []
        )
        self.assertFalse(any("LLM_INVENTED_NUMBERS" in i for i in issues))

    def test_quantitative_question_no_grounding(self):
        """Pregunta cuantitativa sin grounding = sospechoso."""
        from eval.ai_judge import _check_invented_numbers
        issues = _check_invented_numbers(
            "¿cuántos kilómetros totales?", "Se han recorrido 45000 km.",
            "direct_chat", {}, []
        )
        self.assertTrue(any("LLM_QUANTITATIVE_NO_GROUNDING" in i for i in issues))

    def test_quantitative_with_rag_grounding_ok(self):
        """Pregunta cuantitativa con RAG grounding = OK."""
        from eval.ai_judge import _check_invented_numbers
        issues = _check_invented_numbers(
            "¿cuántos días de vacaciones?", "Según el convenio, 30 días.",
            "doc_retrieval", {"retrieval": True}, [{"doc": "convenio.pdf"}]
        )
        self.assertFalse(any("LLM_QUANTITATIVE_NO_GROUNDING" in i for i in issues))

    def test_years_excluded_from_count(self):
        """Años (2024, 2025) no se cuentan como cifras inventadas."""
        from eval.ai_judge import _check_invented_numbers
        issues = _check_invented_numbers(
            "¿Qué pasó?", "En 2024 hubo cambios y en 2025 se aplicaron.",
            "direct_chat", {}, []
        )
        self.assertFalse(any("LLM_INVENTED_NUMBERS" in i for i in issues))


# ═══════════════════════════════════════════════
# Ajuste 4: SQL checks finos
# ═══════════════════════════════════════════════

class TestSQLQualityChecks(unittest.TestCase):

    def test_sql_scope_too_broad_select_star(self):
        from eval.ai_judge import _check_sql_quality
        issues = _check_sql_quality(
            "¿empleados?", "Lista de empleados",
            {"sql_executed": True, "sql_query": 'SELECT * FROM "Empleado"', "engines": {"sql": True}}
        )
        self.assertTrue(any("SQL_SCOPE_TOO_BROAD" in i for i in issues))

    def test_sql_scope_ok_with_where(self):
        from eval.ai_judge import _check_sql_quality
        issues = _check_sql_quality(
            "¿km?", "500 km",
            {"sql_executed": True, "sql_query": 'SELECT SUM(km) FROM "Viaje" WHERE fecha > now()', "engines": {"sql": True}}
        )
        self.assertFalse(any("SQL_SCOPE_TOO_BROAD" in i for i in issues))

    def test_sql_result_misinterpreted_no_data(self):
        from eval.ai_judge import _check_sql_quality
        issues = _check_sql_quality(
            "¿cuántos?", "No hay datos disponibles",
            {"sql_executed": True, "sql_result": "[{\"count\": 42}]", "engines": {"sql": True}}
        )
        self.assertTrue(any("SQL_RESULT_MISINTERPRETED" in i for i in issues))

    def test_sql_empty_answer_detected(self):
        from eval.ai_judge import _check_sql_quality
        issues = _check_sql_quality(
            "¿cuántos?", "",
            {"sql_executed": True, "engines": {"sql": True}}
        )
        self.assertTrue(any("SQL_RESULT_MISINTERPRETED" in i for i in issues))


# ═══════════════════════════════════════════════
# Ajuste 5: Severidad dual
# ═══════════════════════════════════════════════

class TestSeverityDual(unittest.TestCase):

    def test_risk_critical_sql_not_executed(self):
        from eval.ai_judge import _compute_risk_level
        level = _compute_risk_level(["SQL_NOT_EXECUTED: test"], 0.3, "fail")
        self.assertEqual(level, "critical")

    def test_risk_high_invented_numbers(self):
        from eval.ai_judge import _compute_risk_level
        level = _compute_risk_level(["LLM_INVENTED_NUMBERS: test"], 0.3, "warning")
        self.assertEqual(level, "high")

    def test_risk_low_clean(self):
        from eval.ai_judge import _compute_risk_level
        level = _compute_risk_level([], 0.0, "pass")
        self.assertEqual(level, "low")

    def test_risk_critical_high_hallucination(self):
        from eval.ai_judge import _compute_risk_level
        level = _compute_risk_level([], 0.85, "warning")
        self.assertEqual(level, "critical")

    def test_quality_poor_low_grounding(self):
        from eval.ai_judge import _compute_quality_level
        level = _compute_quality_level(0.2, 0.5, True, [])
        self.assertEqual(level, "poor")

    def test_quality_excellent(self):
        from eval.ai_judge import _compute_quality_level
        level = _compute_quality_level(0.9, 0.9, True, [])
        self.assertEqual(level, "excellent")

    def test_quality_good(self):
        from eval.ai_judge import _compute_quality_level
        level = _compute_quality_level(0.7, 0.7, True, [])
        self.assertEqual(level, "good")

    def test_quality_poor_many_issues(self):
        from eval.ai_judge import _compute_quality_level
        level = _compute_quality_level(0.8, 0.8, True, ["a", "b", "c"])
        self.assertEqual(level, "poor")


# ═══════════════════════════════════════════════
# Ajuste 6: Muestreo inteligente
# ═══════════════════════════════════════════════

class TestSmartSampling(unittest.TestCase):

    def test_always_review_sql(self):
        from eval.ai_judge import _should_review
        self.assertTrue(_should_review({"pipeline_executed": "agentic_sql"}, "resp"))

    def test_always_review_predictive(self):
        from eval.ai_judge import _should_review
        self.assertTrue(_should_review({"pipeline_executed": "predictive_forecast"}, "resp"))

    def test_always_review_degraded(self):
        from eval.ai_judge import _should_review
        self.assertTrue(_should_review(
            {"pipeline_executed": "direct_chat", "degraded_from": "agentic_sql"}, "resp"
        ))

    def test_always_review_with_numbers(self):
        from eval.ai_judge import _should_review
        self.assertTrue(_should_review(
            {"pipeline_executed": "direct_chat"},
            "Registramos 1500 empleados y 3200 jornadas."
        ))

    def test_always_review_error(self):
        from eval.ai_judge import _should_review
        self.assertTrue(_should_review(
            {"pipeline_executed": "direct_chat", "error": "timeout"}, "resp"
        ))

    @patch("eval.ai_judge.JUDGE_TRIVIAL_SAMPLE_RATE", 0.0)
    def test_trivial_chat_skipped_at_zero(self):
        from eval.ai_judge import _should_review
        self.assertFalse(_should_review(
            {"pipeline_executed": "direct_chat"}, "Hola"
        ))


# ═══════════════════════════════════════════════
# Ajuste 2: Revisión humana
# ═══════════════════════════════════════════════

class TestHumanReviewFlags(unittest.TestCase):

    def test_fail_requires_human(self):
        from eval.ai_judge import _requires_human_review
        needs, reason = _requires_human_review("fail", "high", "agentic_sql")
        self.assertTrue(needs)
        self.assertIn("fail", reason)

    def test_critical_risk_requires_human(self):
        from eval.ai_judge import _requires_human_review
        needs, reason = _requires_human_review("warning", "critical", "direct_chat")
        self.assertTrue(needs)

    def test_pass_low_risk_no_human(self):
        from eval.ai_judge import _requires_human_review
        needs, reason = _requires_human_review("pass", "low", "direct_chat")
        self.assertFalse(needs)

    @patch("eval.ai_judge.JUDGE_WARNING_SAMPLE_RATE", 1.0)
    def test_warning_sampled_for_human(self):
        from eval.ai_judge import _requires_human_review
        needs, reason = _requires_human_review("warning", "medium", "agentic_sql")
        self.assertTrue(needs)


# ═══════════════════════════════════════════════
# JudgeVerdict v2
# ═══════════════════════════════════════════════

class TestJudgeVerdictV2(unittest.TestCase):

    def test_defaults(self):
        from eval.ai_judge import JudgeVerdict
        v = JudgeVerdict()
        self.assertEqual(v.risk_level, "low")
        self.assertEqual(v.quality_level, "acceptable")
        self.assertFalse(v.requires_human_review)
        self.assertIsNone(v.human_review_reason)


# ═══════════════════════════════════════════════
# Judge response mock (v2)
# ═══════════════════════════════════════════════

class TestJudgeResponseMockV2(unittest.TestCase):

    @patch("eval.ai_judge.JUDGE_ENABLED", True)
    @patch("eval.ai_judge._call_judge_model")
    def test_pass_produces_low_risk_good_quality(self, mock_call):
        from eval.ai_judge import judge_response
        mock_call.return_value = {
            "grounding_score": 0.9, "hallucination_risk": 0.05,
            "usefulness_score": 0.85, "pipeline_correct": True,
            "sql_consistency": True, "verdict": "pass",
            "issues": [], "review_text": "OK",
        }
        result = judge_response(
            "e1", "t1", "a1", "¿Cuántos?", "25 empleados",
            {"pipeline_executed": "agentic_sql", "engines": {"sql": True}, "sql_executed": True}
        )
        self.assertEqual(result.risk_level, "low")
        self.assertIn(result.quality_level, ("good", "excellent"))
        self.assertFalse(result.requires_human_review)

    @patch("eval.ai_judge.JUDGE_ENABLED", True)
    @patch("eval.ai_judge._call_judge_model")
    def test_fail_verdict_triggers_human_review(self, mock_call):
        from eval.ai_judge import judge_response
        mock_call.return_value = {
            "grounding_score": 0.2, "hallucination_risk": 0.8,
            "usefulness_score": 0.3, "pipeline_correct": False,
            "verdict": "fail", "issues": ["HALLUCINATION: datos inventados"],
            "review_text": "Respuesta inventada",
        }
        result = judge_response(
            "e2", "t1", "a1", "¿km?", "Se recorrieron 99999 km",
            {"pipeline_executed": "agentic_sql", "engines": {"sql": False}, "sql_executed": False}
        )
        self.assertTrue(result.requires_human_review)
        self.assertEqual(result.risk_level, "critical")

    @patch("eval.ai_judge.JUDGE_ENABLED", False)
    def test_disabled(self):
        from eval.ai_judge import judge_response
        self.assertIsNone(judge_response("e1", "t1", "a1", "q", "a", {}))


if __name__ == "__main__":
    unittest.main()
