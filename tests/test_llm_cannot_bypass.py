"""
tests/test_llm_cannot_bypass.py — Tests negativos de evasión V3.0
Verifica que el LLM NO puede evadir los controles del sistema.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from llm.result_contracts import (
    OrchestratedResult, ExecutionLog,
    DirectChatResult, DocRetrievalResult, AgenticSQLResult,
    PredictiveInsightResult, PredictiveForecastResult,
    is_llm_only_allowed,
)
from llm.response_renderer import (
    build_constrained_prompt, _PIPELINE_CONSTRAINTS,
)


class TestLLMCannotBypass(unittest.TestCase):
    """
    Tests negativos: verifican que las reglas anti-evasión funcionan.
    """

    # ─── Predicción ───

    def test_forecast_without_result_rejected(self):
        """El LLM NO puede emitir forecast sin PredictiveForecastResult."""
        orch = OrchestratedResult(
            pipeline_executed="predictive_forecast",
            structured_result=None,
            execution_log=ExecutionLog("predictive_forecast", "predictive_forecast"),
            allow_llm_only=False,
        )
        valid, msg = orch.validate()
        self.assertFalse(valid)
        self.assertIn("requiere resultado estructurado", msg)

    def test_forecast_with_wrong_type_rejected(self):
        """El LLM NO puede emitir forecast con tipo incorrecto."""
        orch = OrchestratedResult(
            pipeline_executed="predictive_forecast",
            structured_result=DirectChatResult(),  # tipo equivocado
            execution_log=ExecutionLog("predictive_forecast", "predictive_forecast"),
            allow_llm_only=False,
        )
        valid, msg = orch.validate()
        self.assertFalse(valid)
        self.assertIn("PredictiveForecastResult", msg)

    def test_forecast_prompt_prohibits_recalculation(self):
        """El prompt del LLM prohíbe recalcular predicciones."""
        constraint = _PIPELINE_CONSTRAINTS["predictive_forecast"]
        self.assertIn("PROHIBIDO", constraint)
        self.assertIn("Recalcular", constraint)
        self.assertIn("ajustar", constraint)
        self.assertIn("Modificar el porcentaje de confianza", constraint)

    def test_insight_prompt_prohibits_quantitative(self):
        """El prompt de insight prohíbe predicciones cuantitativas."""
        constraint = _PIPELINE_CONSTRAINTS["predictive_insight"]
        self.assertIn("PROHIBIDO", constraint)
        self.assertIn("predicción cuantitativa", constraint)

    # ─── SQL ───

    def test_sql_without_result_rejected(self):
        """El LLM NO puede responder con cifras sin AgenticSQLResult."""
        orch = OrchestratedResult(
            pipeline_executed="agentic_sql",
            structured_result=None,
            execution_log=ExecutionLog("agentic_sql", "agentic_sql"),
            allow_llm_only=False,
        )
        valid, msg = orch.validate()
        self.assertFalse(valid)

    def test_sql_with_wrong_type_rejected(self):
        """El LLM NO puede responder SQL con tipo incorrecto."""
        orch = OrchestratedResult(
            pipeline_executed="agentic_sql",
            structured_result=DocRetrievalResult(),  # tipo equivocado
            execution_log=ExecutionLog("agentic_sql", "agentic_sql"),
            allow_llm_only=False,
        )
        valid, msg = orch.validate()
        self.assertFalse(valid)
        self.assertIn("AgenticSQLResult", msg)

    def test_sql_prompt_prohibits_invention(self):
        """El prompt SQL prohíbe inventar cifras."""
        constraint = _PIPELINE_CONSTRAINTS["agentic_sql"]
        self.assertIn("PROHIBIDO", constraint)
        self.assertIn("Inventar cifras", constraint)

    # ─── Documentos ───

    def test_doc_retrieval_without_evidence_rejected(self):
        """El LLM NO puede responder documentalmente sin fuentes."""
        orch = OrchestratedResult(
            pipeline_executed="doc_retrieval",
            structured_result=DocRetrievalResult(),  # sin chunks ni fuentes
            execution_log=ExecutionLog("doc_retrieval", "doc_retrieval"),
            allow_llm_only=False,
        )
        valid, msg = orch.validate()
        self.assertFalse(valid)
        self.assertIn("chunks y fuentes reales", msg)

    def test_doc_prompt_prohibits_invention(self):
        """El prompt documental prohíbe inventar información."""
        constraint = _PIPELINE_CONSTRAINTS["doc_retrieval"]
        self.assertIn("PROHIBIDO", constraint)
        self.assertIn("Inventar información", constraint)

    # ─── Policy de allow_llm_only ───

    def test_only_direct_chat_allows_llm_only(self):
        """Solo direct_chat permite respuestas solo-LLM."""
        all_pipelines = ["direct_chat", "doc_retrieval", "agentic_sql",
                         "agentic_sql_rag", "predictive_insight", "predictive_forecast"]
        for pid in all_pipelines:
            allowed = is_llm_only_allowed(pid)
            if pid == "direct_chat":
                self.assertTrue(allowed, f"{pid} debe permitir LLM-only")
            else:
                self.assertFalse(allowed, f"{pid} NO debe permitir LLM-only")

    # ─── Prompt global ───

    def test_global_constraints_in_all_prompts(self):
        """Las restricciones globales se incluyen en todos los prompts."""
        for pid in ["doc_retrieval", "agentic_sql", "predictive_forecast"]:
            log = ExecutionLog(pid, pid)
            orch = OrchestratedResult(
                pipeline_executed=pid,
                structured_result=DirectChatResult(),
                execution_log=log,
            )
            prompt = build_constrained_prompt(orch, "test query")
            self.assertIn("No calcules predicciones", prompt)
            self.assertIn("No inventes confianza", prompt)

    # ─── Trazabilidad ───

    def test_forecast_execution_tracked(self):
        """La ejecución del forecast engine queda registrada."""
        log = ExecutionLog("predictive_forecast", "predictive_forecast")
        log.forecast_engine_executed = True
        self.assertTrue(log.forecast_engine_executed)

    def test_degradation_tracked(self):
        """Las degradaciones quedan registradas."""
        log = ExecutionLog("predictive_forecast", "predictive_insight")
        log.degraded_from = "predictive_forecast"
        log.degraded_to = "predictive_insight"
        self.assertEqual(log.degraded_from, "predictive_forecast")
        self.assertEqual(log.degraded_to, "predictive_insight")


if __name__ == "__main__":
    unittest.main(verbosity=2)
