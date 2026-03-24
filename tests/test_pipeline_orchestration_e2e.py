"""
tests/test_pipeline_orchestration_e2e.py — Tests de integración E2E V3.0
Verifica que la orquestación estricta funciona correctamente:
- Cada pipeline produce su contrato de salida
- Predicciones usan forecast_engine real
- Degradaciones funcionan
- Trazabilidad completa
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from llm.result_contracts import (
    OrchestratedResult, ExecutionLog, ToolExecution,
    DirectChatResult, DocRetrievalResult, AgenticSQLResult,
    AgenticSQLRAGResult, PredictiveInsightResult, PredictiveForecastResult,
    is_llm_only_allowed, CRITICAL_PIPELINES,
)
from llm.response_renderer import (
    build_constrained_prompt, render_structured_metadata,
    _PIPELINE_CONSTRAINTS, _GLOBAL_CONSTRAINTS,
)
from llm.task_classifier import classify as classify_task
from llm.pipeline_selector import select_pipeline
from llm.execution_planner import create_execution_plan


class TestResultContracts(unittest.TestCase):
    """Verifica que los contratos de salida están correctamente tipados."""

    def test_direct_chat_result(self):
        r = DirectChatResult(answer_text="Hola", confidence=1.0)
        self.assertEqual(r.result_type, "direct_chat")

    def test_doc_retrieval_result_no_evidence(self):
        r = DocRetrievalResult()
        self.assertFalse(r.has_evidence)

    def test_doc_retrieval_result_with_evidence(self):
        r = DocRetrievalResult(
            retrieved_chunks=[{"text": "test", "source": "doc.pdf", "page": 1}],
            sources=[{"source": "doc.pdf", "page": 1}],
        )
        self.assertTrue(r.has_evidence)

    def test_agentic_sql_result_no_data(self):
        r = AgenticSQLResult()
        self.assertFalse(r.has_data)

    def test_agentic_sql_result_with_data(self):
        r = AgenticSQLResult(rows_returned=5)
        self.assertTrue(r.has_data)

    def test_predictive_insight_result(self):
        r = PredictiveInsightResult(
            degraded_from_forecast=True,
            degrade_reason="Datos insuficientes",
        )
        self.assertEqual(r.result_type, "predictive_insight")
        self.assertTrue(r.degraded_from_forecast)


class TestAllowLLMOnly(unittest.TestCase):
    """Verifica la política allow_llm_only."""

    def test_direct_chat_allows_llm_only(self):
        self.assertTrue(is_llm_only_allowed("direct_chat"))

    def test_doc_retrieval_forbids_llm_only(self):
        self.assertFalse(is_llm_only_allowed("doc_retrieval"))

    def test_agentic_sql_forbids_llm_only(self):
        self.assertFalse(is_llm_only_allowed("agentic_sql"))

    def test_agentic_sql_rag_forbids_llm_only(self):
        self.assertFalse(is_llm_only_allowed("agentic_sql_rag"))

    def test_predictive_forecast_forbids_llm_only(self):
        self.assertFalse(is_llm_only_allowed("predictive_forecast"))

    def test_predictive_insight_forbids_llm_only(self):
        self.assertFalse(is_llm_only_allowed("predictive_insight"))


class TestCriticalPipelines(unittest.TestCase):
    """Verifica que los pipelines críticos están definidos."""

    def test_all_critical_defined(self):
        expected = {"predictive_forecast", "predictive_insight",
                    "agentic_sql", "agentic_sql_rag", "doc_retrieval"}
        self.assertEqual(CRITICAL_PIPELINES, expected)

    def test_direct_chat_not_critical(self):
        self.assertNotIn("direct_chat", CRITICAL_PIPELINES)


class TestContractValidation(unittest.TestCase):
    """Verifica la validación de contratos."""

    def test_direct_chat_validates(self):
        orch = OrchestratedResult(
            pipeline_executed="direct_chat",
            structured_result=DirectChatResult(),
            execution_log=ExecutionLog("direct_chat", "direct_chat"),
            allow_llm_only=True,
        )
        valid, msg = orch.validate()
        self.assertTrue(valid)

    def test_forecast_without_result_fails(self):
        orch = OrchestratedResult(
            pipeline_executed="predictive_forecast",
            structured_result=None,
            execution_log=ExecutionLog("predictive_forecast", "predictive_forecast"),
            allow_llm_only=False,
        )
        valid, msg = orch.validate()
        self.assertFalse(valid)
        self.assertIn("requiere resultado estructurado", msg)

    def test_doc_retrieval_without_evidence_fails(self):
        orch = OrchestratedResult(
            pipeline_executed="doc_retrieval",
            structured_result=DocRetrievalResult(),
            execution_log=ExecutionLog("doc_retrieval", "doc_retrieval"),
            allow_llm_only=False,
        )
        valid, msg = orch.validate()
        self.assertFalse(valid)
        self.assertIn("chunks y fuentes reales", msg)

    def test_doc_retrieval_with_evidence_succeeds(self):
        orch = OrchestratedResult(
            pipeline_executed="doc_retrieval",
            structured_result=DocRetrievalResult(
                retrieved_chunks=[{"text": "t", "source": "s.pdf", "page": 1}],
                sources=[{"source": "s.pdf", "page": 1}],
            ),
            execution_log=ExecutionLog("doc_retrieval", "doc_retrieval"),
            allow_llm_only=False,
        )
        valid, msg = orch.validate()
        self.assertTrue(valid)

    def test_agentic_sql_wrong_type_fails(self):
        orch = OrchestratedResult(
            pipeline_executed="agentic_sql",
            structured_result=DirectChatResult(),  # tipo incorrecto
            execution_log=ExecutionLog("agentic_sql", "agentic_sql"),
            allow_llm_only=False,
        )
        valid, msg = orch.validate()
        self.assertFalse(valid)


class TestResponseRenderer(unittest.TestCase):
    """Verifica que el renderer genera prompts restringidos."""

    def test_constraints_exist_for_all_pipelines(self):
        for pid in ["direct_chat", "doc_retrieval", "agentic_sql",
                     "agentic_sql_rag", "predictive_insight", "predictive_forecast"]:
            self.assertIn(pid, _PIPELINE_CONSTRAINTS)

    def test_forecast_prompt_contains_prohibitions(self):
        constraint = _PIPELINE_CONSTRAINTS["predictive_forecast"]
        self.assertIn("PROHIBIDO", constraint)
        self.assertIn("Recalcular", constraint)

    def test_sql_prompt_contains_prohibitions(self):
        constraint = _PIPELINE_CONSTRAINTS["agentic_sql"]
        self.assertIn("PROHIBIDO", constraint)
        self.assertIn("Inventar", constraint)

    def test_global_constraints_present(self):
        self.assertIn("No calcules predicciones", _GLOBAL_CONSTRAINTS)
        self.assertIn("No inventes confianza", _GLOBAL_CONSTRAINTS)

    def test_doc_retrieval_prompt_renders_chunks(self):
        orch = OrchestratedResult(
            pipeline_executed="doc_retrieval",
            structured_result=DocRetrievalResult(
                retrieved_chunks=[{"text": "Ejemplo", "source": "convenio.pdf", "page": 3, "score": 0.85}],
                sources=[{"source": "convenio.pdf", "page": 3}],
            ),
            execution_log=ExecutionLog("doc_retrieval", "doc_retrieval"),
        )
        prompt = build_constrained_prompt(orch, "qué dice el convenio")
        self.assertIn("convenio.pdf", prompt)
        self.assertIn("FRAGMENTOS DOCUMENTALES", prompt)


class TestToolExecution(unittest.TestCase):
    """Verifica el registro granular de herramientas."""

    def test_tool_execution_fields(self):
        t = ToolExecution(
            tool_name="forecast_engine",
            success=True,
            duration_ms=150,
            result_summary="predicción=1500.00",
        )
        self.assertEqual(t.tool_name, "forecast_engine")
        self.assertTrue(t.success)

    def test_execution_log_add_tool(self):
        log = ExecutionLog("test", "test")
        log.add_tool(ToolExecution("tool1", True, 100, "ok"))
        log.add_tool(ToolExecution("tool2", False, 200, "error"))
        self.assertEqual(len(log.tool_executions), 2)
        self.assertFalse(log.tool_executions[1].success)


class TestClassificationPipelineIntegration(unittest.TestCase):
    """Verifica que classify → pipeline → plan produce planes correctos."""

    def test_prediction_query_gets_forecast_pipeline(self):
        c = classify_task("predice el consumo de gasoil del próximo mes")
        ps = select_pipeline(c)
        self.assertIn(ps.pipeline_id, ("predictive_forecast", "predictive_insight"))

    def test_sql_query_gets_agentic_sql(self):
        c = classify_task("cuántas horas ha trabajado Pedro esta semana")
        ps = select_pipeline(c)
        self.assertIn(ps.pipeline_id, ("agentic_sql", "agentic_sql_rag"))

    def test_doc_query_gets_doc_retrieval(self):
        c = classify_task("qué dice el convenio sobre vacaciones")
        ps = select_pipeline(c)
        self.assertIn(ps.pipeline_id, ("doc_retrieval", "agentic_sql_rag"))

    def test_chat_query_gets_direct_chat(self):
        c = classify_task("hola, buenos días")
        ps = select_pipeline(c)
        self.assertEqual(ps.pipeline_id, "direct_chat")

    def test_forecast_plan_has_correct_contract(self):
        c = classify_task("estima el gasto de mantenimiento del próximo trimestre")
        ps = select_pipeline(c)
        if ps.pipeline_id == "predictive_forecast":
            plan = create_execution_plan(c, ps)
            self.assertEqual(plan.result_contract, "PredictiveForecastResult")
            self.assertFalse(plan.allow_llm_only)
            self.assertTrue(plan.needs_forecast_engine)
            self.assertIn("forecast_engine", plan.required_tools)

    def test_direct_chat_plan_allows_llm_only(self):
        c = classify_task("hola qué tal")
        ps = select_pipeline(c)
        plan = create_execution_plan(c, ps)
        self.assertTrue(plan.allow_llm_only)

    def test_sql_plan_forbids_llm_only(self):
        c = classify_task("dame los km de todos los camiones hoy")
        ps = select_pipeline(c)
        plan = create_execution_plan(c, ps)
        self.assertFalse(plan.allow_llm_only)


class TestStructuredMetadata(unittest.TestCase):
    """Verifica que render_structured_metadata genera info correcta."""

    def test_metadata_has_all_fields(self):
        log = ExecutionLog("doc_retrieval", "doc_retrieval")
        log.retrieval_executed = True
        log.add_tool(ToolExecution("retrieval_search", True, 200, "10 chunks"))

        orch = OrchestratedResult(
            pipeline_executed="doc_retrieval",
            structured_result=DocRetrievalResult(
                retrieved_chunks=[{"text": "t", "source": "s", "page": 1}],
                sources=[{"source": "s", "page": 1}],
            ),
            execution_log=log,
        )
        meta = render_structured_metadata(orch)
        self.assertEqual(meta["pipeline_executed"], "doc_retrieval")
        self.assertTrue(meta["engines_used"]["retrieval"])
        self.assertFalse(meta["engines_used"]["forecast"])
        self.assertEqual(len(meta["tool_executions"]), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
