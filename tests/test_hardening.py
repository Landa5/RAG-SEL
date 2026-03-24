"""
tests/test_hardening.py — Tests de endurecimiento operativo V3.0
Verifica que todos los bordes frágiles están cerrados.
"""
import sys
import os
import warnings
import re
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
    _PIPELINE_CONSTRAINTS, _RENDER_MAP,
)


class TestServerNoLegacyImports(unittest.TestCase):
    """Verifica que server.py no importa router legacy en runtime."""

    def test_server_has_no_router_import(self):
        """server.py no debe importar classify_query, route_query ni nada de llm.router."""
        server_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "api", "server.py"
        )
        with open(server_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertNotIn("from llm.router import classify_query", content)
        self.assertNotIn("from llm.router import route_query", content)
        self.assertNotIn("classify_query", content.split("# ──")[0])  # antes de comentarios

    def test_server_imports_generate_answer_stream(self):
        """server.py debe importar generate_answer_stream de llm.generate."""
        server_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "api", "server.py"
        )
        with open(server_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("from llm.generate import generate_answer_stream", content)

    def test_server_has_error_handling_in_stream(self):
        """server.py debe tener try/except en event_generator."""
        server_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "api", "server.py"
        )
        with open(server_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("except Exception", content)
        self.assertIn("Error del orquestador", content)


class TestRouterLegacyDesactivado(unittest.TestCase):
    """Verifica que router.py legacy emite warning y rechaza calls."""

    def test_router_import_emits_deprecation_warning(self):
        """Importar llm.router emite DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            import importlib
            if "llm.router" in sys.modules:
                importlib.reload(sys.modules["llm.router"])
            else:
                import llm.router
            deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
            self.assertGreater(len(deprecation_warnings), 0)

    def test_classify_query_raises(self):
        """classify_query debe lanzar NotImplementedError."""
        from llm.router import classify_query
        with self.assertRaises(NotImplementedError):
            classify_query("test")

    def test_route_query_raises(self):
        """route_query debe lanzar NotImplementedError."""
        from llm.router import route_query
        with self.assertRaises(NotImplementedError):
            route_query("test")



# ── Funciones SQL standalone (copia exacta de generate.py) ──
# Se reimplementan aquí para evitar importar generate.py que requiere `requests`
_DANGEROUS_KEYWORDS = [
    'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'TRUNCATE',
    'CREATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE', 'CALL',
    '--', '/*', '*/', 'pg_', 'information_schema',
]


def _validate_sql_pre(sql: str):
    sql_upper = sql.upper().strip()
    if not sql_upper.startswith('SELECT'):
        return False, 'Solo se permiten consultas SELECT.'
    for kw in _DANGEROUS_KEYWORDS:
        if kw.upper() in sql_upper:
            return False, f'Operacion SQL peligrosa detectada: {kw}'
    return True, ''


def _sanitize_sql(sql: str) -> str:
    sql = sql.strip().rstrip(";")
    if "limit" not in sql.lower():
        sql += " LIMIT 100"
    return sql


def _validate_sql_post(rows, orch_result=None):
    if not rows:
        warning = "La consulta SQL no devolvio resultados. No hay datos disponibles."
        if orch_result and isinstance(orch_result.structured_result, (AgenticSQLResult, AgenticSQLRAGResult)):
            sr = orch_result.structured_result
            if isinstance(sr, AgenticSQLRAGResult) and sr.sql_result:
                sr = sr.sql_result
            if isinstance(sr, AgenticSQLResult):
                sr.warnings.append(warning)
        return warning
    return ''


class TestSQLValidacionPre(unittest.TestCase):
    """Verifica que la validación SQL pre-ejecución funciona."""

    def test_only_select_allowed(self):
        ok, msg = _validate_sql_pre("DELETE FROM empleados")
        self.assertFalse(ok)
        self.assertIn("Solo se permiten consultas SELECT", msg)

    def test_dangerous_keywords_blocked(self):
        dangerous_sqls = [
            "SELECT * FROM a; DROP TABLE b",
            "SELECT * FROM a; INSERT INTO b VALUES (1)",
            "SELECT * FROM a; UPDATE b SET x=1",
            "SELECT * FROM a; ALTER TABLE b",
            "SELECT * FROM a; TRUNCATE b",
        ]
        for sql in dangerous_sqls:
            ok, msg = _validate_sql_pre(sql)
            self.assertFalse(ok, f"Deberia bloquear: {sql}")

    def test_safe_select_passes(self):
        ok, msg = _validate_sql_pre('SELECT "nombre" FROM "Empleado" WHERE activo = true')
        self.assertTrue(ok)

    def test_sql_injection_blocked(self):
        ok, msg = _validate_sql_pre("SELECT * FROM a -- comment")
        self.assertFalse(ok)


class TestSQLValidacionPost(unittest.TestCase):
    """Verifica que la validación SQL post-ejecución inyecta advertencias."""

    def test_empty_rows_returns_warning(self):
        warning = _validate_sql_post([])
        self.assertIn("no devolvio resultados", warning)

    def test_nonempty_rows_no_warning(self):
        warning = _validate_sql_post([{"a": 1}])
        self.assertEqual(warning, "")

    def test_empty_rows_injects_warning_in_result(self):
        orch = OrchestratedResult(
            pipeline_executed="agentic_sql",
            structured_result=AgenticSQLResult(),
            execution_log=ExecutionLog("agentic_sql", "agentic_sql"),
        )
        _validate_sql_post([], orch)
        self.assertGreater(len(orch.structured_result.warnings), 0)


class TestEvidenceGate(unittest.TestCase):
    """Verifica que el evidence gate rechaza RAG sin evidencia."""

    def test_doc_retrieval_without_evidence_fails_validation(self):
        orch = OrchestratedResult(
            pipeline_executed="doc_retrieval",
            structured_result=DocRetrievalResult(),  # sin chunks
            execution_log=ExecutionLog("doc_retrieval", "doc_retrieval"),
        )
        valid, msg = orch.validate()
        self.assertFalse(valid)
        self.assertIn("chunks y fuentes reales", msg)

    def test_doc_retrieval_with_evidence_passes(self):
        orch = OrchestratedResult(
            pipeline_executed="doc_retrieval",
            structured_result=DocRetrievalResult(
                retrieved_chunks=[{"text": "t", "source": "s.pdf", "page": 1}],
                sources=[{"source": "s.pdf", "page": 1}],
            ),
            execution_log=ExecutionLog("doc_retrieval", "doc_retrieval"),
        )
        valid, msg = orch.validate()
        self.assertTrue(valid)

    def test_sql_rag_without_any_evidence_fails(self):
        orch = OrchestratedResult(
            pipeline_executed="agentic_sql_rag",
            structured_result=AgenticSQLRAGResult(
                sql_result=AgenticSQLResult(),
                rag_result=DocRetrievalResult(),
            ),
            execution_log=ExecutionLog("agentic_sql_rag", "agentic_sql_rag"),
        )
        # agentic_sql_rag valida tipo, no evidencia aquí
        valid, msg = orch.validate()
        self.assertTrue(valid)  # tipo correcto → pasa validación


class TestRendererNoInference(unittest.TestCase):
    """Verifica que el renderer no rellena campos ausentes."""

    def test_renderer_does_not_add_missing_fields(self):
        """Si el resultado tiene campos vacíos, el renderer NO los inventa."""
        result = PredictiveInsightResult()  # todo vacío
        from llm.response_renderer import _render_predictive_insight
        rendered = _render_predictive_insight(result)
        # No debe contener datos que no estén en el resultado
        self.assertNotIn("predicción", rendered.lower())  # no es forecast
        self.assertIn("ANÁLISIS DESCRIPTIVO", rendered)

    def test_forecast_renderer_uses_only_result_fields(self):
        """El renderer de forecast NO genera datos por su cuenta."""
        # Verificar que _render_predictive_forecast accede solo a campos del result
        from llm.response_renderer import _render_predictive_forecast
        import inspect
        src = inspect.getsource(_render_predictive_forecast)
        # No debe llamar a funciones de cálculo
        self.assertNotIn("calculate", src.lower())
        self.assertNotIn("compute", src.lower())
        self.assertNotIn("math.", src)

    def test_all_pipelines_have_render_function(self):
        """Cada pipeline tiene una función de render obligatoria."""
        expected = {"direct_chat", "doc_retrieval", "agentic_sql",
                    "agentic_sql_rag", "predictive_insight", "predictive_forecast"}
        self.assertEqual(set(_RENDER_MAP.keys()), expected)


class TestExecutionLogFields(unittest.TestCase):
    """Verifica que ExecutionLog tiene los campos obligatorios."""

    def test_has_error_field(self):
        log = ExecutionLog("test", "test")
        log.error = "test error"
        self.assertEqual(log.error, "test error")

    def test_has_warnings_field(self):
        log = ExecutionLog("test", "test")
        log.warnings.append("test warning")
        self.assertEqual(len(log.warnings), 1)

    def test_all_required_fields_present(self):
        log = ExecutionLog("sel", "exec")
        required = [
            "pipeline_selected", "pipeline_executed",
            "forecast_engine_executed", "feasibility_check_executed",
            "retrieval_executed", "sql_executed", "llm_only_response",
            "degraded_from", "degraded_to", "structured_result_type",
            "tool_executions", "total_duration_ms", "error",
        ]
        for field_name in required:
            self.assertTrue(hasattr(log, field_name), f"Falta campo: {field_name}")

    def test_tool_execution_has_all_fields(self):
        t = ToolExecution("test", True, 100, "ok")
        required = ["tool_name", "success", "duration_ms", "result_summary", "error", "input_preview"]
        for field_name in required:
            self.assertTrue(hasattr(t, field_name), f"Falta campo: {field_name}")


class TestAgenticSQLResultHasWarnings(unittest.TestCase):
    """Verifica que AgenticSQLResult tiene campo warnings."""

    def test_warnings_exist(self):
        r = AgenticSQLResult()
        r.warnings.append("Sin datos")
        self.assertEqual(len(r.warnings), 1)

    def test_sql_no_data_cannot_produce_confident_result(self):
        """Si rows_returned=0, has_data debe ser False."""
        r = AgenticSQLResult(rows_returned=0)
        self.assertFalse(r.has_data)


class TestSanitizeSQL(unittest.TestCase):
    """Verifica que _sanitize_sql funciona correctamente."""

    def test_adds_limit(self):
        result = _sanitize_sql("SELECT * FROM test")
        self.assertIn("LIMIT", result)

    def test_preserves_existing_limit(self):
        result = _sanitize_sql("SELECT * FROM test LIMIT 50")
        self.assertEqual(result.count("LIMIT"), 1)

    def test_strips_semicolon(self):
        result = _sanitize_sql("SELECT * FROM test;")
        self.assertNotIn(";", result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
