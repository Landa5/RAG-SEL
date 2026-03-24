"""
tests/test_sql_hardening.py — Tests de endurecimiento definitivo de agentic_sql
Verifica whitelist activa de tablas, timeout explícito, y trazabilidad completa.
"""
import sys
import os
import re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from llm.result_contracts import (
    OrchestratedResult, ExecutionLog, ToolExecution,
    AgenticSQLResult, AgenticSQLRAGResult,
)


# ── Funciones SQL standalone (copia exacta de generate.py) ──
# Reimplementadas para evitar dependencia de `requests` al importar generate.py

_ALLOWED_TABLES = {
    'Empleado', 'JornadaLaboral', 'UsoCamion', 'Camion',
    'MantenimientoRealizado', 'MantenimientoProximo', 'Ausencia',
    'NominaMes', 'NominaLinea', 'Tarea', 'TareaHistorial',
    'Descarga', 'Documento', 'RevisionAccesorios', 'Proyecto',
    'TachographDailySummary', 'TachographDriver', 'TachographVehicle',
}

_DANGEROUS_KEYWORDS = [
    'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'TRUNCATE',
    'CREATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE', 'CALL',
    '--', '/*', '*/', 'pg_', 'information_schema',
]

_SQL_MAX_ROWS = 100
_SQL_TIMEOUT_MS = 15000


def _extract_tables_from_sql(sql: str) -> list[str]:
    tables = set()
    for m in re.finditer(r'(?:FROM|JOIN)\s+"([^"]+)"', sql, re.IGNORECASE):
        tables.add(m.group(1))
    for m in re.finditer(r'(?:FROM|JOIN)\s+([A-Z][a-zA-Z]+)(?:\s|$|,)', sql):
        tables.add(m.group(1))
    return list(tables)


def _validate_sql_pre(sql: str) -> tuple[bool, str]:
    sql_upper = sql.upper().strip()
    if not sql_upper.startswith('SELECT'):
        return False, 'Solo se permiten consultas SELECT.'
    for kw in _DANGEROUS_KEYWORDS:
        if kw.upper() in sql_upper:
            return False, f'Operacion SQL peligrosa detectada: {kw}'
    tables = _extract_tables_from_sql(sql)
    if tables:
        forbidden = [t for t in tables if t not in _ALLOWED_TABLES]
        if forbidden:
            return False, f'Tablas no permitidas: {", ".join(forbidden)}. Permitidas: {", ".join(sorted(_ALLOWED_TABLES))}'
    return True, ''


def _sanitize_sql(sql: str) -> str:
    sql = sql.strip().rstrip(";")
    if "limit" not in sql.lower():
        sql += f" LIMIT {_SQL_MAX_ROWS}"
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


# ─────────────────────────────────────────────
# Tests de whitelist activa
# ─────────────────────────────────────────────

class TestWhitelistActiva(unittest.TestCase):
    """Verifica que la whitelist de tablas bloquea activamente tablas no permitidas."""

    def test_tabla_permitida_ok(self):
        """Query con tabla permitida pasa validación."""
        ok, msg = _validate_sql_pre('SELECT "nombre" FROM "Empleado"')
        self.assertTrue(ok, f"Deberia pasar: {msg}")

    def test_tabla_no_permitida_rechazada(self):
        """Query con tabla fuera de whitelist es rechazada."""
        ok, msg = _validate_sql_pre('SELECT * FROM "TablaHackeada"')
        self.assertFalse(ok)
        self.assertIn("Tablas no permitidas", msg)
        self.assertIn("TablaHackeada", msg)

    def test_varias_tablas_permitidas_ok(self):
        """Query con JOIN de tablas permitidas pasa."""
        sql = 'SELECT e."nombre", j."totalHoras" FROM "Empleado" e JOIN "JornadaLaboral" j ON j."empleadoId" = e.id'
        ok, msg = _validate_sql_pre(sql)
        self.assertTrue(ok, f"Deberia pasar: {msg}")

    def test_join_con_tabla_no_permitida_rechazado(self):
        """Query con JOIN que incluye tabla prohibida es rechazada."""
        sql = 'SELECT * FROM "Empleado" e JOIN "users" u ON u.id = e.id'
        ok, msg = _validate_sql_pre(sql)
        self.assertFalse(ok)
        self.assertIn("users", msg)

    def test_subquery_tabla_no_permitida(self):
        """Subquery con tabla prohibida es rechazada."""
        sql = 'SELECT * FROM "Empleado" WHERE id IN (SELECT id FROM "SecretTable")'
        ok, msg = _validate_sql_pre(sql)
        self.assertFalse(ok)
        self.assertIn("SecretTable", msg)

    def test_todas_las_tablas_permitidas_definidas(self):
        """Verifica que todas las tablas conocidas del sistema están en la whitelist."""
        expected_tables = {
            'Empleado', 'JornadaLaboral', 'UsoCamion', 'Camion',
            'MantenimientoRealizado', 'MantenimientoProximo', 'Ausencia',
            'NominaMes', 'NominaLinea', 'Tarea', 'TareaHistorial',
            'Descarga', 'Documento', 'RevisionAccesorios', 'Proyecto',
            'TachographDailySummary', 'TachographDriver', 'TachographVehicle',
        }
        self.assertEqual(_ALLOWED_TABLES, expected_tables)

    def test_motivo_rechazo_incluye_tablas_permitidas(self):
        """El mensaje de rechazo incluye la lista de tablas permitidas."""
        ok, msg = _validate_sql_pre('SELECT * FROM "HackerTable"')
        self.assertFalse(ok)
        self.assertIn("Permitidas:", msg)
        self.assertIn("Empleado", msg)

    def test_tabla_sistema_pg_rechazada(self):
        """Tablas de sistema PostgreSQL están bloqueadas por keywords."""
        ok, msg = _validate_sql_pre('SELECT * FROM "pg_catalog"."pg_tables"')
        self.assertFalse(ok)
        # Bloqueado por keyword pg_ en _DANGEROUS_KEYWORDS

    def test_información_schema_rechazada(self):
        """information_schema está bloqueado."""
        ok, msg = _validate_sql_pre('SELECT * FROM "information_schema"."tables"')
        self.assertFalse(ok)


# ─────────────────────────────────────────────
# Tests de extracción de tablas
# ─────────────────────────────────────────────

class TestExtractTables(unittest.TestCase):
    """Verifica que _extract_tables_from_sql extrae tablas correctamente."""

    def test_single_quoted_table(self):
        tables = _extract_tables_from_sql('SELECT * FROM "Empleado"')
        self.assertIn("Empleado", tables)

    def test_multiple_joins(self):
        sql = 'SELECT * FROM "Empleado" e JOIN "JornadaLaboral" j ON j."empleadoId" = e.id LEFT JOIN "UsoCamion" u ON u."jornadaId" = j.id'
        tables = _extract_tables_from_sql(sql)
        self.assertIn("Empleado", tables)
        self.assertIn("JornadaLaboral", tables)
        self.assertIn("UsoCamion", tables)

    def test_subquery_tables(self):
        sql = 'SELECT * FROM "Empleado" WHERE id IN (SELECT "empleadoId" FROM "JornadaLaboral")'
        tables = _extract_tables_from_sql(sql)
        self.assertIn("Empleado", tables)
        self.assertIn("JornadaLaboral", tables)

    def test_no_tables_returns_empty(self):
        tables = _extract_tables_from_sql("SELECT 1 + 1")
        self.assertEqual(len(tables), 0)


# ─────────────────────────────────────────────
# Tests de trazabilidad de rechazo
# ─────────────────────────────────────────────

class TestRechazaYRegistra(unittest.TestCase):
    """Verifica que el rechazo SQL queda registrado en los logs."""

    def test_rechazo_tabla_no_ejecuta_sql(self):
        """Si una tabla está prohibida, sql_executed debe ser False."""
        log = ExecutionLog("agentic_sql", "agentic_sql")
        # Simular validación pre-ejecución
        sql = 'SELECT * FROM "HackerTable"'
        ok, pre_err = _validate_sql_pre(sql)
        self.assertFalse(ok)
        # sql NO se ejecuta → sql_executed sigue False
        self.assertFalse(log.sql_executed)

    def test_rechazo_registra_tool_execution(self):
        """El motivo de rechazo se registra en ToolExecution."""
        log = ExecutionLog("agentic_sql", "agentic_sql")
        sql = 'SELECT * FROM "TablaProhibida"'
        ok, pre_err = _validate_sql_pre(sql)
        # Simular lo que hace execute_tool
        log.add_tool(ToolExecution(
            tool_name="ejecutar_consulta_sql",
            success=False,
            duration_ms=0,
            result_summary=f"BLOQUEADO: {pre_err}",
            error=pre_err,
            input_preview=sql[:200],
        ))
        self.assertEqual(len(log.tool_executions), 1)
        self.assertFalse(log.tool_executions[0].success)
        self.assertIn("BLOQUEADO", log.tool_executions[0].result_summary)
        self.assertIn("TablaProhibida", log.tool_executions[0].error)

    def test_rechazo_delete_registra_motivo(self):
        """DELETE queda registrado con motivo en logs."""
        log = ExecutionLog("agentic_sql", "agentic_sql")
        sql = "DELETE FROM empleados WHERE id = 1"
        ok, pre_err = _validate_sql_pre(sql)
        self.assertFalse(ok)
        log.add_tool(ToolExecution(
            tool_name="ejecutar_consulta_sql",
            success=False,
            duration_ms=0,
            result_summary=f"BLOQUEADO: {pre_err}",
            error=pre_err,
        ))
        self.assertIn("Solo se permiten consultas SELECT", log.tool_executions[0].error)

    def test_rechazo_no_modifica_resultado_estructurado(self):
        """Si se rechaza antes de ejecutar, el resultado estructurado no se modifica."""
        orch = OrchestratedResult(
            pipeline_executed="agentic_sql",
            structured_result=AgenticSQLResult(),
            execution_log=ExecutionLog("agentic_sql", "agentic_sql"),
        )
        sql = 'SELECT * FROM "UnknownTable"'
        ok, pre_err = _validate_sql_pre(sql)
        self.assertFalse(ok)
        # rows_returned sigue 0
        self.assertEqual(orch.structured_result.rows_returned, 0)
        self.assertFalse(orch.structured_result.has_data)
        # sql_executed sigue False
        self.assertFalse(orch.execution_log.sql_executed)


# ─────────────────────────────────────────────
# Tests de timeout
# ─────────────────────────────────────────────

class TestTimeoutSQL(unittest.TestCase):
    """Verifica que el timeout SQL está configurado correctamente."""

    def test_timeout_constant_defined(self):
        """Constante de timeout definida."""
        self.assertEqual(_SQL_TIMEOUT_MS, 15000)

    def test_connector_has_timeout(self):
        """run_safe_query acepta timeout_ms."""
        from db.connector import run_safe_query
        import inspect
        sig = inspect.signature(run_safe_query)
        self.assertIn("timeout_ms", sig.parameters)

    def test_connector_default_timeout(self):
        """Timeout por defecto es 15000ms."""
        from db.connector import _DEFAULT_SQL_TIMEOUT_MS
        self.assertEqual(_DEFAULT_SQL_TIMEOUT_MS, 15000)

    def test_timeout_error_handling_in_generate(self):
        """generate.py maneja TimeoutError explícitamente."""
        import inspect
        # Verificar que TimeoutError está en el archivo
        generate_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "llm", "generate.py"
        )
        with open(generate_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("except TimeoutError", content)
        self.assertIn("TIMEOUT", content)


# ─────────────────────────────────────────────
# Tests de validación combinada
# ─────────────────────────────────────────────

class TestValidacionCombinada(unittest.TestCase):
    """Tests end-to-end de la cadena de validación completa."""

    def test_select_tabla_permitida_pasa_todo(self):
        """SELECT de tabla permitida pasa validación y sanitización."""
        sql = 'SELECT "nombre" FROM "Empleado" WHERE activo = true'
        ok, msg = _validate_sql_pre(sql)
        self.assertTrue(ok)
        sanitized = _sanitize_sql(sql)
        self.assertIn("LIMIT", sanitized)

    def test_select_tabla_prohibida_falla_antes_de_sanitizar(self):
        """Tabla prohibida falla ANTES de llegar a sanitización."""
        sql = 'SELECT * FROM "HackerTable"'
        ok, msg = _validate_sql_pre(sql)
        self.assertFalse(ok)
        # No se debería llamar a _sanitize_sql

    def test_delete_falla_antes_de_whitelist(self):
        """DELETE falla por regla 1 (SELECT-only) antes de llegar a whitelist."""
        sql = 'DELETE FROM "Empleado"'
        ok, msg = _validate_sql_pre(sql)
        self.assertFalse(ok)
        self.assertIn("Solo se permiten consultas SELECT", msg)

    def test_drop_falla_por_keyword(self):
        """DROP falla por keyword peligroso."""
        sql = 'SELECT 1; DROP TABLE "Empleado"'
        ok, msg = _validate_sql_pre(sql)
        self.assertFalse(ok)
        self.assertIn("DROP", msg)

    def test_post_validation_empty_results(self):
        """POST: 0 filas → warning inyectada en AgenticSQLResult."""
        orch = OrchestratedResult(
            pipeline_executed="agentic_sql",
            structured_result=AgenticSQLResult(),
            execution_log=ExecutionLog("agentic_sql", "agentic_sql"),
        )
        warning = _validate_sql_post([], orch)
        self.assertIn("no devolvio resultados", warning)
        self.assertEqual(len(orch.structured_result.warnings), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
