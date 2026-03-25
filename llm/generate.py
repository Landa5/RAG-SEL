"""
llm/generate.py — Agente RAG con orquestación estricta V3.0
Flujo: Orchestrator (classify → pipeline → execute → structured_result) → LLM (solo redacción)
El LLM nunca calcula — solo explica/redacta el resultado del sistema.
"""
import json
import time
import requests
from datetime import datetime, timedelta
from config import GEMINI_API_KEY
from llm.tools import GEMINI_TOOLS
from retrieval.search import search
from db import connector as db
from db import model_db as mdb
from llm.model_router import route as route_model, ModelInfo
from llm.fallback_engine import FallbackEngine
from llm.orchestrator import run_orchestrated_pipeline
from llm.result_contracts import (
    OrchestratedResult, AgenticSQLResult, AgenticSQLRAGResult,
    DocRetrievalResult, ToolExecution,
)
from llm.response_renderer import render_structured_metadata

# Compatibilidad: alias rápido para forzar modelos
_FORCE_MAP = {"pro": "gemini-3.1-pro-preview", "flash": "gemini-3-flash-preview"}

_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/"

def _url_sync(model_name: str) -> str:
    return f"{_BASE_URL}{model_name}:generateContent?key={GEMINI_API_KEY}"

def _url_stream(model_name: str) -> str:
    return f"{_BASE_URL}{model_name}:streamGenerateContent?alt=sse&key={GEMINI_API_KEY}"


# ─────────────────────────────────────────────
# Router dinámico V3.0
# ─────────────────────────────────────────────

def _route_model(pipeline_id: str, query: str, force_model: str = None):
    """Enruta al modelo apropiado."""
    force_id = None
    if force_model:
        force_id = _FORCE_MAP.get(force_model, force_model)

    try:
        routing = route_model(query, pipeline_id, force_id)
    except Exception as e:
        print(f"⚠️ Router falló: {e}. Usando fallback.")
        routing = _emergency_routing()

    return routing


def _emergency_routing():
    """Routing de emergencia si todo falla."""
    from llm.model_router import RoutingDecision
    return RoutingDecision(
        primary=ModelInfo(
            uuid="fallback", model_id="gemini-3-flash-preview",
            display_name="Gemini 3 Flash (emergency)",
            provider="google", price_input=0.50, price_output=3.00
        ),
        fallback_chain=[],
        reason="Emergency fallback — router no disponible",
        composite_score=0.0,
        score_breakdown={},
        all_candidates_scores=[],
    )


def _log_routing_decision(query, classification, pipeline_sel, model_name, routing):
    """Registra la decisión de routing en BD."""
    try:
        return mdb.log_routing_decision(
            query_preview=query[:150],
            task_family=classification["family"],
            detected_language="es",
            detected_features={
                "secondary": classification.get("caps", []),
                "predictive_intent": classification.get("predictive_intent", False),
                "complexity": classification.get("complexity", 3),
            },
            pipeline_id=pipeline_sel,
            pipeline_reason="",
            selected_model=model_name,
            selection_reason=routing.reason,
            composite_score=routing.composite_score,
            score_breakdown=routing.score_breakdown,
            fallback_chain=[m.model_id for m in routing.fallback_chain],
        )
    except Exception as e:
        print(f"⚠️ Error logging routing: {e}")
        return None


def _update_routing_log(log_id, tokens_in, tokens_out, model_info,
                        pipeline_id, latency_ms, error=None,
                        fallback_triggered=False, fallback_model=None,
                        fallback_reason=None):
    """Actualiza log con datos post-ejecución."""
    if not log_id:
        return
    try:
        pi = model_info.price_input if hasattr(model_info, 'price_input') else 0.5
        po = model_info.price_output if hasattr(model_info, 'price_output') else 3.0
        cost = (tokens_in * pi + tokens_out * po) / 1_000_000
        mdb.update_routing_log_execution(
            log_id=log_id, tokens_input=tokens_in, tokens_output=tokens_out,
            cost_estimated=cost, cost_actual=cost, latency_ms=latency_ms,
            error=error, fallback_triggered=fallback_triggered,
            fallback_model_used=fallback_model, fallback_reason=fallback_reason,
        )
    except Exception as e:
        print(f"⚠️ Error actualizando routing log: {e}")


# ─────────────────────────────────────────────
# Esquema de BD para el system prompt
# ─────────────────────────────────────────────

DB_SCHEMA_SEL = """
=== ESQUEMA SEL (Control Horario / Logística) ===
TABLAS (PostgreSQL, nombres entre comillas dobles):

"Empleado": id, usuario, activo(bool), nombre, apellidos, dni, telefono, email, direccion, rol(enum: ADMIN/CONDUCTOR/MECANICO/RRHH), puestoTrabajo, horaEntradaPrevista, horaSalidaPrevista, diasVacaciones, diasExtras, horasExtra, fechaAlta, fechaBaja
"JornadaLaboral": id, fecha, horaEntrada, horaSalida, totalHoras(float), estado, observaciones, empleadoId → Empleado
"UsoCamion": id, jornadaId → JornadaLaboral, camionId → Camion, horaInicio, horaFin, kmInicial, kmFinal, kmRecorridos, descargasCount, viajesCount, litrosRepostados(float), notas
"Camion": id, matricula, modelo, marca, kmActual, itvVencimiento, seguroVencimiento, tacografoVencimiento, adrVencimiento, activo(bool)
"MantenimientoRealizado": id, camionId → Camion, fecha, taller, kmEnEseMomento, descripcion, piezasCambiadas, costo(float), tipo, proximoKmPrevisto
"MantenimientoProximo": id, camionId → Camion, tipoMantenimiento, kmObjetivo, fechaObjetivo, estado
"Ausencia": id, tipo(enum: VACACIONES/BAJA_MEDICA/PERMISO/OTROS), fechaInicio, fechaFin, estado, observaciones, horas, empleadoId → Empleado
"NominaMes": id, empleadoId → Empleado, year, month, estado, totalBruto(float), totalVariables(float)
"NominaLinea": id, nominaId → NominaMes, conceptoNombre, cantidad, rate, importe, orden
"Tarea": id, tipo, estado, prioridad, matricula, titulo, descripcion, fechaLimite, fechaInicio, fechaCierre, resumenCierre, creadoPorId → Empleado, asignadoAId → Empleado, camionId → Camion, descargas, proyectoId, privada(bool)
"TareaHistorial": id, tareaId → Tarea, autorId → Empleado, tipoAccion, mensaje, estadoNuevo, createdAt
"Descarga": id, hora, litros, tipoGasoil, lugar, usoCamionId → UsoCamion
"Documento": id, nombre, tipo, url, camionId → Camion, empleadoId → Empleado, createdAt
"RevisionAccesorios": id, camionId → Camion, empleadoId → Empleado, mes, instruccionesEscritas(bool), guantes(bool), triangulos(bool), observaciones
"Proyecto": id, nombre, descripcion, activo(bool), estado
"TachographDailySummary": id, driverId → TachographDriver, vehicleId, date, totalDrivingMinutes, totalOtherWorkMinutes, totalRestMinutes, totalBreakMinutes
"TachographDriver": id, linkedEmployeeId → Empleado, fullName, cardNumber, active(bool)
"TachographVehicle": id, linkedVehicleId → Camion, plateNumber, vin, active(bool)

RELACIONES CLAVE SEL:
- UsoCamion.jornadaId → JornadaLaboral → Empleado (conductor → camión)
- Descarga.usoCamionId → UsoCamion
- MantenimientoRealizado.camionId → Camion
- TachographDriver.linkedEmployeeId → Empleado
"""

DB_SCHEMA_CROMOS = """
=== ESQUEMA APP CROMOS (Gestión de cromos/cards coleccionables) ===
TABLAS (PostgreSQL, nombres entre comillas dobles):

"Card": id, player(text), year(text), year_season(text), collection(text), collection_number(text), card_number(text), description(text), publisher(text), variant(text), print_run(text), owner(text), state(text: IN_INVENTORY/GRADING/SOLD/OTHER), alias_visible(text), certificate_type(text), certificate_number(text), grade(text), notes(text), "createdAt", "updatedAt", "deletedAt"

"Purchase": id, "cardId" → Card, date(text), source(text), payer(text), units(int), unit_price(float), shipping(float), taxes(float), total_price(float), currency(text), exchange_rate(float), notes(text), economic_owner(text), recorded_by(text), import_key(text), year_season(text), box(text), source_sheet(text), source_row(int), "deletedAt", "createdAt", "updatedAt"

"Sale": id, "cardId" → Card, date(text), platform(text), buyer(text), sale_price(float), sale_plus_shipping(float), commission(float), shipping_cost(float), net_profit(float), total_amount(float), shipping_charged(float), currency(text), units(int), notes(text), economic_owner(text), cash_receiver(text), recorded_by(text), import_key(text), season(text), certificate(text), "deletedAt", "createdAt", "updatedAt"

"InventoryUnit": id, "cardId" → Card, "purchaseId" → Purchase, unit_index(int), state(text: RAW/GRADING/GRADED/FOR_SALE/SOLD/RESERVED), box_id(int), sale_id(int), certificate_number(text), grade(text), grading_cost(float), verified(int), notes(text), economic_owner(text), cost_base(float), cost_extra(float), "createdAt", "updatedAt"

"GradingSubmission": id, "submissionId"(text), service(text: PSA/BGS/SGC), status(text: DRAFT/SENT/IN_PROCESS/RECEIVED), date_created(text), date_sent(text), date_received(text), shipping_cost(float), grading_cost(float), total_cards(int), notes(text), "createdAt", "updatedAt"
"GradingItem": id, "submissionId" → GradingSubmission, "cardId" → Card, cert_number(text), grade(text), cost_per_card(float), notes(text), "createdAt"

"CostAllocationEvent": id, date(text), type(text), description(text), total_amount(float), allocation_method(text), source_reference(text), notes(text), payer(text), economic_owner(text), "createdAt"
"CostAllocationItem": id, event_id → CostAllocationEvent, "cardId" → Card, "purchaseId" → Purchase, allocated_amount(float), declared_value(float), affected_units(int), manual_amount(float), notes(text), comment(text)

"FinancialTransaction": id, date(timestamptz), direction(text: OUTFLOW/INFLOW/TRANSFER/ADJUSTMENT), category(text), amount(float), currency(text), exchange_rate(float), amount_eur(float), recorded_by(text), payer(text), economic_owner(text), cash_receiver(text), description(text), notes(text), source_entity_type(text), source_entity_id(int), purchase_lot_id(int), inventory_unit_id(int), sale_id(int), "createdAt"
"PartnerTransaction": id, date(timestamptz), from_partner(text), to_partner(text), amount(float), type(text: LOAN/REPAYMENT/COMPENSATION/ADJUSTMENT/SETTLEMENT), description(text), financial_tx_id → FinancialTransaction, "createdAt"

"Box": id, name(text), location(text), description(text), "createdAt"
"BoxMovement": id, unit_id → InventoryUnit, from_box_id(int), to_box_id(int), moved_by(text), reason(text), "createdAt"
"SaleItem": id, sale_id → Sale, inventory_unit_id → InventoryUnit, sale_price(float), allocated_shipping(float), allocated_commission(float), economic_owner(text), "createdAt"
"Expense": id, date(text), category(text), description(text), amount(float), payer(text), notes(text), "deletedAt", "createdAt"
"GeneralExpense": id, date(text), concept(text), units(int), amount(float), notes(text), "createdAt"
"User": id, name(text), email(text), password(text), role(text: ADMIN/OPERATOR), "createdAt"
"CardAttachment": id, card_id → Card, file_name(text), file_url(text), file_type(text), file_size(int), uploaded_by(text), "createdAt"

RELACIONES CLAVE CROMOS:
- InventoryUnit."cardId" → Card (cada unidad pertenece a un cromo)
- InventoryUnit."purchaseId" → Purchase (cada unidad viene de una compra)
- Purchase."cardId" → Card (compra de un cromo)
- Sale."cardId" → Card (venta de un cromo)
- GradingItem."cardId" → Card, GradingItem."submissionId" → GradingSubmission
- SaleItem.inventory_unit_id → InventoryUnit (qué unidad se vendió)
- CostAllocationItem.event_id → CostAllocationEvent (gastos imputados)

CONSULTAS ÚTILES CROMOS:
- Total inversión: SELECT SUM(total_price) FROM "Purchase" WHERE "deletedAt" IS NULL
- Cromos en inventario: SELECT COUNT(*) FROM "Card" WHERE state = 'IN_INVENTORY' AND "deletedAt" IS NULL
- Unidades por estado: SELECT state, COUNT(*) FROM "InventoryUnit" GROUP BY state
- Jugador más comprado: SELECT c.player, COUNT(*) FROM "Card" c JOIN "Purchase" p ON p."cardId" = c.id GROUP BY c.player ORDER BY COUNT(*) DESC
"""

# Schema combinado para el prompt
DB_SCHEMA = DB_SCHEMA_SEL + "\n" + DB_SCHEMA_CROMOS


def get_system_prompt() -> str:
    """Genera el prompt de sistema con la fecha actual y esquema de BD."""
    now = datetime.now()
    lunes = now - timedelta(days=now.weekday())
    domingo = lunes + timedelta(days=6)

    fecha_hoy = now.strftime("%d/%m/%Y %H:%M")
    dia_semana = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"][now.weekday()]
    rango_semana = f"{lunes.strftime('%Y-%m-%d')} al {domingo.strftime('%Y-%m-%d')}"
    fecha_iso = now.strftime("%Y-%m-%d")

    return f"""Eres un asistente inteligente multi-tenant. Según la app conectada, puedes gestionar datos de:
- SEL (Servicios y Entregas Logísticas): empleados, jornadas, camiones, nóminas, ausencias, tacógrafo.
- App Cromos: inventario de cromos/cards coleccionables, compras, ventas, grading PSA/BGS, costes, cajas.

Tienes acceso a dos herramientas: una para consultar la base de datos SQL y otra para buscar en documentos PDF.

FECHA ACTUAL: Hoy es {dia_semana} {fecha_hoy}. Fecha ISO: {fecha_iso}. Semana actual: {rango_semana}.

ESQUEMA DE LA BASE DE DATOS:
{DB_SCHEMA}

REGLAS SQL:
- Usa SIEMPRE comillas dobles para nombres de tabla y columnas con mayúsculas: "Card", "cardId", "Empleado", "kmRecorridos", etc.
- Usa JOINs para combinar tablas relacionadas.
- Para App Cromos: filtra siempre WHERE "deletedAt" IS NULL para excluir registros eliminados.
- Para SEL: Cuando pregunten por "esta semana", filtra: WHERE fecha >= '{lunes.strftime('%Y-%m-%d')}' AND fecha <= '{domingo.strftime('%Y-%m-%d')}'
- Para SEL: Cuando pregunten por "hoy", filtra: WHERE fecha::date = '{fecha_iso}'
- Añade ORDER BY y LIMIT para consultas grandes.
- Puedes hacer múltiples consultas si necesitas datos de diferentes tablas.

COMPORTAMIENTO OBLIGATORIO:
- SIEMPRE usa las herramientas INMEDIATAMENTE sin pedir confirmación.
- NUNCA hagas preguntas al usuario. NUNCA termines con "¿Te gustaría...?" ni "¿Puedo...?".
- Responde SIEMPRE en español con datos concretos.
- Si una consulta SQL falla, intenta corregirla y volver a ejecutar.

CUÁNDO USAR CADA HERRAMIENTA:
- 'ejecutar_consulta_sql' → datos de empleados, jornadas, camiones, nóminas, ausencias, tacógrafo, Y TAMBIÉN cromos, inventario, compras, ventas, grading, costes.
- 'buscar_documentos_pdf' → facturas, normativas, convenios colectivos, albaranes, documentos escaneados.

DETECCIÓN DE DOMINIO:
- Si la pregunta menciona cromos, cards, jugadores, colecciones, PSA, grading, inventario de cromos → usa tablas del esquema CROMOS.
- Si la pregunta menciona empleados, conductores, camiones, jornadas, nóminas → usa tablas del esquema SEL.

FORMATO DE RESPUESTA:
- Sé conciso y directo. Usa listas o tablas cuando haya muchos datos.
- Si citas documentos, incluye el nombre del archivo.
- No inventes datos. Si no hay información suficiente, di qué datos faltan (sin preguntar)."""


# ─────────────────────────────────────────────
# Herramientas SQL/RAG
# ─────────────────────────────────────────────

def _format_sql_results(rows: list[dict]) -> str:
    if not rows:
        return "La consulta no devolvió resultados."
    headers = list(rows[0].keys())
    lines = [" | ".join(str(h) for h in headers)]
    lines.append("-" * len(lines[0]))
    for row in rows:
        vals = []
        for h in headers:
            v = row[h]
            if v is None:
                vals.append("–")
            elif isinstance(v, datetime):
                vals.append(v.strftime("%d/%m/%Y %H:%M"))
            elif isinstance(v, float):
                vals.append(f"{v:.2f}")
            else:
                vals.append(str(v))
        lines.append(" | ".join(vals))
    return f"{len(rows)} resultados:\n" + "\n".join(lines)


# ─────────────────────────────────────────────
# Validación SQL endurecida V3.0
# ─────────────────────────────────────────────

_ALLOWED_TABLES = {
    # SEL tables
    'Empleado', 'JornadaLaboral', 'UsoCamion', 'Camion',
    'MantenimientoRealizado', 'MantenimientoProximo', 'Ausencia',
    'NominaMes', 'NominaLinea', 'Tarea', 'TareaHistorial',
    'Descarga', 'Documento', 'RevisionAccesorios', 'Proyecto',
    'TachographDailySummary', 'TachographDriver', 'TachographVehicle',
    # App Cromos tables
    'Card', 'Purchase', 'Sale', 'InventoryUnit',
    'GradingSubmission', 'GradingItem',
    'CostAllocationEvent', 'CostAllocationItem',
    'FinancialTransaction', 'PartnerTransaction',
    'Box', 'BoxMovement', 'SaleItem',
    'Expense', 'GeneralExpense', 'User', 'CardAttachment',
    'AuditLog', 'MigrationIssue', 'FinancialIssue', 'CashAccount',
}

_DANGEROUS_KEYWORDS = [
    'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'TRUNCATE',
    'CREATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE', 'CALL',
    '--', '/*', '*/', 'pg_', 'information_schema',
]

_SQL_MAX_ROWS = 100
_SQL_TIMEOUT_MS = 15000  # 15 segundos


def _extract_tables_from_sql(sql: str) -> list[str]:
    """Extrae nombres de tabla referenciados en una query SQL."""
    import re
    tables = set()
    # Patrón: FROM "Table" o JOIN "Table" (con comillas dobles)
    for m in re.finditer(r'(?:FROM|JOIN)\s+"([^"]+)"', sql, re.IGNORECASE):
        tables.add(m.group(1))
    # Patrón: FROM Table o JOIN Table (sin comillas, PascalCase)
    for m in re.finditer(r'(?:FROM|JOIN)\s+([A-Z][a-zA-Z]+)(?:\s|$|,)', sql):
        tables.add(m.group(1))
    return list(tables)


def _validate_sql_pre(sql: str) -> tuple[bool, str]:
    """
    Validación PRE-ejecución de SQL. Devuelve (ok, mensaje_error).
    Reglas:
      1. Solo SELECT
      2. Sin keywords peligrosos
      3. Whitelist activa de tablas
    """
    sql_upper = sql.upper().strip()

    # 1. Solo SELECT
    if not sql_upper.startswith('SELECT'):
        return False, 'Solo se permiten consultas SELECT.'

    # 2. Operaciones peligrosas
    for kw in _DANGEROUS_KEYWORDS:
        if kw.upper() in sql_upper:
            return False, f'Operacion SQL peligrosa detectada: {kw}'

    # 3. Whitelist activa de tablas
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


def _validate_sql_post(rows: list, orch_result: OrchestratedResult = None) -> str:
    """Validación POST-ejecución de SQL. Inyecta advertencias si procede."""
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


def execute_tool(call_name: str, call_args: dict, collected_sources: list,
                 orch_result: OrchestratedResult = None,
                 tenant_ctx=None) -> str:
    """Ejecuta la herramienta local y registra en trazabilidad. Multi-tenant aware."""
    start = time.time()

    # Extraer contexto del tenant
    _tid = tenant_ctx.tenant_id if tenant_ctx else None
    _db_url = tenant_ctx.database_url if tenant_ctx else None

    if call_name == "buscar_documentos_pdf":
        query = call_args.get("query", "")
        try:
            print(f"🛠️  Agente ejecutando: buscar_documentos_pdf({query})")
            chunks = search(query, tenant_id=_tid)
            duration = int((time.time() - start) * 1000)

            # Registrar en trazabilidad
            if orch_result:
                orch_result.execution_log.retrieval_executed = True
                orch_result.execution_log.add_tool(ToolExecution(
                    tool_name="buscar_documentos_pdf",
                    success=True,
                    duration_ms=duration,
                    result_summary=f"{len(chunks)} chunks",
                    input_preview=query[:200],
                ))
                # Actualizar resultado estructurado
                _update_structured_with_rag(orch_result, chunks)

            if not chunks:
                return "No se encontraron documentos relevantes."
            res = []
            for i, c in enumerate(chunks, 1):
                res.append(f"[Fragmento {i} - {c['source']}, pág {c['page']}]\n{c['text']}")
                collected_sources.append({"source": c["source"], "page": c["page"], "score": c.get("score", 0)})
            return "\n\n---\n\n".join(res)
        except Exception as e:
            duration = int((time.time() - start) * 1000)
            if orch_result:
                orch_result.execution_log.add_tool(ToolExecution(
                    tool_name="buscar_documentos_pdf", success=False,
                    duration_ms=duration, result_summary=f"Error: {str(e)[:100]}",
                    error=str(e),
                ))
            return f"Error buscando documentos: {str(e)}"

    elif call_name == "ejecutar_consulta_sql":
        sql = call_args.get("sql", "")
        desc = call_args.get("descripcion", "")

        # Validación PRE-ejecución
        ok, pre_err = _validate_sql_pre(sql)
        if not ok:
            duration = int((time.time() - start) * 1000)
            if orch_result:
                orch_result.execution_log.add_tool(ToolExecution(
                    tool_name="ejecutar_consulta_sql", success=False,
                    duration_ms=duration, result_summary=f"BLOQUEADO: {pre_err}",
                    error=pre_err, input_preview=sql[:200],
                ))
            return f"SQL rechazado: {pre_err}"

        try:
            sql = _sanitize_sql(sql)
            print(f"SQL [{desc}]: {sql}")
            rows = db.run_safe_query(sql, database_url=_db_url)
            result = _format_sql_results(rows)
            duration = int((time.time() - start) * 1000)
            print(f"   -> {len(rows)} filas")

            # Registrar en trazabilidad
            if orch_result:
                orch_result.execution_log.sql_executed = True
                orch_result.execution_log.add_tool(ToolExecution(
                    tool_name="ejecutar_consulta_sql",
                    success=True,
                    duration_ms=duration,
                    result_summary=f"{len(rows)} filas -- {desc}",
                    input_preview=sql[:200],
                ))
                _update_structured_with_sql(orch_result, sql, desc, rows)

            # Validación POST-ejecución
            post_warning = _validate_sql_post(rows, orch_result)
            if post_warning:
                result += f"\n\nADVERTENCIA: {post_warning}"

            return result
        except TimeoutError as te:
            duration = int((time.time() - start) * 1000)
            timeout_msg = str(te)
            print(f"   TIMEOUT SQL: {timeout_msg}")
            if orch_result:
                orch_result.execution_log.add_tool(ToolExecution(
                    tool_name="ejecutar_consulta_sql", success=False,
                    duration_ms=duration, result_summary=f"TIMEOUT: {timeout_msg[:100]}",
                    error=timeout_msg, input_preview=sql[:200],
                ))
            return f"La consulta SQL excedio el tiempo maximo permitido ({_SQL_TIMEOUT_MS}ms). Simplifica la consulta."
        except Exception as e:
            error_msg = str(e).split("\n")[0]
            duration = int((time.time() - start) * 1000)
            print(f"   Error SQL: {error_msg}")
            if orch_result:
                orch_result.execution_log.add_tool(ToolExecution(
                    tool_name="ejecutar_consulta_sql", success=False,
                    duration_ms=duration, result_summary=f"Error: {error_msg[:100]}",
                    error=error_msg, input_preview=sql[:200],
                ))
            return f"Error SQL: {error_msg}. Revisa nombres de tabla/columna en el esquema y reintenta."

    elif call_name == "consultar_base_datos":
        return "Herramienta obsoleta. Usa 'ejecutar_consulta_sql' para consultar la base de datos."

    return f"Herramienta desconocida: {call_name}"


def _update_structured_with_sql(orch: OrchestratedResult, sql: str, desc: str, rows: list):
    """Actualiza resultado estructurado con datos SQL post-ejecución."""
    sr = orch.structured_result
    query_info = {"sql": sql, "description": desc, "rows_count": len(rows)}

    if isinstance(sr, AgenticSQLResult):
        sr.sql_queries.append(query_info)
        sr.rows_returned += len(rows)
        if rows:
            sr.structured_findings += _format_sql_results(rows) + "\n"
            sr.raw_results.extend(rows)
            sr.confidence = min(1.0, sr.confidence + 0.3)

    elif isinstance(sr, AgenticSQLRAGResult) and sr.sql_result:
        sr.sql_result.sql_queries.append(query_info)
        sr.sql_result.rows_returned += len(rows)
        if rows:
            sr.sql_result.structured_findings += _format_sql_results(rows) + "\n"
            sr.sql_result.raw_results.extend(rows)
            sr.sql_result.confidence = min(1.0, sr.sql_result.confidence + 0.3)
            sr.confidence = min(1.0, sr.confidence + 0.2)


def _update_structured_with_rag(orch: OrchestratedResult, chunks: list):
    """Actualiza resultado estructurado con datos RAG post-ejecución."""
    sr = orch.structured_result

    if isinstance(sr, AgenticSQLRAGResult) and sr.rag_result:
        sr.rag_result.retrieved_chunks.extend(chunks)
        new_sources = {
            (c["source"], c["page"]): {"source": c["source"], "page": c["page"], "score": c.get("score", 0)}
            for c in chunks
        }
        sr.rag_result.sources = list(new_sources.values())
        sr.confidence = min(1.0, sr.confidence + 0.2)


# ─────────────────────────────────────────────
# Generación con orquestación estricta
# ─────────────────────────────────────────────

def generate_answer_stream(query: str, chat_history: list = None,
                           force_model: str = None, tenant_ctx=None):
    """
    Agente RAG con Streaming (SSE) + Orquestación Estricta V3.0.
    1. Orquestador ejecuta pipeline real → resultado estructurado
    2. LLM recibe prompt restringido → solo explica/redacta
    3. Post-ejecución: valida que se usaron herramientas reales
    tenant_ctx: PropagacIón multi-tenant a búsquedas y SQL.
    """
    start_total = time.time()

    # ═══ PASO 1: Orquestación (ANTES del LLM) ═══
    orch = run_orchestrated_pipeline(query, force_model, tenant_ctx=tenant_ctx)

    # Enrutar modelo
    routing = _route_model(orch.pipeline_executed, query, force_model)
    model_name = routing.primary.model_id
    orch.model_name = model_name
    url_sync = _url_sync(model_name)

    print(f"🧠 Modelo: {model_name} (score={routing.composite_score:.3f}) — {routing.reason}")

    # Log de routing
    log_id = _log_routing_decision(
        query, orch.classification_info, orch.pipeline_executed,
        model_name, routing,
    )

    # Emitir status al frontend
    tier = "⚡ Flash" if "flash" in model_name.lower() else "🧠 Pro"
    pipeline_label = orch.pipeline_executed.replace('_', ' ').title()
    family = orch.classification_info.get("family", "?")
    yield f"data: {json.dumps({'status': f'{tier} · {pipeline_label} · {family}'}, ensure_ascii=False)}\n\n"

    # ═══ PASO 2: Status de pre-ejecución ═══
    if orch.execution_log.forecast_engine_executed:
        fr = orch.structured_result
        if hasattr(fr, 'prediction'):
            yield f"data: {json.dumps({'status': f'📊 Predicción: {fr.prediction:.2f} ({fr.confidence:.0%} confianza)'}, ensure_ascii=False)}\n\n"
        else:
            yield f"data: {json.dumps({'status': '📊 Análisis descriptivo (datos insuficientes para predicción cuantitativa)'}, ensure_ascii=False)}\n\n"
    elif orch.execution_log.degraded_from:
        yield f"data: {json.dumps({'status': f'⚠️ Degradado de {orch.execution_log.degraded_from} a {orch.pipeline_executed}'}, ensure_ascii=False)}\n\n"
    elif orch.execution_log.retrieval_executed:
        sr = orch.structured_result
        if hasattr(sr, 'retrieved_chunks'):
            yield f"data: {json.dumps({'status': f'📚 {len(sr.retrieved_chunks)} documentos relevantes'}, ensure_ascii=False)}\n\n"

    # ═══ PASO 3: Construir prompt para el LLM ═══
    effective_query = orch.prompt_for_llm

    # Determinar si el LLM necesita tools
    needs_tools = orch.pipeline_executed in ("agentic_sql", "agentic_sql_rag", "predictive_insight")

    contents = []
    contents.append({"role": "user", "parts": [{"text": get_system_prompt()}]})
    contents.append({"role": "model", "parts": [{"text": "Entendido."}]})
    if chat_history:
        for msg in chat_history[-6:]:
            role = "user" if msg["role"] == "user" else "model"
            contents.append({"role": role, "parts": [{"text": msg["content"]}]})
    contents.append({"role": "user", "parts": [{"text": effective_query}]})

    payload = {
        "contents": contents,
        "generationConfig": {"temperature": 0.1}
    }

    # Solo dar tools al LLM si el pipeline lo requiere
    if needs_tools:
        payload["tools"] = GEMINI_TOOLS
        # Forzar al LLM a usar al menos una herramienta en la primera respuesta
        payload["tool_config"] = {
            "function_calling_config": {
                "mode": "ANY"
            }
        }

    total_tokens_in = 0
    total_tokens_out = 0

    resp1 = requests.post(url_sync, json=payload, timeout=60)
    if resp1.status_code != 200:
        yield f"data: {json.dumps({'error': 'Error en Gemini API'}, ensure_ascii=False)}\n\n"
        _update_routing_log(log_id, 0, 0, routing.primary, orch.pipeline_executed,
                            int((time.time()-start_total)*1000), error=f"HTTP {resp1.status_code}")
        return

    data = resp1.json()
    total_tokens_in += data.get("usageMetadata", {}).get("promptTokenCount", 0)
    total_tokens_out += data.get("usageMetadata", {}).get("candidatesTokenCount", 0)

    if not data.get("candidates"):
        yield f"data: {json.dumps({'chunk': 'Sin respuesta del modelo.'}, ensure_ascii=False)}\n\n"
        return

    message = data["candidates"][0]["content"]
    parts = message.get("parts", [])

    # ═══ PASO 4: Tool calling (solo si needs_tools) ═══
    MAX_TOOL_ROUNDS = 5
    collected_sources = []
    used_tools = False

    if needs_tools:
        for _round in range(MAX_TOOL_ROUNDS):
            func_calls = [p for p in parts if "functionCall" in p]
            if not func_calls:
                break

            used_tools = True
            contents.append(message)

            func_responses = []
            for fc_part in func_calls:
                call_name = fc_part["functionCall"]["name"]
                call_args = fc_part["functionCall"].get("args", {})

                desc = call_args.get("descripcion", call_name)
                yield f"data: {json.dumps({'status': f'Consultando: {desc}...'}, ensure_ascii=False)}\n\n"
                tool_res = execute_tool(call_name, call_args, collected_sources, orch, tenant_ctx=tenant_ctx)

                func_responses.append({
                    "functionResponse": {
                        "name": call_name,
                        "response": {"result": tool_res}
                    }
                })

            contents.append({"role": "function", "parts": func_responses})
            payload["contents"] = contents

            resp_next = requests.post(url_sync, json=payload, timeout=60)
            if resp_next.status_code != 200:
                err = resp_next.text[:300]
                print(f"⚠️  Error Gemini ronda {_round+1}: {resp_next.status_code} - {err}")
                yield f"data: {json.dumps({'error': f'Error en Gemini API: {resp_next.status_code}'}, ensure_ascii=False)}\n\n"
                _update_routing_log(log_id, total_tokens_in, total_tokens_out, routing.primary,
                                    orch.pipeline_executed, int((time.time()-start_total)*1000),
                                    error=f"Round {_round+1}: HTTP {resp_next.status_code}")
                return

            data_next = resp_next.json()
            total_tokens_in += data_next.get("usageMetadata", {}).get("promptTokenCount", 0)
            total_tokens_out += data_next.get("usageMetadata", {}).get("candidatesTokenCount", 0)

            if not data_next.get("candidates"):
                yield f"data: {json.dumps({'chunk': 'Sin respuesta del modelo.'}, ensure_ascii=False)}\n\n"
                return

            message = data_next["candidates"][0]["content"]
            parts = message.get("parts", [])

    # ═══ PASO 5: Validación post-ejecución ═══
    valid, validation_msg = orch.validate()
    execution_error = None
    if not valid:
        execution_error = validation_msg
        if orch.is_critical:
            print(f"CONTRATO INCUMPLIDO [{orch.pipeline_executed}]: {validation_msg}")
            yield f"data: {json.dumps({'status': f'Advertencia: {validation_msg}'}, ensure_ascii=False)}\n\n"

    # ═══ PASO 6: Evidence gate ═══
    # Si pipeline RAG sin evidencia → inyectar advertencia explícita
    sr = orch.structured_result
    if orch.pipeline_executed in ("doc_retrieval", "agentic_sql_rag"):
        has_rag_evidence = False
        if isinstance(sr, DocRetrievalResult):
            has_rag_evidence = sr.has_evidence
        elif isinstance(sr, AgenticSQLRAGResult) and sr.rag_result:
            has_rag_evidence = sr.rag_result.has_evidence
        if not has_rag_evidence:
            yield f"data: {json.dumps({'status': 'Sin evidencia documental suficiente'}, ensure_ascii=False)}\n\n"

    # Si pipeline SQL sin ejecución real → inyectar advertencia
    if orch.pipeline_executed in ("agentic_sql", "agentic_sql_rag"):
        if not orch.execution_log.sql_executed and not used_tools:
            yield f"data: {json.dumps({'status': 'Sin ejecucion SQL real'}, ensure_ascii=False)}\n\n"

    # ═══ PASO 7: Emitir fuentes y respuesta ═══
    unique_sources = list({(s["source"], s["page"]): s for s in collected_sources}.values())
    if unique_sources:
        yield f"data: {json.dumps({'sources': unique_sources}, ensure_ascii=False)}\n\n"

    text_parts = [p for p in parts if "text" in p]
    if text_parts:
        for tp in text_parts:
            yield f"data: {json.dumps({'chunk': tp['text']}, ensure_ascii=False)}\n\n"
    elif not used_tools:
        yield f"data: {json.dumps({'chunk': 'No se pudo generar una respuesta.'}, ensure_ascii=False)}\n\n"

    # ═══ PASO 8: Logging final — SIEMPRE se ejecuta ═══
    latency_ms = int((time.time() - start_total) * 1000)
    orch.execution_log.total_duration_ms = latency_ms
    if execution_error:
        orch.execution_log.error = execution_error

    _update_routing_log(log_id, total_tokens_in, total_tokens_out,
                        routing.primary, orch.pipeline_executed, latency_ms,
                        error=execution_error)

    # Log de ejecución del pipeline — OBLIGATORIO en éxito, error y degradación
    try:
        mdb.log_pipeline_execution(orch.execution_log)
    except Exception as e:
        print(f"Error logging pipeline execution: {e}")

    # Metadata de routing + orquestación
    meta = render_structured_metadata(orch)
    meta["model"] = model_name
    meta["score"] = round(routing.composite_score, 3)
    if execution_error:
        meta["contract_error"] = execution_error
    yield f"data: {json.dumps({'routing': meta}, ensure_ascii=False)}\n\n"

    yield "data: [DONE]\n\n"


def generate_answer_agentic(query: str, chat_history: list = None,
                            tenant_ctx=None) -> dict:
    """Agente RAG síncrono con orquestación estricta V3.0."""
    chunks = []
    full_text = ""
    sources = []
    pipeline = ""
    family = ""
    model = ""

    for chunk_str in generate_answer_stream(query, chat_history, tenant_ctx=tenant_ctx):
        if not chunk_str.startswith("data: "):
            continue
        data_str = chunk_str[6:].strip()
        if data_str == "[DONE]":
            break
        try:
            data = json.loads(data_str)
            if "chunk" in data:
                full_text += data["chunk"]
            if "sources" in data:
                sources = data["sources"]
            if "routing" in data:
                pipeline = data["routing"].get("pipeline_executed", "")
                family = data["routing"].get("result_type", "")
                model = data["routing"].get("model", "")
        except json.JSONDecodeError:
            continue

    return {
        "answer": full_text or "No se pudo generar una respuesta.",
        "sources": sources,
        "source_mode": "orchestrated",
        "model_used": model,
        "pipeline": pipeline,
        "task_family": family,
    }


# ── Compatibilidad con viejo app/main.py ──
def generate_answer(*args, **kwargs) -> dict:
    if "query" in kwargs:
        query = kwargs["query"]
        history = kwargs.get("chat_history")
    else:
        query = args[0]
        history = args[2] if len(args) > 2 else []
    return generate_answer_agentic(query, history)
