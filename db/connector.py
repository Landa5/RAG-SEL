"""
db/connector.py — Consultas de solo lectura a la BD de sel-control-horario
"""
import os
import psycopg2
import psycopg2.extras
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")


def _clean_db_url(url: str) -> str:
    """
    Elimina parámetros de query que psycopg2 no soporta
    (ej: pgbouncer=true que añade Supabase).
    """
    from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
    parsed = urlparse(url)
    # Filtrar parámetros no soportados por psycopg2
    params = parse_qs(parsed.query, keep_blank_values=True)
    params.pop("pgbouncer", None)
    params.pop("sslmode", None)  # lo ponemos nosotros si hace falta
    clean_query = urlencode({k: v[0] for k, v in params.items()})
    clean = urlunparse(parsed._replace(query=clean_query))
    return clean


def _get_conn():
    """Abre una conexión a PostgreSQL compatible con Supabase + psycopg2."""
    clean_url = _clean_db_url(DATABASE_URL)
    conn = psycopg2.connect(
        clean_url,
        cursor_factory=psycopg2.extras.RealDictCursor,
        sslmode="require"
    )
    conn.set_session(readonly=True, autocommit=True)
    return conn



# ─────────────────────────────────────────────
# Empleados
# ─────────────────────────────────────────────

def get_empleados(solo_activos: bool = True) -> list[dict]:
    """Devuelve la lista de empleados (activos por defecto)."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            filtro = "WHERE activo = TRUE" if solo_activos else ""
            cur.execute(f"""
                SELECT id, nombre, apellidos, rol, "puestoTrabajo",
                       "horaEntradaPrevista", "horaSalidaPrevista",
                       "diasVacaciones", "diasExtras", "horasExtra",
                       "fechaAlta", "fechaBaja", activo
                FROM "Empleado"
                {filtro}
                ORDER BY nombre
            """)
            return [dict(r) for r in cur.fetchall()]


def buscar_empleado(nombre_parcial: str) -> list[dict]:
    """Busca empleados por nombre o apellidos (insensible a mayúsculas)."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, nombre, apellidos, rol, "puestoTrabajo", activo,
                       "diasVacaciones", "diasExtras", "horasExtra"
                FROM "Empleado"
                WHERE LOWER(nombre || ' ' || COALESCE(apellidos,'')) LIKE LOWER(%s)
                ORDER BY nombre
            """, (f"%{nombre_parcial}%",))
            return [dict(r) for r in cur.fetchall()]


# ─────────────────────────────────────────────
# Jornadas
# ─────────────────────────────────────────────

def get_jornadas_empleado(empleado_id: int, mes: int, anio: int) -> list[dict]:
    """Devuelve las jornadas de un empleado en un mes/año concreto."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT j.id, j.fecha, j."horaEntrada", j."horaSalida",
                       j."totalHoras", j.estado, j.observaciones
                FROM "JornadaLaboral" j
                WHERE j."empleadoId" = %s
                  AND EXTRACT(MONTH FROM j.fecha) = %s
                  AND EXTRACT(YEAR  FROM j.fecha) = %s
                ORDER BY j.fecha
            """, (empleado_id, mes, anio))
            return [dict(r) for r in cur.fetchall()]


def get_resumen_jornadas(empleado_id: int, mes: int, anio: int) -> dict:
    """Devuelve un resumen agregado de jornadas (días trabajados, horas totales)."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS dias_trabajados,
                    COALESCE(SUM("totalHoras"), 0) AS horas_totales,
                    COUNT(*) FILTER (WHERE estado = 'INCIDENCIA') AS incidencias
                FROM "JornadaLaboral"
                WHERE "empleadoId" = %s
                  AND EXTRACT(MONTH FROM fecha) = %s
                  AND EXTRACT(YEAR  FROM fecha) = %s
            """, (empleado_id, mes, anio))
            row = cur.fetchone()
            return dict(row) if row else {}


# ─────────────────────────────────────────────
# Ausencias
# ─────────────────────────────────────────────

def get_ausencias(empleado_id: int = None, mes: int = None,
                  anio: int = None, tipo: str = None) -> list[dict]:
    """Devuelve ausencias con filtros opcionales."""
    conditions = []
    params = []

    if empleado_id:
        conditions.append('"empleadoId" = %s')
        params.append(empleado_id)
    if mes and anio:
        conditions.append("""
            (EXTRACT(MONTH FROM "fechaInicio") = %s AND EXTRACT(YEAR FROM "fechaInicio") = %s)
            OR (EXTRACT(MONTH FROM "fechaFin")   = %s AND EXTRACT(YEAR FROM "fechaFin")   = %s)
        """)
        params.extend([mes, anio, mes, anio])
    if tipo:
        conditions.append("tipo = %s")
        params.append(tipo.upper())

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT a.id, a.tipo, a."fechaInicio", a."fechaFin",
                       a.estado, a.observaciones, a.horas,
                       e.nombre, e.apellidos
                FROM "Ausencia" a
                JOIN "Empleado" e ON e.id = a."empleadoId"
                {where}
                ORDER BY a."fechaInicio" DESC
                LIMIT 50
            """, params)
            return [dict(r) for r in cur.fetchall()]


# ─────────────────────────────────────────────
# Nóminas
# ─────────────────────────────────────────────

def get_nomina(empleado_id: int, mes: int, anio: int) -> dict | None:
    """Devuelve el resumen de nómina de un empleado en un mes/año."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT n.id, n.year, n.month, n.estado,
                       n."totalBruto", n."totalVariables",
                       json_agg(json_build_object(
                           'concepto', l."conceptoNombre",
                           'cantidad', l.cantidad,
                           'rate',     l.rate,
                           'importe',  l.importe
                       ) ORDER BY l.orden) AS lineas
                FROM "NominaMes" n
                LEFT JOIN "NominaLinea" l ON l."nominaId" = n.id
                WHERE n."empleadoId" = %s AND n.year = %s AND n.month = %s
                GROUP BY n.id
            """, (empleado_id, anio, mes))
            row = cur.fetchone()
            return dict(row) if row else None


# ─────────────────────────────────────────────
# Camiones / Flota
# ─────────────────────────────────────────────

def get_camiones(solo_activos: bool = True) -> list[dict]:
    """Devuelve el estado de la flota de camiones."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            filtro = "WHERE activo = TRUE" if solo_activos else ""
            cur.execute(f"""
                SELECT id, matricula, modelo, marca, "kmActual",
                       "itvVencimiento", "seguroVencimiento",
                       "tacografoVencimiento", "adrVencimiento", activo
                FROM "Camion"
                {filtro}
                ORDER BY matricula
            """)
            return [dict(r) for r in cur.fetchall()]


# ─────────────────────────────────────────────
# Mantenimientos / Reparaciones
# ─────────────────────────────────────────────

def get_mantenimientos(matricula: str = None, limite: int = 500) -> list[dict]:
    """
    Devuelve los mantenimientos realizados agrupables por matrícula.
    Deduplica registros con misma fecha, camión y descripción.
    """
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cond = 'AND c.matricula ILIKE %s' if matricula else ''
            params = [f'%{matricula}%'] if matricula else []
            cur.execute(f"""
                SELECT DISTINCT ON (c.matricula, m.fecha, m.descripcion)
                    c.matricula,
                    m.fecha,
                    m.taller,
                    m."kmEnEseMomento"   AS km,
                    m.descripcion,
                    m."piezasCambiadas",
                    m.costo,
                    m.tipo,
                    m."proximoKmPrevisto"
                FROM "MantenimientoRealizado" m
                JOIN "Camion" c ON c.id = m."camionId"
                WHERE TRUE {cond}
                ORDER BY c.matricula, m.fecha DESC, m.descripcion, m.id DESC
                LIMIT %s
            """, params + [limite])
            return [dict(r) for r in cur.fetchall()]


def get_uso_camion(matricula: str = None, fecha_desde: str = None,
                   fecha_hasta: str = None, limite: int = 100) -> list[dict]:
    """
    Devuelve registros de uso de camión con km iniciales, finales y recorridos.
    Permite filtrar por matrícula y rango de fechas (formato YYYY-MM-DD).
    """
    with _get_conn() as conn:
        with conn.cursor() as cur:
            conditions = ["TRUE"]
            params = []

            if matricula:
                conditions.append('c.matricula ILIKE %s')
                params.append(f'%{matricula}%')
            if fecha_desde:
                conditions.append('u."horaInicio" >= %s::timestamp')
                params.append(fecha_desde)
            if fecha_hasta:
                conditions.append('u."horaInicio" < (%s::date + 1)::timestamp')
                params.append(fecha_hasta)

            where = " AND ".join(conditions)
            cur.execute(f"""
                SELECT c.matricula,
                       e.nombre || ' ' || COALESCE(e.apellidos,'') AS conductor,
                       u."horaInicio"::date AS fecha,
                       u."kmInicial",
                       u."kmFinal",
                       u."kmRecorridos"
                FROM "UsoCamion" u
                JOIN "Camion" c ON c.id = u."camionId"
                JOIN "JornadaLaboral" j ON j.id = u."jornadaId"
                JOIN "Empleado" e ON e.id = j."empleadoId"
                WHERE {where}
                ORDER BY u."horaInicio" DESC
                LIMIT %s
            """, params + [limite])
            return [dict(r) for r in cur.fetchall()]


def get_tareas_abiertas(empleado_id: int = None) -> list[dict]:
    """Devuelve tareas en estado pendiente o en curso."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cond = 'AND t."asignadoAId" = %s' if empleado_id else ""
            params = [empleado_id] if empleado_id else []
            cur.execute(f"""
                SELECT t.id, t.titulo, t.tipo, t.estado, t.prioridad,
                       t."fechaLimite", t.matricula,
                       e.nombre || ' ' || COALESCE(e.apellidos,'') AS asignado
                FROM "Tarea" t
                LEFT JOIN "Empleado" e ON e.id = t."asignadoAId"
                WHERE t.estado NOT IN ('COMPLETADA','CANCELADA')
                {cond}
                ORDER BY t.prioridad, t."fechaLimite" NULLS LAST
                LIMIT 30
            """, params)
            return [dict(r) for r in cur.fetchall()]


# ─────────────────────────────────────────────
# Consulta genérica (solo SELECT) — con timeout + multi-tenant
# ─────────────────────────────────────────────

_DEFAULT_SQL_TIMEOUT_MS = 15000  # 15 segundos


def _get_tenant_conn(database_url: str):
    """Abre conexión a la BD de un tenant específico."""
    clean_url = _clean_db_url(database_url)
    conn = psycopg2.connect(
        clean_url,
        cursor_factory=psycopg2.extras.RealDictCursor,
        sslmode="require"
    )
    conn.set_session(readonly=True, autocommit=True)
    return conn


def run_safe_query(sql: str, timeout_ms: int = None, database_url: str = None) -> list[dict]:
    """
    Ejecuta una consulta SQL arbitraria de solo lectura.
    - Rechaza cualquier sentencia que no empiece por SELECT.
    - Aplica timeout explícito vía statement_timeout de PostgreSQL.
    - Si database_url se proporciona, conecta a la BD del tenant.
    """
    sql_stripped = sql.strip().upper()
    if not sql_stripped.startswith("SELECT"):
        raise ValueError("Solo se permiten consultas SELECT.")

    timeout = timeout_ms or _DEFAULT_SQL_TIMEOUT_MS

    # Elegir conexión: BD del tenant o BD global
    conn_func = lambda: _get_tenant_conn(database_url) if database_url else _get_conn()

    with conn_func() as conn:
        with conn.cursor() as cur:
            # Timeout explícito a nivel de la sesión
            cur.execute(f"SET LOCAL statement_timeout = '{timeout}'")
            try:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]
            except psycopg2.extensions.QueryCanceledError:
                raise TimeoutError(
                    f"Consulta SQL cancelada: excedio el timeout de {timeout}ms. "
                    f"Query: {sql[:200]}"
                )
