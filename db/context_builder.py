"""
db/context_builder.py — Convierte datos de la BD en texto legible para el LLM
"""
from datetime import datetime


def _fmt_fecha(val) -> str:
    if val is None:
        return "–"
    if isinstance(val, str):
        return val[:10]
    return val.strftime("%d/%m/%Y")


def _fmt_horas(val) -> str:
    if val is None:
        return "–"
    return f"{float(val):.1f} h"


# ─────────────────────────────────────────────

def empleados_a_texto(empleados: list[dict]) -> str:
    if not empleados:
        return "No se encontraron empleados."
    lineas = ["**Empleados activos:**\n"]
    for e in empleados:
        nombre = f"{e.get('nombre','')} {e.get('apellidos') or ''}".strip()
        rol = e.get("rol", "–")
        puesto = e.get("puestoTrabajo") or "–"
        vacaciones = e.get("diasVacaciones", 30)
        extras = e.get("diasExtras", 0)
        horas_extra = e.get("horasExtra", 0)
        entrada = e.get("horaEntradaPrevista") or "–"
        salida = e.get("horaSalidaPrevista") or "–"
        lineas.append(
            f"- **{nombre}** | Rol: {rol} | Puesto: {puesto} | "
            f"Horario: {entrada}–{salida} | "
            f"Vacaciones: {vacaciones} días, extras: {extras} días, horas extra acum.: {horas_extra}"
        )
    return "\n".join(lineas)


def jornadas_a_texto(resumen: dict, jornadas: list[dict],
                     nombre_empleado: str, mes: int, anio: int) -> str:
    meses = ["", "enero", "febrero", "marzo", "abril", "mayo", "junio",
             "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
    nombre_mes = meses[mes] if 1 <= mes <= 12 else str(mes)

    dias = resumen.get("dias_trabajados", 0)
    horas = float(resumen.get("horas_totales", 0))
    incidencias = resumen.get("incidencias", 0)

    lineas = [
        f"**Jornadas de {nombre_empleado} — {nombre_mes} {anio}:**",
        f"- Días trabajados: {dias}",
        f"- Horas totales: {horas:.1f} h",
        f"- Incidencias: {incidencias}",
        ""
    ]

    for j in jornadas:
        fecha = _fmt_fecha(j.get("fecha"))
        entrada = j.get("horaEntrada", "–")
        if hasattr(entrada, "strftime"):
            entrada = entrada.strftime("%H:%M")
        salida = j.get("horaSalida")
        if salida and hasattr(salida, "strftime"):
            salida = salida.strftime("%H:%M")
        elif salida:
            salida = str(salida)[:5]
        else:
            salida = "Sin cerrar"

        total = _fmt_horas(j.get("totalHoras"))
        estado = j.get("estado", "–")
        obs = j.get("observaciones") or ""
        linea = f"  · {fecha}: {entrada} → {salida} ({total}) [{estado}]"
        if obs:
            linea += f" — {obs}"
        lineas.append(linea)

    return "\n".join(lineas)


def ausencias_a_texto(ausencias: list[dict]) -> str:
    if not ausencias:
        return "No se encontraron ausencias con los filtros indicados."
    lineas = ["**Ausencias:**\n"]
    for a in ausencias:
        nombre = f"{a.get('nombre','')} {a.get('apellidos') or ''}".strip()
        tipo = a.get("tipo", "–")
        inicio = _fmt_fecha(a.get("fechaInicio"))
        fin = _fmt_fecha(a.get("fechaFin"))
        estado = a.get("estado", "–")
        horas = a.get("horas")
        dur = f"{horas} h" if horas else "día completo"
        obs = a.get("observaciones") or ""
        linea = f"- **{nombre}**: {tipo} del {inicio} al {fin} ({dur}) — Estado: {estado}"
        if obs:
            linea += f" | {obs}"
        lineas.append(linea)
    return "\n".join(lineas)


def nomina_a_texto(nomina: dict, nombre_empleado: str) -> str:
    if not nomina:
        return f"No se encontró nómina registrada para {nombre_empleado} en ese período."

    mes_str = f"{nomina.get('month', '?')}/{nomina.get('year', '?')}"
    bruto = nomina.get("totalBruto", 0)
    variables = nomina.get("totalVariables", 0)
    estado = nomina.get("estado", "–")

    lineas = [
        f"**Nómina de {nombre_empleado} — {mes_str}:**",
        f"- Estado: {estado}",
        f"- Total bruto: {bruto:.2f} €",
        f"- Variables: {variables:.2f} €",
        ""
    ]
    for ln in (nomina.get("lineas") or []):
        if isinstance(ln, dict):
            lineas.append(
                f"  · {ln.get('concepto','?')}: {ln.get('cantidad',0)} × {ln.get('rate',0):.4f} = {ln.get('importe',0):.2f} €"
            )
    return "\n".join(lineas)


def camiones_a_texto(camiones: list[dict]) -> str:
    if not camiones:
        return "No se encontraron camiones."
    hoy = datetime.today().date()
    lineas = ["**Flota de camiones:**\n"]
    for c in camiones:
        mat = c.get("matricula", "–")
        modelo = f"{c.get('marca','?')} {c.get('modelo','?')}".strip()
        km = c.get("kmActual", 0)

        def venc(dv):
            if not dv:
                return "–"
            d = dv.date() if hasattr(dv, "date") else dv
            dias = (d - hoy).days
            emoji = "🔴" if dias < 30 else ("🟡" if dias < 90 else "🟢")
            return f"{d.strftime('%d/%m/%Y')} ({emoji} {dias} días)"

        lineas.append(
            f"- **{mat}** ({modelo}) | KM: {km:,} | "
            f"ITV: {venc(c.get('itvVencimiento'))} | "
            f"Seguro: {venc(c.get('seguroVencimiento'))} | "
            f"Tacógrafo: {venc(c.get('tacografoVencimiento'))}"
        )
    return "\n".join(lineas)


def tareas_a_texto(tareas: list[dict]) -> str:
    if not tareas:
        return "No hay tareas abiertas que mostrar."
    lineas = ["**Tareas abiertas:**\n"]
    for t in tareas:
        titulo = t.get("titulo", "–")
        estado = t.get("estado", "–")
        prio = t.get("prioridad", "–")
        asignado = t.get("asignado") or "Sin asignar"
        limit = _fmt_fecha(t.get("fechaLimite"))
        mat = t.get("matricula") or ""
        extra = f" | Camión: {mat}" if mat else ""
        lineas.append(
            f"- [{prio}] **{titulo}** → {estado} | Asignado: {asignado} | Límite: {limit}{extra}"
        )
    return "\n".join(lineas)


def mantenimientos_a_texto(registros: list[dict]) -> str:
    """Formatea los mantenimientos agrupados por matrícula."""
    if not registros:
        return "No se encontraron registros de mantenimiento."

    # Agrupar por matrícula
    from collections import defaultdict
    por_matricula = defaultdict(list)
    for r in registros:
        por_matricula[r.get("matricula", "–")].append(r)

    lineas = ["**Mantenimientos realizados por vehículo:**\n"]
    for mat, reps in sorted(por_matricula.items()):
        total_coste = sum(float(r.get("costo") or 0) for r in reps)
        lineas.append(f"\n### 🚛 Matrícula: {mat}  (Total: {total_coste:,.2f} €)")
        for r in reps:
            fecha = _fmt_fecha(r.get("fecha"))
            taller = r.get("taller") or "–"
            km = r.get("km") or 0
            desc = r.get("descripcion") or "–"
            piezas = r.get("piezasCambiadas") or ""
            costo = float(r.get("costo") or 0)
            prox = r.get("proximoKmPrevisto")
            linea = (
                f"  · **{fecha}** | Taller: {taller} | KM: {km:,} | "
                f"Coste: {costo:,.2f} € | {desc}"
            )
            if piezas:
                linea += f" | Piezas: {piezas}"
            if prox:
                linea += f" | Próx. mantenimiento: {prox:,} km"
            lineas.append(linea)
    return "\n".join(lineas)


def uso_camion_a_texto(registros: list[dict]) -> str:
    """Formatea los registros de uso de camión con km diarios."""
    if not registros:
        return "No se encontraron registros de uso de camión en el período indicado."

    from collections import defaultdict
    por_matricula = defaultdict(list)
    for r in registros:
        por_matricula[r.get("matricula", "–")].append(r)

    lineas = ["**Kilómetros recorridos por camión:**\n"]
    for mat, usos in sorted(por_matricula.items()):
        total_km = sum(r.get("kmRecorridos") or 0 for r in usos)
        lineas.append(f"\n### 🚛 {mat}  (Total: {total_km:,} km en {len(usos)} jornadas)")
        for u in usos:
            fecha = _fmt_fecha(u.get("fecha"))
            conductor = u.get("conductor") or "–"
            km_ini = u.get("kmInicial") or 0
            km_fin = u.get("kmFinal") or 0
            km_rec = u.get("kmRecorridos") or 0
            lineas.append(
                f"  · {fecha}: {conductor} | {km_ini:,} → {km_fin:,} km ({km_rec:,} km recorridos)"
            )
    return "\n".join(lineas)
