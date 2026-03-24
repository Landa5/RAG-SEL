"""
app/main.py — Interfaz de chat RAG con Streamlit
"""
import sys
import os
import json
import hashlib
import tempfile
from pathlib import Path
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from ingestion.load_pdfs import load_pdf
from ingestion.index_documents import index_chunks
from retrieval.search import search, list_indexed_documents, get_all_chunks_by_source
from llm.generate import generate_answer
from llm.router import classify_query, extract_mes_anio, extract_nombre_empleado
from db import connector as db
from db import context_builder as cb

# ─── Configuración de página ───
st.set_page_config(
    page_title="RAG-SEL · Chat con tus documentos",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── Estilos ───
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    .stApp {
        background: #0f172a;
    }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background: #1e293b !important;
        border-right: 2px solid #334155 !important;
    }
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] div,
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] .stMarkdown,
    [data-testid="stSidebar"] .stMarkdown * {
        color: #f1f5f9 !important;
    }
    [data-testid="stSidebar"] em { color: #94a3b8 !important; }
    hr { border-color: #334155 !important; }

    /* ── File uploader VISIBLE ── */
    [data-testid="stFileUploader"] {
        background: #0f172a !important;
        border: 2px dashed #6366f1 !important;
        border-radius: 12px !important;
        padding: 0.5rem !important;
    }
    [data-testid="stFileUploader"] section {
        background: #0f172a !important;
        border: none !important;
    }
    [data-testid="stFileUploader"] span,
    [data-testid="stFileUploader"] p,
    [data-testid="stFileUploader"] small {
        color: #94a3b8 !important;
    }
    [data-testid="stFileUploader"] button {
        background: #1e293b !important;
        color: #e2e8f0 !important;
        border: 1px solid #475569 !important;
        border-radius: 8px !important;
    }

    /* ── Text input (ruta carpeta) ── */
    [data-testid="stTextInput"] input {
        background: #0f172a !important;
        color: #f1f5f9 !important;
        border: 1.5px solid #475569 !important;
        border-radius: 10px !important;
    }
    [data-testid="stTextInput"] input::placeholder { color: #475569 !important; }
    [data-testid="stTextInput"] label { color: #94a3b8 !important; }

    /* ── Tabs ── */
    [data-testid="stTabs"] [role="tab"] {
        color: #94a3b8 !important;
        font-weight: 500;
    }
    [data-testid="stTabs"] [role="tab"][aria-selected="true"] {
        color: #818cf8 !important;
        border-bottom: 2px solid #818cf8 !important;
    }
    [data-baseweb="tab-list"] {
        background: transparent !important;
        border-bottom: 1px solid #334155 !important;
    }

    /* ── Título principal ── */
    .main-title {
        font-size: 2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #818cf8 0%, #a78bfa 50%, #f472b6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.2rem;
    }
    .main-subtitle {
        color: #94a3b8;
        font-size: 0.95rem;
        margin-bottom: 1.5rem;
    }

    /* ── Chat ── */
    [data-testid="stChatMessage"] {
        background: #1e293b !important;
        border: 1px solid #334155 !important;
        border-radius: 14px !important;
        margin: 6px 0 !important;
    }
    [data-testid="stChatMessage"] p,
    [data-testid="stChatMessage"] li,
    [data-testid="stChatMessage"] span {
        color: #e2e8f0 !important;
    }

    /* ── Fuentes ── */
    .source-card {
        background: #172554;
        border: 1px solid #3b82f6;
        border-radius: 10px;
        padding: 0.55rem 0.9rem;
        margin: 0.3rem 0;
        font-size: 0.83rem;
        color: #bfdbfe !important;
    }
    .source-card b { color: #93c5fd !important; }

    /* ── Botones ── */
    .stButton > button {
        background: linear-gradient(135deg, #6366f1, #8b5cf6) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
        font-size: 0.9rem !important;
        transition: all 0.25s ease !important;
        width: 100% !important;
    }
    .stButton > button:hover {
        box-shadow: 0 6px 20px rgba(99,102,241,0.45) !important;
        transform: translateY(-2px) !important;
    }
    .stButton > button:disabled {
        background: #1e293b !important;
        color: #475569 !important;
        border: 1px solid #334155 !important;
    }

    /* ── Chat input ── */
    [data-testid="stChatInput"] textarea {
        background: #1e293b !important;
        color: #f1f5f9 !important;
        border: 1px solid #334155 !important;
        border-radius: 12px !important;
    }
    [data-testid="stChatInput"] textarea::placeholder { color: #64748b !important; }

    /* ── Tags docs ── */
    .doc-tag {
        display: inline-block;
        background: #172554;
        border: 1px solid #3b82f6;
        border-radius: 20px;
        padding: 4px 12px;
        font-size: 0.8rem;
        color: #93c5fd !important;
        margin: 3px;
    }

    /* ── Expander ── */
    [data-testid="stExpander"] {
        background: #1e293b !important;
        border: 1px solid #334155 !important;
        border-radius: 10px !important;
    }
    [data-testid="stExpander"] summary { color: #94a3b8 !important; }

    /* ── Checkbox & Radio ── */
    .stRadio label, .stCheckbox label { color: #e2e8f0 !important; }

    /* ── Info / success / error ── */
    [data-testid="stAlert"] { border-radius: 10px !important; }

    /* ── Ocultar solo lo innecesario, NO el toggle del sidebar ── */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }

    /* Ocultar el texto "Made with Streamlit" y el icono hamburguesa del header */
    header [data-testid="stToolbar"] { display: none !important; }
    header { background: transparent !important; }

    /* ── Botón de COLAPSAR sidebar (cuando está abierto) ── */
    [data-testid="stSidebarCollapseButton"] button {
        background: #4f46e5 !important;
        border-radius: 8px !important;
        color: white !important;
    }
    [data-testid="stSidebarCollapseButton"] svg { fill: white !important; }

    /* ── Botón de EXPANDIR sidebar (cuando está cerrado) ── */
    [data-testid="collapsedControl"] {
        background: #4f46e5 !important;
        border-radius: 0 10px 10px 0 !important;
        width: 28px !important;
        visibility: visible !important;
        opacity: 1 !important;
    }
    [data-testid="collapsedControl"] svg { fill: white !important; }

</style>
""", unsafe_allow_html=True)

# ─── Estado de sesión ───
if "messages" not in st.session_state:
    st.session_state.messages = []
if "llm_provider" not in st.session_state:
    st.session_state.llm_provider = "gemini"
if "query_mode" not in st.session_state:
    st.session_state.query_mode = "auto"
if "response_cache" not in st.session_state:
    st.session_state.response_cache = {}
if "feedback" not in st.session_state:
    st.session_state.feedback = []


def process_and_index(chunks_list: list, name: str, force: bool) -> int:
    """Indexa una lista de chunks ya cargados. Devuelve nº indexados."""
    indexed = index_chunks(chunks_list, force=force)
    return indexed


# ─── SIDEBAR ───
with st.sidebar:
    st.markdown("## 📚 RAG-SEL")
    st.markdown("*Chat con tus documentos PDF*")
    st.divider()

    # Selector de modelo
    st.markdown("### 🤖 Modelo de IA")
    provider = st.radio(
        "proveedor",
        options=["gemini", "openai"],
        format_func=lambda x: "✨ Gemini 2.0 Flash" if x == "gemini" else "🟢 GPT-4o Mini",
        horizontal=False,
        label_visibility="collapsed"
    )
    st.session_state.llm_provider = provider
    st.divider()

    # Selector de modo
    st.markdown("### 🔍 Fuente de consulta")
    mode_options = {
        "auto":   "🔄 Automático",
        "docs":   "📄 Solo documentos",
        "db":     "🗃️ Solo base de datos",
        "hybrid": "🧩 Híbrido (ambas)",
    }
    selected_mode = st.radio(
        "modo",
        options=list(mode_options.keys()),
        format_func=lambda x: mode_options[x],
        horizontal=False,
        label_visibility="collapsed"
    )
    st.session_state.query_mode = selected_mode
    st.divider()

    # Filtros de fecha
    st.markdown("### 📅 Filtro por fecha")
    use_date_filter = st.checkbox("Activar filtro de fecha", value=False)
    if use_date_filter:
        col_from, col_to = st.columns(2)
        with col_from:
            date_from = st.date_input("Desde", value=date(2023, 1, 1), key="date_from")
        with col_to:
            date_to = st.date_input("Hasta", value=date.today(), key="date_to")
    else:
        date_from = None
        date_to = None
    st.divider()

    st.markdown("### 📄 Indexar documentos")
    tab_files, tab_folder = st.tabs(["📎 Archivos", "📁 Carpeta"])

    force_reindex = st.checkbox("🔄 Forzar reindexado", value=False)

    # ── Tab 1: Subida de archivos individuales ──
    with tab_files:
        uploaded_files = st.file_uploader(
            "Sube uno o varios PDFs",
            type=["pdf"],
            accept_multiple_files=True,
            label_visibility="collapsed"
        )

        if st.button("⚡ Indexar archivos", disabled=not uploaded_files, key="btn_files"):
            total = 0
            bar = st.progress(0)
            for i, f in enumerate(uploaded_files):
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                    tmp.write(f.read())
                    tmp_path = tmp.name
                with st.spinner(f"Procesando '{f.name}'..."):
                    try:
                        chunks = load_pdf(tmp_path)
                        for c in chunks:
                            c["source"] = f.name
                        total += index_chunks(chunks, force=force_reindex)
                    except Exception as e:
                        st.error(f"❌ {f.name}: {e}")
                    finally:
                        os.unlink(tmp_path)
                bar.progress((i + 1) / len(uploaded_files))
            if total > 0:
                st.success(f"✅ {total} fragmentos indexados")
                st.balloons()
            else:
                st.info("ℹ️ Ya estaban indexados")

    # ── Tab 2: Carpeta local ──
    with tab_folder:
        folder_path = st.text_input(
            "Ruta de la carpeta",
            placeholder="C:\\Users\\...\\mis_documentos",
            label_visibility="collapsed"
        )

        # Vista previa de PDFs en la carpeta
        if folder_path:
            p = Path(folder_path)
            if p.exists() and p.is_dir():
                pdfs = list(p.glob("**/*.pdf"))
                if pdfs:
                    st.markdown(f"<span style='color:#94a3b8;font-size:0.82rem;'>📋 {len(pdfs)} PDF(s) encontrados</span>", unsafe_allow_html=True)
                    with st.expander("Ver archivos", expanded=False):
                        for pdf in pdfs[:20]:
                            st.markdown(f"<span style='color:#93c5fd;font-size:0.78rem;'>• {pdf.name}</span>", unsafe_allow_html=True)
                        if len(pdfs) > 20:
                            st.markdown(f"<span style='color:#64748b;font-size:0.76rem;'>... y {len(pdfs)-20} más</span>", unsafe_allow_html=True)
                else:
                    st.warning("⚠️ No se encontraron PDFs en esa carpeta")
            elif folder_path:
                st.error("❌ La ruta no existe o no es una carpeta")

        btn_folder_disabled = not (folder_path and Path(folder_path).exists() and list(Path(folder_path).glob("**/*.pdf")))
        if st.button("⚡ Indexar carpeta", disabled=btn_folder_disabled, key="btn_folder"):
            p = Path(folder_path)
            pdfs = list(p.glob("**/*.pdf"))
            total = 0
            bar = st.progress(0)
            for i, pdf_path in enumerate(pdfs):
                with st.spinner(f"Procesando '{pdf_path.name}' ({i+1}/{len(pdfs)})..."):
                    try:
                        chunks = load_pdf(str(pdf_path))
                        total += index_chunks(chunks, force=force_reindex)
                    except Exception as e:
                        st.error(f"❌ {pdf_path.name}: {e}")
                bar.progress((i + 1) / len(pdfs))
            if total > 0:
                st.success(f"✅ {total} fragmentos indexados desde {len(pdfs)} archivo(s)")
                st.balloons()
            else:
                st.info("ℹ️ Ya estaban todos indexados")

    st.divider()

    # Documentos indexados
    st.markdown("### 🗃️ Documentos indexados")
    docs = list_indexed_documents()
    if docs:
        for doc in docs:
            st.markdown(f'<span class="doc-tag">📄 {doc}</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span style="color:#64748b;font-size:0.85rem;">Ningún documento indexado aún</span>', unsafe_allow_html=True)

    st.divider()

    if st.button("🗑️ Limpiar conversación"):
        st.session_state.messages = []
        st.session_state.response_cache = {}
        st.rerun()


# ─── ÁREA PRINCIPAL DE CHAT ───
st.markdown('<h1 class="main-title">💬 Chat con tus documentos</h1>', unsafe_allow_html=True)
st.markdown('<p class="main-subtitle">Haz preguntas sobre tus PDFs en lenguaje natural</p>', unsafe_allow_html=True)

if not st.session_state.messages:
    st.markdown("""
    <div style="text-align:center; padding: 4rem 2rem;">
        <div style="font-size:4rem;">📖</div>
        <div style="font-size:1.05rem; margin-top:1rem; color:#64748b;">
            Sube PDFs o apunta a una carpeta en el panel izquierdo y empieza a preguntar
        </div>
    </div>
    """, unsafe_allow_html=True)
else:
    for msg in st.session_state.messages:
        if msg["role"] == "user":
            with st.chat_message("user", avatar="🧑"):
                st.markdown(msg["content"])
        else:
            with st.chat_message("assistant", avatar="🤖"):
                st.markdown(msg["content"])
                if msg.get("sources"):
                    with st.expander(f"📎 {len(msg['sources'])} fuente(s) utilizadas", expanded=False):
                        for src in msg["sources"]:
                            score_pct = int(src.get("score", 0) * 100)
                            st.markdown(
                                f'<div class="source-card">📄 <b>{src["source"]}</b> · Página {src["page"]} · Relevancia: {score_pct}%</div>',
                                unsafe_allow_html=True
                            )

# ─── INPUT DEL CHAT ───


def _build_db_context_streamlit(question: str) -> str:
    """Consulta la BD y devuelve texto de contexto enriquecido."""
    contextos = []
    mes, anio = extract_mes_anio(question)
    nombre = extract_nombre_empleado(question)
    try:
        if nombre:
            empleados = db.buscar_empleado(nombre)
        else:
            empleados = db.get_empleados()

        if empleados:
            contextos.append(cb.empleados_a_texto(empleados))
            emp = empleados[0]
            emp_id = emp["id"]
            emp_nombre = f"{emp.get('nombre','')} {emp.get('apellidos') or ''}".strip()
            resumen = db.get_resumen_jornadas(emp_id, mes, anio)
            jornadas = db.get_jornadas_empleado(emp_id, mes, anio)
            if jornadas or resumen.get("dias_trabajados", 0):
                contextos.append(cb.jornadas_a_texto(resumen, jornadas, emp_nombre, mes, anio))
            ausencias = db.get_ausencias(empleado_id=emp_id, mes=mes, anio=anio)
            if ausencias:
                contextos.append(cb.ausencias_a_texto(ausencias))
            nomina = db.get_nomina(emp_id, mes, anio)
            contextos.append(cb.nomina_a_texto(nomina, emp_nombre))

        q_lower = question.lower()
        if any(w in q_lower for w in ["camión", "camiones", "flota", "matrícula", "itv",
                                       "seguro", "tacógrafo"]):
            camiones = db.get_camiones()
            if camiones:
                contextos.append(cb.camiones_a_texto(camiones))

        if any(w in q_lower for w in ["tarea", "tareas", "avería", "incidencia"]):
            emp_id_p = empleados[0]["id"] if empleados else None
            tareas = db.get_tareas_abiertas(emp_id_p)
            if tareas:
                contextos.append(cb.tareas_a_texto(tareas))

        if any(w in q_lower for w in [
            "taller", "reparación", "reparaciones", "mantenimiento", "mantenimientos",
            "km", "kilómetros", "coste", "coste reparación", "historial",
            "descripción de la reparación", "piezas", "agrupes"
        ]):
            # Extraer matrícula si la mencionan en la pregunta
            import re
            mat_match = re.search(r'\b[0-9]{4}[A-Z]{3}\b|\b[A-Z]{1,2}[0-9]{4}[A-Z]{1,2}\b', question, re.IGNORECASE)
            mat_filtro = mat_match.group() if mat_match else None
            mantenimientos = db.get_mantenimientos(matricula=mat_filtro)
            if mantenimientos:
                contextos.append(cb.mantenimientos_a_texto(mantenimientos))

    except Exception as e:
        st.warning(f"⚠️ Error al consultar BD: {e}")
    return "\n\n---\n\n".join(contextos)



_MODE_BADGE = {
    "db":     ("🗃️", "Base de datos",  "#0f4c75"),
    "docs":   ("📄", "Documentos",      "#1a3a1a"),
    "hybrid": ("🧩", "Híbrido",          "#3d1f5e"),
    "auto":   ("🔄", "Automático",       "#1e293b"),
}

if query := st.chat_input("Escribe tu pregunta aquí..."):
    st.session_state.messages.append({"role": "user", "content": query})
    with st.chat_message("user", avatar="🧑"):
        st.markdown(query)

    with st.chat_message("assistant", avatar="🤖"):
        # Detectar modo efectivo
        effective_mode = st.session_state.query_mode
        if effective_mode == "auto":
            effective_mode = classify_query(query)

        # Comprobar caché
        cache_key = hashlib.md5(f"{query}|{effective_mode}".encode()).hexdigest()
        cached = st.session_state.response_cache.get(cache_key)
        if cached:
            result = cached
            st.caption("⚡ Respuesta en caché")
        else:
            # Buscar en Qdrant si procede
            chunks = []
            if effective_mode in ("docs", "hybrid"):
                aggregate_keywords = [
                    "todos los documentos", "todos los pdf", "todos los archivos",
                    "agrupar", "agrupes", "agrupar todos", "resumen de todos",
                    "lista de todos", "listado", "compara",
                ]
                q_lower_agg = query.lower()
                is_aggregate = any(kw in q_lower_agg for kw in aggregate_keywords)

                with st.spinner("🔍 Buscando en documentos..."):
                    try:
                        if is_aggregate:
                            chunks = get_all_chunks_by_source(chunks_per_source=3)
                            st.info(f"📊 Modo agregación: cargados {len(chunks)} fragmentos de {len(set(c['source'] for c in chunks))} documentos")
                        else:
                            chunks = search(query)
                    except Exception:
                        chunks = []

            # Consultar BD si procede
            db_context = None
            if effective_mode in ("db", "hybrid"):
                with st.spinner("🗃️ Consultando base de datos..."):
                    db_context = _build_db_context_streamlit(query)

            with st.spinner("✍️ Generando respuesta..."):
                result = generate_answer(
                    query=query,
                    chunks=chunks,
                    chat_history=st.session_state.messages[:-1],
                    provider=st.session_state.llm_provider,
                    db_context=db_context,
                    source_mode=effective_mode
                )

        # Badge de fuente usada
        em, label, color = _MODE_BADGE.get(effective_mode, _MODE_BADGE["auto"])
        st.markdown(
            f'<span style="background:{color};color:#e2e8f0;padding:3px 10px;'
            f'border-radius:12px;font-size:0.78rem;">'
            f'{em} Fuente: {label}</span>',
            unsafe_allow_html=True
        )
        st.markdown(result["answer"])

        if result["sources"]:
            with st.expander(f"📎 {len(result['sources'])} fuente(s) de documentos", expanded=False):
                for src in result["sources"]:
                    score_pct = int(src.get("score", 0) * 100)
                    st.markdown(
                        f'<div class="source-card">📄 <b>{src["source"]}</b> · Página {src["page"]} · Relevancia: {score_pct}%</div>',
                        unsafe_allow_html=True
                    )

        # Botones de feedback
        msg_idx = len(st.session_state.messages)
        fb_col1, fb_col2, fb_col3 = st.columns([1, 1, 10])
        with fb_col1:
            if st.button("👍", key=f"fb_up_{msg_idx}"):
                st.session_state.feedback.append({
                    "query": query, "rating": "positive",
                    "mode": effective_mode, "ts": datetime.now().isoformat()
                })
                st.toast("✅ ¡Gracias por tu feedback!")
        with fb_col2:
            if st.button("👎", key=f"fb_down_{msg_idx}"):
                st.session_state.feedback.append({
                    "query": query, "rating": "negative",
                    "mode": effective_mode, "ts": datetime.now().isoformat()
                })
                st.toast("📝 Gracias, intentaremos mejorar.")

        # Guardar caché
        cache_key = hashlib.md5(f"{query}|{effective_mode}".encode()).hexdigest()
        st.session_state.response_cache[cache_key] = result

    st.session_state.messages.append({
        "role": "assistant",
        "content": result["answer"],
        "sources": result["sources"],
        "source_mode": effective_mode
    })
