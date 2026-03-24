"""
ingestion/load_pdfs.py — Carga y chunking de PDFs con OCR estructurado
Modo híbrido:
  1. pdfplumber para PDFs digitales
  2. Gemini Vision OCR para páginas escaneadas → extrae JSON estructurado
  3. Genera resumen automático de cada documento
"""
import base64
import json
import requests
import pdfplumber
import fitz  # PyMuPDF
from pathlib import Path
from langchain_text_splitters import RecursiveCharacterTextSplitter
from config import CHUNK_SIZE, CHUNK_OVERLAP, GEMINI_API_KEY

OCR_MIN_CHARS = 100

GEMINI_VISION_URL = (
    "https://generativelanguage.googleapis.com/v1/models/"
    "gemini-2.0-flash:generateContent?key={key}"
)

GEMINI_TEXT_URL = (
    "https://generativelanguage.googleapis.com/v1/models/"
    "gemini-2.0-flash:generateContent?key={key}"
)

# ─── Prompt OCR estructurado ───
OCR_PROMPT = """Analiza esta imagen de un documento (factura, albarán, etc.) y extrae TODA la información.

Devuelve DOS secciones separadas por la línea "---METADATA---":

PRIMERA SECCIÓN: El texto completo y fiel del documento, tal como aparece.

---METADATA---

SEGUNDA SECCIÓN: Un JSON con los campos extraídos (si no encuentras un campo, pon null):
{
  "tipo_documento": "factura|albaran|presupuesto|otro",
  "numero_factura": "...",
  "fecha": "DD/MM/YYYY",
  "matricula": "...",
  "taller": "nombre del proveedor/taller",
  "descripcion": "resumen breve de los trabajos/productos",
  "piezas": ["pieza1", "pieza2"],
  "importe_base": 0.00,
  "iva": 0.00,
  "total": 0.00,
  "cliente": "nombre del cliente",
  "proveedor": "nombre del proveedor"
}"""

SUMMARY_PROMPT = """Resume este documento en 2-3 líneas en español.
Si es una factura, incluye: proveedor, cliente, matrícula (si hay), importe total y descripción breve.
Documento:
{text}"""


def _call_gemini_vision(img_bytes: bytes) -> tuple[str, dict]:
    """
    Envía imagen a Gemini Vision y devuelve (texto_extraido, metadatos_json).
    """
    img_b64 = base64.b64encode(img_bytes).decode("utf-8")
    payload = {
        "contents": [{"parts": [
            {"inline_data": {"mime_type": "image/png", "data": img_b64}},
            {"text": OCR_PROMPT}
        ]}],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 4096}
    }
    try:
        resp = requests.post(
            GEMINI_VISION_URL.format(key=GEMINI_API_KEY),
            json=payload, timeout=60
        )
        if resp.status_code != 200:
            return "", {}
        raw = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()

        # Separar texto y metadatos
        if "---METADATA---" in raw:
            parts = raw.split("---METADATA---", 1)
            text = parts[0].strip()
            json_str = parts[1].strip()
            # Extraer JSON (puede tener texto extra)
            start = json_str.find("{")
            end = json_str.rfind("}") + 1
            if start >= 0 and end > start:
                metadata = json.loads(json_str[start:end])
            else:
                metadata = {}
        else:
            text = raw
            metadata = {}
        return text, metadata
    except Exception as e:
        print(f"  ⚠️ Gemini Vision error: {e}")
        return "", {}


def _generate_summary(text: str) -> str:
    """Genera resumen breve del documento con Gemini."""
    if not text or len(text) < 50:
        return ""
    # Limitar texto a 3000 chars para el resumen
    truncated = text[:3000]
    payload = {
        "contents": [{"parts": [{"text": SUMMARY_PROMPT.format(text=truncated)}]}],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 300}
    }
    try:
        resp = requests.post(
            GEMINI_TEXT_URL.format(key=GEMINI_API_KEY),
            json=payload, timeout=30
        )
        if resp.status_code == 200:
            return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        pass
    return ""


def _render_page_to_image(pdf_path: str, page_index: int) -> bytes:
    """Renderiza una página del PDF a PNG con PyMuPDF a 200 DPI."""
    doc = fitz.open(pdf_path)
    page = doc[page_index]
    mat = fitz.Matrix(200 / 72, 200 / 72)
    pix = page.get_pixmap(matrix=mat)
    img_bytes = pix.tobytes("png")
    doc.close()
    return img_bytes


def extract_text_from_pdf(pdf_path: str) -> list[dict]:
    """
    Extrae texto página a página.
    Para páginas escaneadas usa OCR con Gemini Vision y extrae metadatos.
    """
    pages = []
    pdf_path_str = str(pdf_path)
    source_name = Path(pdf_path_str).name

    with pdfplumber.open(pdf_path_str) as pdf:
        for i, page in enumerate(pdf.pages):
            text = (page.extract_text() or "").strip()

            if len(text) >= OCR_MIN_CHARS:
                # PDF digital — texto suficiente
                pages.append({
                    "text": text,
                    "page": i + 1,
                    "source": source_name,
                    "ocr": False,
                    "metadata": {}
                })
            else:
                # Página escaneada → OCR con Gemini Vision
                print(f"  🔍 OCR página {i+1} de '{source_name}'...")
                img = _render_page_to_image(pdf_path_str, i)
                ocr_text, metadata = _call_gemini_vision(img)
                if ocr_text and len(ocr_text) > 20:
                    pages.append({
                        "text": ocr_text,
                        "page": i + 1,
                        "source": source_name,
                        "ocr": True,
                        "metadata": metadata
                    })

    return pages


def split_into_chunks(pages: list[dict]) -> list[dict]:
    """
    Chunking inteligente:
    - Facturas (1-2 páginas): cada página = 1 chunk completo
    - Documentos largos: RecursiveCharacterTextSplitter
    Conserva metadatos estructurados.
    """
    is_invoice = len(pages) <= 3  # Facturas suelen tener 1-3 páginas

    chunks = []
    if is_invoice:
        # Una factura = 1 chunk por página (no trocear)
        for page_data in pages:
            chunks.append({
                "text": page_data["text"],
                "page": page_data["page"],
                "source": page_data["source"],
                "ocr": page_data.get("ocr", False),
                "metadata": page_data.get("metadata", {}),
                "chunk_id": f"{page_data['source']}_p{page_data['page']}_c0"
            })
    else:
        # Documentos largos: split normal
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=CHUNK_SIZE,
            chunk_overlap=CHUNK_OVERLAP,
            separators=["\n\n", "\n", ". ", " ", ""]
        )
        for page_data in pages:
            sub_chunks = splitter.split_text(page_data["text"])
            for j, chunk_text in enumerate(sub_chunks):
                chunks.append({
                    "text": chunk_text,
                    "page": page_data["page"],
                    "source": page_data["source"],
                    "ocr": page_data.get("ocr", False),
                    "metadata": page_data.get("metadata", {}),
                    "chunk_id": f"{page_data['source']}_p{page_data['page']}_c{j}"
                })

    return chunks


def load_pdf(pdf_path: str) -> list[dict]:
    """Pipeline completo: PDF → chunks con OCR + metadatos + resumen."""
    pages = extract_text_from_pdf(pdf_path)
    n_ocr = sum(1 for p in pages if p.get("ocr"))
    chunks = split_into_chunks(pages)

    # Generar resumen del documento completo
    full_text = "\n\n".join(p["text"] for p in pages)
    summary = _generate_summary(full_text)
    if summary:
        # Agregar chunk especial de resumen
        metadata_combined = {}
        for p in pages:
            if p.get("metadata"):
                metadata_combined.update(p["metadata"])

        chunks.append({
            "text": f"[RESUMEN] {summary}",
            "page": 0,
            "source": Path(pdf_path).name,
            "ocr": False,
            "metadata": metadata_combined,
            "chunk_id": f"{Path(pdf_path).name}_summary",
            "is_summary": True
        })

    modo = f"({n_ocr} pág. OCR)" if n_ocr else "(digital)"
    print(f"📄 '{Path(pdf_path).name}': {len(pages)} pág. → {len(chunks)} chunks {modo}")
    return chunks
