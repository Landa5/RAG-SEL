"""
retrieval/search.py — Búsqueda semántica + híbrida + re-ranking V3.0 Multi-Tenant
  1. Query rewriting para mejorar búsqueda documental
  2. Top-K dinámico por pipeline
  3. Filtrado por metadatos
  4. Búsqueda semántica (vectores) con Qdrant
  5. Búsqueda por keywords en payload
  6. Fusión de resultados (RRF)
  7. Re-ranking con cross-encoder
  8. Aislamiento por tenant_id (OBLIGATORIO)
"""
import re
from collections import defaultdict
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, MatchText, Range
from ingestion.embeddings import embed_query
from ingestion.index_documents import get_qdrant_client
from config import COLLECTION_NAME, TOP_K
from retrieval.reranker import rerank


# ─────────────────────────────────────────────
# Query rewriting
# ─────────────────────────────────────────────

def rewrite_retrieval_query(query: str, pipeline_context: dict = None) -> str:
    """
    Reescribe la query del usuario para mejorar la búsqueda documental.
    Transforma preguntas conversacionales en queries documentales más precisas.
    """
    q = query.lower().strip()

    # Reescrituras por patrón
    rewrites = [
        (r"^qué dice el (convenio|documento|contrato) sobre (.+)", r"\2 \1"),
        (r"^según el (convenio|documento), (.+)", r"\2"),
        (r"^cuántos días de (.+) tengo", r"días \1 trabajador"),
        (r"^qué derechos tengo .* sobre (.+)", r"\1 derechos trabajador"),
        (r"^puedo (.+) según el convenio", r"\1 convenio colectivo"),
        (r"^cuáles son las condiciones de (.+)", r"condiciones \1"),
        (r"^qué normativa aplica a (.+)", r"normativa \1"),
    ]

    for pattern, replacement in rewrites:
        match = re.match(pattern, q, re.IGNORECASE)
        if match:
            return re.sub(pattern, replacement, q, flags=re.IGNORECASE)

    # Quitar palabras interrogativas que no ayudan al retrieval
    noise_words = {
        "qué", "cuál", "cómo", "cuándo", "cuánto", "dime", "dame",
        "explica", "muéstrame", "necesito", "quiero", "puedes",
    }
    words = q.split()
    cleaned = [w for w in words if w not in noise_words]
    return " ".join(cleaned) if cleaned else query


# ─────────────────────────────────────────────
# Top-K dinámico
# ─────────────────────────────────────────────

def _dynamic_top_k(pipeline_id: str = None, complexity: int = 3) -> int:
    """
    Calcula el top-k óptimo según el pipeline y la complejidad.
    """
    base_k = {
        "direct_chat": 5,
        "doc_retrieval": 15,
        "agentic_sql": 5,
        "agentic_sql_rag": 10,
        "predictive_insight": 5,
        "predictive_forecast": 5,
    }
    k = base_k.get(pipeline_id, TOP_K)

    # Ajustar por complejidad
    if complexity >= 7:
        k = min(k + 5, 25)
    elif complexity <= 2:
        k = max(k - 3, 3)

    return k


# ─────────────────────────────────────────────
# Metadata filtering
# ─────────────────────────────────────────────

def _build_metadata_filter(query: str, filter_source: str = None,
                           tenant_id: str = None) -> Filter:
    """
    Construye filtro de metadatos a partir de la query.
    tenant_id se inyecta SIEMPRE como filtro obligatorio.
    """
    must_conditions = []

    # FILTRO TENANT OBLIGATORIO
    if tenant_id:
        must_conditions.append(
            FieldCondition(key="tenant_id", match=MatchValue(value=str(tenant_id)))
        )

    if filter_source:
        must_conditions.append(
            FieldCondition(key="source", match=MatchValue(value=filter_source))
        )

    q = query.lower()

    # Filtro por tipo de documento
    doc_types = {
        "factura": "factura", "albarán": "albaran", "albaran": "albaran",
        "convenio": "convenio", "contrato": "contrato", "normativa": "normativa",
        "itv": "itv", "seguro": "seguro",
    }
    for keyword, doc_type in doc_types.items():
        if keyword in q:
            must_conditions.append(
                FieldCondition(key="text", match=MatchText(text=doc_type))
            )
            break

    # Filtro por matrícula
    mat_match = re.search(r'\b\d{4}[A-Za-z]{3}\b', query)
    if mat_match:
        must_conditions.append(
            FieldCondition(key="matricula", match=MatchValue(value=mat_match.group().upper()))
        )

    if not must_conditions:
        return None

    return Filter(must=must_conditions)


# ─────────────────────────────────────────────
# Búsqueda principal
# ─────────────────────────────────────────────

def search(query: str, top_k: int = None, filter_source: str = None,
           use_rerank: bool = True, pipeline_id: str = None,
           complexity: int = 3, tenant_id: str = None) -> list[dict]:
    """
    Búsqueda híbrida: semántica + keywords, con re-ranking opcional.
    V3.0 Multi-Tenant: tenant_id obligatorio si se proporciona.
    """
    client = get_qdrant_client()

    # Top-K dinámico si no se especificó
    if top_k is None:
        top_k = _dynamic_top_k(pipeline_id, complexity)

    # Metadata filter con tenant_id inyectado
    search_filter = _build_metadata_filter(query, filter_source, tenant_id=tenant_id)

    # Si hay tenant_id pero no se incluyó en filtro, forzar filtro mínimo
    if tenant_id and not search_filter:
        search_filter = Filter(must=[
            FieldCondition(key="tenant_id", match=MatchValue(value=str(tenant_id)))
        ])

    # ─── 1. Búsqueda semántica (vectores) ───
    query_vector = embed_query(query)
    fetch_k = top_k * 3 if use_rerank else top_k

    sem_response = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        limit=fetch_k,
        query_filter=search_filter,
        with_payload=True
    )

    sem_chunks = [
        {
            "text": r.payload.get("text", ""),
            "source": r.payload.get("source", ""),
            "page": r.payload.get("page", 0),
            "score": round(r.score, 4),
            "matricula": r.payload.get("matricula"),
            "fecha": r.payload.get("fecha"),
            "taller": r.payload.get("taller"),
            "total": r.payload.get("total"),
            "is_summary": r.payload.get("is_summary", False),
        }
        for r in sem_response.points
    ]

    # ─── 2. Búsqueda por keywords ───
    kw_chunks = _keyword_search(client, query, filter_source, fetch_k, tenant_id=tenant_id)

    # ─── 3. Fusión RRF ───
    merged = _reciprocal_rank_fusion(sem_chunks, kw_chunks)

    # ─── 4. Re-ranking ───
    if use_rerank and merged:
        return rerank(query, merged, top_k=top_k)

    return merged[:top_k]


def _keyword_search(client: QdrantClient, query: str,
                    filter_source: str = None, limit: int = 20,
                    tenant_id: str = None) -> list[dict]:
    """Busca por keywords en los campos de metadatos. Filtro tenant_id inyectado."""
    stopwords = {
        "que", "los", "las", "del", "por", "con", "una", "son", "para",
        "como", "más", "sus", "este", "esta", "todos", "todas", "tiene",
        "quiero", "dame", "dime", "empresa", "puedes", "puede", "cuáles",
        "cuál", "cuánto", "cuántos", "qué", "cómo", "cuándo",
    }
    words = [w.strip(".,;:!?¿¡()[]") for w in query.lower().split()
             if len(w) > 3 and w.lower() not in stopwords]

    if not words:
        return []

    results = []
    for word in words[:5]:
        try:
            scroll_filter_must = [
                FieldCondition(key="text", match=MatchText(text=word))
            ]
            if filter_source:
                scroll_filter_must.append(
                    FieldCondition(key="source", match=MatchValue(value=filter_source))
                )
            # FILTRO TENANT OBLIGATORIO
            if tenant_id:
                scroll_filter_must.append(
                    FieldCondition(key="tenant_id", match=MatchValue(value=str(tenant_id)))
                )

            batch, _ = client.scroll(
                collection_name=COLLECTION_NAME,
                scroll_filter=Filter(must=scroll_filter_must),
                limit=limit // len(words),
                with_payload=True
            )
            for r in batch:
                results.append({
                    "text": r.payload.get("text", ""),
                    "source": r.payload.get("source", ""),
                    "page": r.payload.get("page", 0),
                    "score": 0.5,
                    "matricula": r.payload.get("matricula"),
                    "fecha": r.payload.get("fecha"),
                    "taller": r.payload.get("taller"),
                    "total": r.payload.get("total"),
                    "is_summary": r.payload.get("is_summary", False),
                })
        except Exception:
            continue

    return results


def _reciprocal_rank_fusion(list_a: list[dict], list_b: list[dict],
                             k: int = 60) -> list[dict]:
    """Reciprocal Rank Fusion: combina dos listas rankeadas."""
    scores: dict[str, float] = {}
    chunk_map: dict[str, dict] = {}

    def _key(c):
        return f"{c['source']}|{c['page']}|{c['text'][:80]}"

    for rank, chunk in enumerate(list_a):
        key = _key(chunk)
        scores[key] = scores.get(key, 0) + 1.0 / (k + rank + 1)
        chunk_map[key] = chunk

    for rank, chunk in enumerate(list_b):
        key = _key(chunk)
        scores[key] = scores.get(key, 0) + 1.0 / (k + rank + 1)
        if key not in chunk_map:
            chunk_map[key] = chunk

    sorted_keys = sorted(scores, key=scores.get, reverse=True)
    return [chunk_map[k] for k in sorted_keys]


# ─── Funciones auxiliares (compatibilidad) ───

def list_indexed_documents() -> list[str]:
    """Devuelve los nombres de documentos indexados en Qdrant."""
    client = get_qdrant_client()
    try:
        results, _ = client.scroll(
            collection_name=COLLECTION_NAME,
            limit=1000,
            with_payload=["source"]
        )
        sources = list({r.payload["source"] for r in results})
        return sorted(sources)
    except Exception:
        return []


def get_all_chunks_by_source(chunks_per_source: int = 3) -> list[dict]:
    """Recupera todos los documentos de Qdrant con sus primeros N chunks."""
    client = get_qdrant_client()
    all_results = []
    offset = None

    while True:
        batch, next_offset = client.scroll(
            collection_name=COLLECTION_NAME,
            limit=500,
            offset=offset,
            with_payload=True
        )
        all_results.extend(batch)
        if next_offset is None:
            break
        offset = next_offset

    by_source: dict[str, list] = defaultdict(list)
    for r in all_results:
        src = r.payload.get("source", "")
        by_source[src].append({
            "text": r.payload.get("text", ""),
            "source": src,
            "page": r.payload.get("page", 0),
            "score": 1.0,
            "is_summary": r.payload.get("is_summary", False),
            "matricula": r.payload.get("matricula"),
        })

    result = []
    for src in sorted(by_source.keys()):
        chunks = by_source[src]
        summaries = [c for c in chunks if c.get("is_summary")]
        normals = [c for c in chunks if not c.get("is_summary")]
        normals.sort(key=lambda x: x["page"])
        selected = summaries + normals[:max(0, chunks_per_source - len(summaries))]
        result.extend(selected)

    return result
