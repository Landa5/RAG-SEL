"""
ingestion/index_documents.py — Indexación de chunks en Qdrant
"""
import uuid
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct, Filter,
    FieldCondition, MatchValue
)
from ingestion.embeddings import embed_texts
from config import (
    QDRANT_URL, QDRANT_API_KEY, COLLECTION_NAME, EMBEDDING_DIMS
)


def get_qdrant_client() -> QdrantClient:
    """Crea y devuelve el cliente de Qdrant."""
    return QdrantClient(
        url=QDRANT_URL,
        api_key=QDRANT_API_KEY if QDRANT_API_KEY else None,
        timeout=30
    )


def ensure_collection(client: QdrantClient):
    """Crea la colección en Qdrant si no existe."""
    collections = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in collections:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(
                size=EMBEDDING_DIMS,
                distance=Distance.COSINE
            )
        )
        print(f"✅ Colección '{COLLECTION_NAME}' creada en Qdrant")
    else:
        print(f"ℹ️  Colección '{COLLECTION_NAME}' ya existe")


def is_document_indexed(client: QdrantClient, filename: str) -> bool:
    """Comprueba si ya hay chunks de este documento indexados."""
    results = client.scroll(
        collection_name=COLLECTION_NAME,
        scroll_filter=Filter(
            must=[FieldCondition(key="source", match=MatchValue(value=filename))]
        ),
        limit=1
    )
    return len(results[0]) > 0


def index_chunks(chunks: list[dict], force: bool = False) -> int:
    """
    Indexa una lista de chunks en Qdrant.
    - Si el documento ya está indexado y force=False, omite.
    - Devuelve el número de chunks indexados.
    """
    if not chunks:
        return 0

    client = get_qdrant_client()
    ensure_collection(client)

    filename = chunks[0]["source"]

    if not force and is_document_indexed(client, filename):
        print(f"⚠️  '{filename}' ya está indexado. Usa force=True para reindexar.")
        return 0

    # Eliminar chunks anteriores si se fuerza reindexado
    if force:
        client.delete(
            collection_name=COLLECTION_NAME,
            points_selector=Filter(
                must=[FieldCondition(key="source", match=MatchValue(value=filename))]
            )
        )

    texts = [c["text"] for c in chunks]
    print(f"🔢 Generando embeddings para {len(texts)} chunks...")
    vectors = embed_texts(texts)

    points = []
    for chunk, vector in zip(chunks, vectors):
        payload = {
            "text": chunk["text"],
            "source": chunk["source"],
            "page": chunk["page"],
            "chunk_id": chunk.get("chunk_id", ""),
            "ocr": chunk.get("ocr", False),
            "is_summary": chunk.get("is_summary", False),
        }
        # Metadatos estructurados del OCR
        meta = chunk.get("metadata", {})
        if meta:
            for key in ["matricula", "fecha", "taller", "descripcion",
                         "total", "proveedor", "cliente", "tipo_documento",
                         "numero_factura"]:
                if meta.get(key) is not None:
                    payload[key] = meta[key]

        points.append(PointStruct(
            id=str(uuid.uuid4()),
            vector=vector,
            payload=payload
        ))

    client.upsert(collection_name=COLLECTION_NAME, points=points)
    print(f"✅ {len(points)} chunks indexados para '{filename}'")
    return len(points)


# ─────────────────────────────────────────────
# Multi-tenant: indexación con tenant_id
# ─────────────────────────────────────────────

def index_chunks_with_tenant(chunks: list[dict], tenant_id: str) -> int:
    """
    Indexa chunks en Qdrant con tenant_id en el payload.
    Obligatorio para aislamiento multi-tenant.
    """
    if not chunks:
        return 0
    if not tenant_id:
        raise ValueError("tenant_id obligatorio para indexar chunks")

    client = get_qdrant_client()
    ensure_collection(client)

    texts = [c["text"] for c in chunks]
    vectors = embed_texts(texts)

    points = []
    for chunk, vector in zip(chunks, vectors):
        payload = {
            "text": chunk["text"],
            "source": chunk["source"],
            "page": chunk["page"],
            "chunk_id": chunk.get("chunk_id", ""),
            "ocr": chunk.get("ocr", False),
            "is_summary": chunk.get("is_summary", False),
            "tenant_id": str(tenant_id),  # AISLAMIENTO MULTI-TENANT
        }
        meta = chunk.get("metadata", {})
        if meta:
            for key in ["matricula", "fecha", "taller", "descripcion",
                         "total", "proveedor", "cliente", "tipo_documento",
                         "numero_factura"]:
                if meta.get(key) is not None:
                    payload[key] = meta[key]

        points.append(PointStruct(
            id=str(uuid.uuid4()),
            vector=vector,
            payload=payload
        ))

    client.upsert(collection_name=COLLECTION_NAME, points=points)
    print(f"✅ {len(points)} chunks indexados (tenant: {tenant_id})")
    return len(points)


def delete_chunks_by_source_and_tenant(filename: str, tenant_id: str):
    """
    Elimina chunks de Qdrant filtrados por source + tenant_id.
    Obligatorio: nunca borrar sin filtro de tenant.
    """
    if not tenant_id:
        raise ValueError("tenant_id obligatorio para borrar chunks")

    client = get_qdrant_client()
    client.delete(
        collection_name=COLLECTION_NAME,
        points_selector=Filter(
            must=[
                FieldCondition(key="source", match=MatchValue(value=filename)),
                FieldCondition(key="tenant_id", match=MatchValue(value=str(tenant_id))),
            ]
        )
    )
    print(f"🗑️ Chunks eliminados: {filename} (tenant: {tenant_id})")

