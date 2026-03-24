"""
retrieval/reranker.py — Re-ranking de resultados con cross-encoder
Reordena los chunks recuperados por búsqueda semántica usando un modelo
de cross-encoder que evalúa la relevancia pregunta↔chunk de forma
más precisa que la similitud coseno.
"""
from sentence_transformers import CrossEncoder

_reranker = None
RERANKER_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"


def get_reranker() -> CrossEncoder:
    """Singleton del cross-encoder (se carga una sola vez)."""
    global _reranker
    if _reranker is None:
        print("🔄 Cargando cross-encoder para re-ranking...")
        _reranker = CrossEncoder(RERANKER_MODEL)
    return _reranker


def rerank(query: str, chunks: list[dict], top_k: int = 15) -> list[dict]:
    """
    Reordena los chunks por relevancia real usando cross-encoder.

    Args:
        query: Pregunta del usuario.
        chunks: Lista de chunks con campo 'text'.
        top_k: Cuántos devolver tras reordenar.

    Returns:
        Lista reordenada de chunks (los más relevantes primero).
    """
    if not chunks:
        return []

    reranker = get_reranker()

    # Pares (pregunta, texto_chunk) para el cross-encoder
    pairs = [(query, c["text"]) for c in chunks]
    scores = reranker.predict(pairs)

    # Asignar scores y ordenar
    for chunk, score in zip(chunks, scores):
        chunk["rerank_score"] = float(score)

    ranked = sorted(chunks, key=lambda x: x["rerank_score"], reverse=True)
    return ranked[:top_k]
