"""
ingestion/embeddings.py — Embeddings locales con sentence-transformers
Gratuito, sin API key, compatible con Python 3.14
Modelo: paraphrase-multilingual-MiniLM-L12-v2 (384 dims, rápido y preciso para español/inglés)

NOTA: sentence-transformers + PyTorch son pesados (~800MB RAM).
En entornos con RAM limitada (Render free), se cargan bajo demanda.
"""

_model = None
MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"


def get_embedding_model():
    """Carga el modelo una sola vez (singleton). Primera vez descarga ~90MB."""
    global _model
    if _model is None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise RuntimeError(
                "sentence-transformers no está instalado. "
                "Instálalo con: pip install sentence-transformers"
            )
        print(f"⏳ Cargando modelo de embeddings '{MODEL_NAME}'...")
        _model = SentenceTransformer(MODEL_NAME)
        print("✅ Modelo cargado correctamente")
    return _model


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Genera embeddings para una lista de textos."""
    model = get_embedding_model()
    vectors = model.encode(texts, show_progress_bar=False, convert_to_numpy=True)
    return vectors.tolist()


def embed_query(query: str) -> list[float]:
    """Genera embedding para una consulta."""
    model = get_embedding_model()
    vector = model.encode([query], show_progress_bar=False, convert_to_numpy=True)
    return vector[0].tolist()
