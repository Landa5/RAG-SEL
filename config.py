"""
config.py — Configuración centralizada del proyecto RAG-SEL
"""
import os
from dotenv import load_dotenv

load_dotenv()

# === API Keys ===
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# === Qdrant ===
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "rag_sel_docs")

# === LLM Provider ===
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini")  # "gemini" o "openai"

# === Parámetros RAG ===
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
TOP_K = 15

# Embeddings locales multilingüe (español nativo)
EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"
EMBEDDING_DIMS = 384

# LLM (para generación de respuestas)
LLM_MODEL_GEMINI = "gemini-2.0-flash"
LLM_MODEL_OPENAI = "gpt-4o-mini"
