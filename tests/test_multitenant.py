"""
tests/test_multitenant.py — Tests de la plataforma multi-tenant RAG-SEL
Cubre: scopes, connection_ref, upload validation, aislamiento, schemas.
NOTA: Tests de auth/API requieren FastAPI — se saltan si no está disponible.
"""
import sys
import os
import hashlib
import inspect
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from unittest.mock import patch, MagicMock

# Comprobar dependencias opcionales
try:
    import fastapi
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

try:
    import qdrant_client
    HAS_QDRANT = True
except ImportError:
    HAS_QDRANT = False

try:
    import sentence_transformers
    HAS_SBERT = True
except ImportError:
    HAS_SBERT = False


# ─────────────────────────────────────────────
# Tests de Scopes
# ─────────────────────────────────────────────

class TestScopeValidation(unittest.TestCase):
    def test_valid_scopes(self):
        from db.tenant_db import VALID_SCOPES
        expected = {
            "query:run", "rag:query", "documents:upload", "documents:list",
            "documents:delete", "analytics:query", "predictions:run",
            "executions:read",
            "admin:tenants", "admin:apps", "admin:credentials",
            "admin:providers", "admin:usage", "admin:reviews",
            "superadmin:*",
        }
        self.assertEqual(VALID_SCOPES, expected)

    def test_invalid_scope_rejected(self):
        from db.tenant_db import VALID_SCOPES
        self.assertNotIn("hack:everything", VALID_SCOPES)

    def test_15_scopes_defined(self):
        from db.tenant_db import VALID_SCOPES
        self.assertEqual(len(VALID_SCOPES), 15)


# ─────────────────────────────────────────────
# Tests de API Key Hashing
# ─────────────────────────────────────────────

class TestApiKeyHashing(unittest.TestCase):
    def test_hash_is_sha256(self):
        from db.tenant_db import _hash_api_key
        key = "rsk_test_key_12345"
        self.assertEqual(_hash_api_key(key), hashlib.sha256(key.encode()).hexdigest())

    def test_different_keys_different_hashes(self):
        from db.tenant_db import _hash_api_key
        self.assertNotEqual(_hash_api_key("key_one"), _hash_api_key("key_two"))


# ─────────────────────────────────────────────
# Tests de Connection Ref
# ─────────────────────────────────────────────

class TestConnectionRef(unittest.TestCase):
    def test_env_ref(self):
        from db.tenant_db import resolve_database_url
        with patch.dict(os.environ, {"MY_DB_URL": "postgresql://test:5432/db"}):
            self.assertEqual(resolve_database_url("env:MY_DB_URL"), "postgresql://test:5432/db")

    def test_env_ref_missing(self):
        from db.tenant_db import resolve_database_url
        with self.assertRaises(ValueError):
            resolve_database_url("env:NONEXISTENT_VAR_XYZ")

    def test_vault_ref_not_implemented(self):
        from db.tenant_db import resolve_database_url
        with self.assertRaises(NotImplementedError):
            resolve_database_url("vault:my_secret")

    def test_empty_ref(self):
        from db.tenant_db import resolve_database_url
        with self.assertRaises(ValueError):
            resolve_database_url("")

    def test_unknown_format(self):
        from db.tenant_db import resolve_database_url
        with self.assertRaises(ValueError):
            resolve_database_url("ftp://something")


# ─────────────────────────────────────────────
# Tests de Upload Validation
# ─────────────────────────────────────────────

class TestUploadValidation(unittest.TestCase):
    @patch("db.tenant_db.get_tenant")
    @patch("db.tenant_db._execute")
    def test_size_limit(self, mock_execute, mock_tenant):
        from db.tenant_db import check_upload_allowed
        mock_tenant.return_value = {
            "active": True, "max_document_size_mb": 10,
            "allowed_mime_types": ["application/pdf"], "max_documents": 500,
        }
        ok, msg = check_upload_allowed("t1", 11 * 1024 * 1024, "application/pdf", "abc")
        self.assertFalse(ok)
        self.assertIn("limite", msg)

    @patch("db.tenant_db.get_tenant")
    @patch("db.tenant_db._execute")
    def test_mime_type(self, mock_execute, mock_tenant):
        from db.tenant_db import check_upload_allowed
        mock_tenant.return_value = {
            "active": True, "max_document_size_mb": 50,
            "allowed_mime_types": ["application/pdf"], "max_documents": 500,
        }
        ok, msg = check_upload_allowed("t1", 1024, "application/zip", "abc")
        self.assertFalse(ok)
        self.assertIn("MIME", msg)

    @patch("db.tenant_db.get_tenant")
    @patch("db.tenant_db._execute")
    def test_quota_exceeded(self, mock_execute, mock_tenant):
        from db.tenant_db import check_upload_allowed
        mock_tenant.return_value = {
            "active": True, "max_document_size_mb": 50,
            "allowed_mime_types": ["application/pdf"], "max_documents": 5,
        }
        mock_execute.return_value = [{"count": 5}]
        ok, msg = check_upload_allowed("t1", 1024, "application/pdf", "abc")
        self.assertFalse(ok)
        self.assertIn("Cuota", msg)

    @patch("db.tenant_db.get_tenant")
    @patch("db.tenant_db._execute")
    def test_dedup(self, mock_execute, mock_tenant):
        from db.tenant_db import check_upload_allowed
        mock_tenant.return_value = {
            "active": True, "max_document_size_mb": 50,
            "allowed_mime_types": ["application/pdf"], "max_documents": 500,
        }
        mock_execute.side_effect = [[{"count": 0}], [{"id": "existing"}]]
        ok, msg = check_upload_allowed("t1", 1024, "application/pdf", "abc123")
        self.assertFalse(ok)
        self.assertIn("duplicado", msg)

    @patch("db.tenant_db.get_tenant")
    def test_inactive_tenant(self, mock_tenant):
        from db.tenant_db import check_upload_allowed
        mock_tenant.return_value = {"active": False}
        ok, msg = check_upload_allowed("t1", 1024, "application/pdf", "abc")
        self.assertFalse(ok)
        self.assertIn("desactivado", msg)

    @patch("db.tenant_db.get_tenant")
    def test_tenant_not_found(self, mock_tenant):
        from db.tenant_db import check_upload_allowed
        mock_tenant.return_value = None
        ok, msg = check_upload_allowed("t1", 1024, "application/pdf", "abc")
        self.assertFalse(ok)
        self.assertIn("no encontrado", msg)


# ─────────────────────────────────────────────
# Tests de Aislamiento
# ─────────────────────────────────────────────

class TestAislamientoConnector(unittest.TestCase):
    """Aislamiento en connector — sin dependencias externas."""

    def test_connector_accepts_database_url(self):
        from db.connector import run_safe_query
        self.assertIn("database_url", inspect.signature(run_safe_query).parameters)

    def test_connector_has_tenant_conn(self):
        from db.connector import _get_tenant_conn
        self.assertTrue(callable(_get_tenant_conn))


@unittest.skipUnless(HAS_QDRANT, "qdrant_client no instalado")
class TestAislamientoQdrant(unittest.TestCase):
    """Aislamiento en búsqueda Qdrant."""

    def test_search_accepts_tenant_id(self):
        from retrieval.search import search
        self.assertIn("tenant_id", inspect.signature(search).parameters)

    def test_metadata_filter_includes_tenant(self):
        from retrieval.search import _build_metadata_filter
        f = _build_metadata_filter("test query", tenant_id="tenant-abc")
        self.assertIsNotNone(f)
        tenant_conds = [c for c in f.must if hasattr(c, 'key') and c.key == "tenant_id"]
        self.assertEqual(len(tenant_conds), 1)

    def test_metadata_filter_without_tenant(self):
        from retrieval.search import _build_metadata_filter
        f = _build_metadata_filter("test query")
        self.assertIsNone(f)

    def test_keyword_search_accepts_tenant_id(self):
        from retrieval.search import _keyword_search
        self.assertIn("tenant_id", inspect.signature(_keyword_search).parameters)


@unittest.skipUnless(HAS_QDRANT and HAS_SBERT, "qdrant_client o sentence_transformers no instalado")
class TestAislamientoIngestion(unittest.TestCase):
    """Aislamiento en indexación."""

    def test_index_requires_tenant_id(self):
        from ingestion.index_documents import index_chunks_with_tenant
        with self.assertRaises(ValueError):
            index_chunks_with_tenant([{"text": "test"}], "")

    def test_delete_requires_tenant_id(self):
        from ingestion.index_documents import delete_chunks_by_source_and_tenant
        with self.assertRaises(ValueError):
            delete_chunks_by_source_and_tenant("file.pdf", "")

    def test_index_empty_chunks_returns_zero(self):
        from ingestion.index_documents import index_chunks_with_tenant
        self.assertEqual(index_chunks_with_tenant([], "some-tenant"), 0)


# ─────────────────────────────────────────────
# Tests de Platform Schema
# ─────────────────────────────────────────────

class TestPlatformSchema(unittest.TestCase):
    def test_schema_name(self):
        from db.tenant_db import SCHEMA
        self.assertEqual(SCHEMA, "platform")

    def test_schema_not_rag_engine(self):
        from db.tenant_db import SCHEMA as P
        from db.model_db import SCHEMA as R
        self.assertNotEqual(P, R)


# ─────────────────────────────────────────────
# Tests de API (solo si FastAPI disponible)
# ─────────────────────────────────────────────

@unittest.skipUnless(HAS_FASTAPI, "FastAPI no instalado")
class TestTenantContext(unittest.TestCase):
    def test_context_fields(self):
        from api.auth import TenantContext
        ctx = TenantContext(
            tenant_id="t1", app_id="a1", app_name="Test",
            scopes=["query:run"], database_url="postgresql://...",
        )
        self.assertEqual(ctx.tenant_id, "t1")
        self.assertEqual(ctx.scopes, ["query:run"])

    def test_defaults(self):
        from api.auth import TenantContext
        ctx = TenantContext(tenant_id="t1", app_id="a1", app_name="App")
        self.assertEqual(ctx.max_documents, 500)
        self.assertIsNone(ctx.database_url)


@unittest.skipUnless(HAS_FASTAPI, "FastAPI no instalado")
class TestAPISchemas(unittest.TestCase):
    def test_query_request(self):
        from api.api_v1 import QueryRequest
        req = QueryRequest(question="Hola")
        self.assertEqual(req.question, "Hola")

    def test_query_request_empty_rejected(self):
        from api.api_v1 import QueryRequest
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            QueryRequest(question="")

    def test_health_response(self):
        from api.api_v1 import HealthResponse
        h = HealthResponse()
        self.assertEqual(h.status, "ok")
        self.assertEqual(h.version, "3.0.0")


@unittest.skipUnless(HAS_FASTAPI, "FastAPI no instalado")
class TestAdminSchemas(unittest.TestCase):
    def test_create_app(self):
        from api.admin_v1 import CreateAppRequest
        req = CreateAppRequest(name="App", scopes=["query:run"])
        self.assertEqual(req.name, "App")

    def test_slug_pattern(self):
        from api.admin_v1 import CreateTenantRequest
        from pydantic import ValidationError
        CreateTenantRequest(name="T", slug="mi-tenant")
        with self.assertRaises(ValidationError):
            CreateTenantRequest(name="T", slug="MI TENANT")


@unittest.skipUnless(HAS_FASTAPI, "FastAPI no instalado")
class TestRouters(unittest.TestCase):
    def test_api_v1_prefix(self):
        from api.api_v1 import router
        self.assertEqual(router.prefix, "/api/v1")

    def test_admin_v1_prefix(self):
        from api.admin_v1 import router
        self.assertEqual(router.prefix, "/admin/v1")


class TestServerNoLegacy(unittest.TestCase):
    def test_no_legacy_imports(self):
        path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "api", "server.py")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertNotIn("from llm.router import", content)
        self.assertNotIn("import llm.router", content)


if __name__ == "__main__":
    unittest.main(verbosity=2)
