"""
tests/test_providers.py — Tests del módulo de proveedores IA + rate limiting
Cubre: cifrado Fernet, sanitización, CRUD mock, resolve_provider,
       effective_limit, 429 burst/diario/coste, precedencia tenant/app.
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Establecer RAGSEL_FERNET_KEY para tests
os.environ.setdefault("RAGSEL_FERNET_KEY", "cpRxgMjG48xRJDQbvGDYQUpgbK0ZoWvNRxO5ul8bZE0=")

# ── Check dependencias opcionales ──
try:
    from cryptography.fernet import Fernet
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

try:
    from fastapi import HTTPException
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False


# ═══════════════════════════════════════════════
# Fernet Encryption
# ═══════════════════════════════════════════════

@unittest.skipUnless(HAS_CRYPTO, "cryptography no instalado")
class TestFernetEncryption(unittest.TestCase):
    """Tests de cifrado/descifrado de API keys."""

    def test_encrypt_decrypt_roundtrip(self):
        from db.provider_db import encrypt_api_key, decrypt_api_key
        original = "sk-test-1234567890abcdef"
        encrypted = encrypt_api_key(original)
        self.assertNotEqual(encrypted, original)
        self.assertEqual(decrypt_api_key(encrypted), original)

    def test_encrypted_not_plaintext(self):
        from db.provider_db import encrypt_api_key
        api_key = "secret_api_key_value"
        encrypted = encrypt_api_key(api_key)
        self.assertNotIn(api_key, encrypted)

    def test_different_encryptions_each_time(self):
        """Fernet usa IV aleatorio, cada cifrado es distinto."""
        from db.provider_db import encrypt_api_key
        enc1 = encrypt_api_key("same_key")
        enc2 = encrypt_api_key("same_key")
        self.assertNotEqual(enc1, enc2)


# ═══════════════════════════════════════════════
# Sanitización
# ═══════════════════════════════════════════════

class TestSanitizeProvider(unittest.TestCase):

    def test_sanitize_removes_encrypted_key(self):
        from db.provider_db import _sanitize_provider
        row = {"id": "u1", "provider_name": "google",
               "api_key_encrypted": "secret", "status": "active"}
        result = _sanitize_provider(row)
        self.assertNotIn("api_key_encrypted", result)
        self.assertTrue(result["has_api_key"])

    def test_sanitize_no_key(self):
        from db.provider_db import _sanitize_provider
        row = {"id": "u2", "provider_name": "openai",
               "api_key_encrypted": None, "status": "pending_setup"}
        result = _sanitize_provider(row)
        self.assertFalse(result["has_api_key"])

    def test_sanitize_none_returns_none(self):
        from db.provider_db import _sanitize_provider
        self.assertIsNone(_sanitize_provider(None))


# ═══════════════════════════════════════════════
# Effective Limit (precedencia tenant/app)
# ═══════════════════════════════════════════════

class TestEffectiveLimit(unittest.TestCase):

    def _eff(self, a, b):
        from api.rate_limit import _effective_limit
        return _effective_limit(a, b)

    @unittest.skipUnless(HAS_FASTAPI, "FastAPI no instalado")
    def test_min_both_present(self):
        self.assertEqual(self._eff(100, 50), 50)
        self.assertEqual(self._eff(50, 100), 50)

    @unittest.skipUnless(HAS_FASTAPI, "FastAPI no instalado")
    def test_tenant_only(self):
        self.assertEqual(self._eff(100, None), 100)

    @unittest.skipUnless(HAS_FASTAPI, "FastAPI no instalado")
    def test_app_only(self):
        self.assertEqual(self._eff(None, 30), 30)

    @unittest.skipUnless(HAS_FASTAPI, "FastAPI no instalado")
    def test_both_none_means_no_limit(self):
        self.assertIsNone(self._eff(None, None))

    @unittest.skipUnless(HAS_FASTAPI, "FastAPI no instalado")
    def test_zero_is_valid_limit(self):
        self.assertEqual(self._eff(0, 50), 0)


# ═══════════════════════════════════════════════
# CRUD Mock
# ═══════════════════════════════════════════════

class TestProviderCRUDMock(unittest.TestCase):

    def _fake_row(self, **overrides):
        base = {
            "id": "uuid-new", "provider_name": "google",
            "display_name": "Google Gemini", "api_key_encrypted": "enc_xxx",
            "status": "active", "is_default": False, "priority": 100,
            "tenant_id": None, "models_available": [], "capabilities": {},
            "max_rpm": 60, "max_tpm": 100000, "monthly_budget_usd": None,
            "monthly_spent_usd": 0, "last_health_check": None,
            "last_error": None, "notes": "", "api_base_url": None,
            "created_at": "2026-01-01", "updated_at": "2026-01-01",
        }
        base.update(overrides)
        return base

    @unittest.skipUnless(HAS_CRYPTO, "cryptography no instalado")
    @patch("db.provider_db._execute")
    def test_create_with_key(self, mock_exec):
        from db.provider_db import create_provider
        mock_exec.return_value = [self._fake_row()]
        result = create_provider("google", "Google Gemini", api_key="test-key")
        self.assertIsNotNone(result)
        self.assertTrue(result["has_api_key"])
        self.assertNotIn("api_key_encrypted", result)

    @patch("db.provider_db._execute")
    def test_create_without_key(self, mock_exec):
        from db.provider_db import create_provider
        mock_exec.return_value = [self._fake_row(
            api_key_encrypted=None, status="pending_setup")]
        result = create_provider("anthropic", "Claude")
        self.assertEqual(result["status"], "pending_setup")
        self.assertFalse(result["has_api_key"])

    @patch("db.provider_db._execute")
    def test_list_providers(self, mock_exec):
        from db.provider_db import list_providers
        mock_exec.return_value = [self._fake_row()]
        result = list_providers()
        self.assertEqual(len(result), 1)
        self.assertNotIn("api_key_encrypted", result[0])

    @patch("db.provider_db._execute")
    def test_delete_provider(self, mock_exec):
        from db.provider_db import delete_provider
        mock_exec.return_value = None
        self.assertTrue(delete_provider("uuid-del"))


# ═══════════════════════════════════════════════
# Resolve provider (global + override tenant)
# ═══════════════════════════════════════════════

class TestResolveProvider(unittest.TestCase):

    @patch("db.provider_db._execute_one")
    def test_resolve_global_fallback(self, mock_one):
        from db.provider_db import resolve_provider
        mock_one.side_effect = [
            None,  # tenant override
            {"id": "g1", "provider_name": "google", "status": "active",
             "api_key_encrypted": None, "tenant_id": None,
             "display_name": "G"},
        ]
        result = resolve_provider("google", tenant_id="t1")
        self.assertIsNotNone(result)

    @patch("db.provider_db._execute_one")
    def test_resolve_tenant_wins(self, mock_one):
        from db.provider_db import resolve_provider
        mock_one.side_effect = [
            {"id": "t1", "provider_name": "google", "status": "active",
             "api_key_encrypted": None, "tenant_id": "t-x",
             "display_name": "G Override"},
        ]
        result = resolve_provider("google", tenant_id="t-x")
        self.assertEqual(result["display_name"], "G Override")

    @patch("db.provider_db._execute_one")
    def test_resolve_not_found(self, mock_one):
        from db.provider_db import resolve_provider
        mock_one.return_value = None
        self.assertIsNone(resolve_provider("nope", tenant_id="t1"))


# ═══════════════════════════════════════════════
# Default provider
# ═══════════════════════════════════════════════

class TestDefaultProvider(unittest.TestCase):

    @patch("db.provider_db._execute")
    def test_unset_default_global(self, mock_exec):
        from db.provider_db import _unset_default
        _unset_default(None)
        mock_exec.assert_called_once()

    @patch("db.provider_db._execute")
    def test_unset_default_tenant(self, mock_exec):
        from db.provider_db import _unset_default
        _unset_default("tenant-x")
        mock_exec.assert_called_once()


# ═══════════════════════════════════════════════
# Rate Limiting
# ═══════════════════════════════════════════════

@unittest.skipUnless(HAS_FASTAPI, "FastAPI no instalado")
class TestRateLimitRules(unittest.TestCase):
    """Tests de rate limiting con mocks de BD."""

    def _make_ctx(self):
        from api.auth import TenantContext
        return TenantContext(
            tenant_id="t1", app_id="a1", app_name="test",
            scopes=["query:run"], database_url=None
        )

    @patch("api.rate_limit._get_monthly_cost", return_value=10.0)
    @patch("api.rate_limit._count_tenant_queries_day_natural", return_value=50)
    @patch("api.rate_limit._count_queries_window", return_value=5)
    @patch("api.rate_limit._get_app_limits", return_value={})
    @patch("api.rate_limit._get_tenant_limits", return_value={
        "max_queries_per_minute": 20, "max_queries_per_day": 1000,
        "max_monthly_cost_usd": 100})
    def test_under_limits_passes(self, *_):
        from api.rate_limit import check_rate_limit
        result = check_rate_limit(self._make_ctx())
        self.assertEqual(result.tenant_id, "t1")

    @patch("api.rate_limit._count_queries_window", return_value=10)
    @patch("api.rate_limit._get_app_limits", return_value={})
    @patch("api.rate_limit._get_tenant_limits", return_value={
        "max_queries_per_minute": 10, "max_queries_per_day": 1000,
        "max_monthly_cost_usd": None})
    def test_burst_exceeded_429(self, *_):
        from api.rate_limit import check_rate_limit
        with self.assertRaises(HTTPException) as cm:
            check_rate_limit(self._make_ctx())
        self.assertEqual(cm.exception.status_code, 429)
        self.assertIn("queries/minuto", cm.exception.detail)

    @patch("api.rate_limit._get_monthly_cost", return_value=0)
    @patch("api.rate_limit._count_tenant_queries_day_natural", return_value=100)
    @patch("api.rate_limit._count_queries_window", return_value=5)
    @patch("api.rate_limit._get_app_limits", return_value={})
    @patch("api.rate_limit._get_tenant_limits", return_value={
        "max_queries_per_minute": 20, "max_queries_per_day": 100,
        "max_monthly_cost_usd": None})
    def test_daily_exceeded_429(self, *_):
        from api.rate_limit import check_rate_limit
        with self.assertRaises(HTTPException) as cm:
            check_rate_limit(self._make_ctx())
        self.assertEqual(cm.exception.status_code, 429)
        self.assertIn("queries/d", cm.exception.detail)

    @patch("api.rate_limit._get_monthly_cost", return_value=55.0)
    @patch("api.rate_limit._count_tenant_queries_day_natural", return_value=10)
    @patch("api.rate_limit._count_queries_window", return_value=1)
    @patch("api.rate_limit._get_app_limits", return_value={})
    @patch("api.rate_limit._get_tenant_limits", return_value={
        "max_queries_per_minute": 20, "max_queries_per_day": 1000,
        "max_monthly_cost_usd": 50.0})
    def test_cost_exceeded_429(self, *_):
        from api.rate_limit import check_rate_limit
        with self.assertRaises(HTTPException) as cm:
            check_rate_limit(self._make_ctx())
        self.assertEqual(cm.exception.status_code, 429)
        self.assertIn("Presupuesto mensual", cm.exception.detail)

    @patch("api.rate_limit._count_queries_window", return_value=5)
    @patch("api.rate_limit._get_app_limits",
           return_value={"max_queries_per_minute": 5})
    @patch("api.rate_limit._get_tenant_limits", return_value={
        "max_queries_per_minute": 20, "max_queries_per_day": 1000,
        "max_monthly_cost_usd": None})
    def test_app_more_restrictive_429(self, *_):
        """App limit 5/min, tenant 20/min → effective=5 → 429."""
        from api.rate_limit import check_rate_limit
        with self.assertRaises(HTTPException) as cm:
            check_rate_limit(self._make_ctx())
        self.assertEqual(cm.exception.status_code, 429)

    @patch("api.rate_limit._get_app_limits", return_value={
        "max_queries_per_minute": None, "max_queries_per_day": None,
        "max_monthly_cost_usd": None})
    @patch("api.rate_limit._get_tenant_limits", return_value={
        "max_queries_per_minute": None, "max_queries_per_day": None,
        "max_monthly_cost_usd": None})
    def test_both_null_no_limit(self, *_):
        """Ambos NULL → sin restricción."""
        from api.rate_limit import check_rate_limit
        result = check_rate_limit(self._make_ctx())
        self.assertEqual(result.tenant_id, "t1")


if __name__ == "__main__":
    unittest.main()
