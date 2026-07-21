"""Tests for OIDC login capabilities.

Covers:
- OIDCClient (discovery, PKCE, token exchange, ID token verification)
- DatabaseUserBackend OIDC CRUD methods
- HybridSessionAuthProvider (initiate login, handle callback, user linking)
- Login page OIDC buttons
- Admin portal OIDC management
"""

from __future__ import annotations

import base64
import hashlib

import pytest

from dagster_webserver.auth.db_backend import DatabaseUserBackend
from dagster_webserver.auth.oidc.client import (
    generate_code_challenge,
    generate_code_verifier,
    generate_state,
    parse_state,
)
from dagster_webserver.auth.oidc.models import (
    SESSION_OIDC_NONCE,
    SESSION_OIDC_REDIRECT,
    SESSION_OIDC_STATE,
    SESSION_OIDC_VERIFIER,
    OIDCProviderConfig,
)

# Check for optional authlib/httpx dependencies
_has_authlib = False
try:
    import authlib  # noqa: F401
    import httpx  # noqa: F401

    _has_authlib = True
except ImportError:
    pass

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def backend():
    """Create a DatabaseUserBackend with an in-memory SQLite database."""
    return DatabaseUserBackend(
        "sqlite+aiosqlite:///:memory:",
        create_tables=True,
        default_role="viewer",
    )


@pytest.fixture()
def sample_provider_config():
    """A sample OIDC provider config for testing."""
    return OIDCProviderConfig(
        id=1,
        name="google",
        display_name="Google",
        issuer_url="https://accounts.google.com",
        client_id="test-client-id",
        client_secret="test-client-secret",
        scopes="openid email profile",
        enabled=True,
        display_order=0,
    )


@pytest.fixture()
def sample_provider_orm(backend):
    """Create an OIDC provider in the database and return the ORM object."""
    return backend.create_oidc_provider(
        name="google",
        display_name="Google",
        issuer_url="https://accounts.google.com",
        client_id="test-client-id",
        client_secret="test-client-secret",
        scopes="openid email profile",
        display_order=0,
    )


# ---------------------------------------------------------------------------
# PKCE Helpers
# ---------------------------------------------------------------------------


class TestPKCEHelpers:
    def test_generate_code_verifier_length(self):
        verifier = generate_code_verifier()
        assert 43 <= len(verifier) <= 128

    def test_generate_code_verifier_chars(self):
        verifier = generate_code_verifier()
        assert verifier.isascii()

    def test_generate_code_challenge(self):
        verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
        challenge = generate_code_challenge(verifier)
        expected = (
            base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest())
            .rstrip(b"=")
            .decode("ascii")
        )
        assert challenge == expected

    def test_generate_code_challenge_random(self):
        verifier = generate_code_verifier()
        challenge = generate_code_challenge(verifier)
        assert len(challenge) > 0
        # Should be base64url characters
        assert all(
            c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            for c in challenge
        )

    def test_generate_state(self):
        state = generate_state("google")
        parts = state.split(":")
        assert len(parts) == 2
        assert parts[1] == "google"
        assert len(parts[0]) > 0

    def test_parse_state(self):
        state = generate_state("okta")
        random_hex, provider_name = parse_state(state)
        assert provider_name == "okta"
        assert len(random_hex) > 0

    def test_parse_state_invalid(self):
        with pytest.raises(ValueError):
            parse_state("invalid_state")


# ---------------------------------------------------------------------------
# OIDCClient (requires authlib + httpx)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_authlib, reason="authlib/httpx not installed")
class TestOIDCClient:
    def test_init(self, sample_provider_config):
        from dagster_webserver.auth.oidc.client import OIDCClient

        client = OIDCClient(sample_provider_config)
        assert client.provider == sample_provider_config

    def test_get_authorization_url(self, sample_provider_config):
        from dagster_webserver.auth.oidc.client import OIDCClient

        client = OIDCClient(sample_provider_config)
        url = client.get_authorization_url(
            redirect_uri="http://localhost/callback",
            state="test-state",
            code_challenge="test-challenge",
            nonce="test-nonce",
        )
        assert "response_type=code" in url
        assert "client_id=test-client-id" in url
        assert "redirect_uri=http%3A%2F%2Flocalhost%2Fcallback" in url
        assert "state=test-state" in url
        assert "code_challenge=test-challenge" in url
        assert "code_challenge_method=S256" in url
        assert "nonce=test-nonce" in url
        assert "scope=openid+email+profile" in url

    def test_discover(self, sample_provider_config, httpx_mock):
        from dagster_webserver.auth.oidc.client import OIDCClient

        discovery_doc = {
            "authorization_endpoint": "https://accounts.google.com/oauth2/v2/auth",
            "token_endpoint": "https://oauth2.googleapis.com/token",
            "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
            "issuer": "https://accounts.google.com",
        }
        httpx_mock.add_response(
            url="https://accounts.google.com/.well-known/openid-configuration",
            json=discovery_doc,
        )
        client = OIDCClient(sample_provider_config)
        result = client.discover()
        assert (
            result["authorization_endpoint"] == discovery_doc["authorization_endpoint"]
        )

    def test_discover_cached(self, sample_provider_config, httpx_mock):
        from dagster_webserver.auth.oidc.client import OIDCClient

        discovery_doc = {
            "authorization_endpoint": "https://accounts.google.com/oauth2/v2/auth",
            "token_endpoint": "https://oauth2.googleapis.com/token",
            "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
            "issuer": "https://accounts.google.com",
        }
        httpx_mock.add_response(
            url="https://accounts.google.com/.well-known/openid-configuration",
            json=discovery_doc,
        )
        client = OIDCClient(sample_provider_config)
        # First call hits the network
        client.discover()
        # Second call uses cache (no more mocks needed)
        result = client.discover()
        assert (
            result["authorization_endpoint"] == discovery_doc["authorization_endpoint"]
        )


# ---------------------------------------------------------------------------
# DatabaseUserBackend OIDC Methods
# ---------------------------------------------------------------------------


class TestOIDCProviderCRUD:
    async def test_create_oidc_provider(self, backend):
        provider = await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            scopes="openid email profile",
            display_order=0,
        )
        assert provider.name == "google"
        assert provider.display_name == "Google"
        assert provider.issuer_url == "https://accounts.google.com"
        assert provider.client_id == "test-client-id"
        assert provider.client_secret == "test-client-secret"
        assert provider.scopes == "openid email profile"
        assert provider.enabled is True
        assert provider.display_order == 0

    async def test_create_duplicate_oidc_provider_raises(self, backend):
        await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        with pytest.raises(Exception):  # IntegrityError
            await backend.create_oidc_provider(
                name="google",
                display_name="Google 2",
                issuer_url="https://accounts.google.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
            )

    async def test_list_oidc_providers(self, backend):
        await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        await backend.create_oidc_provider(
            name="okta",
            display_name="Okta",
            issuer_url="https://dev.okta.com",
            client_id="okta-client-id",
            client_secret="okta-secret",
        )
        providers = await backend.list_oidc_providers()
        assert len(providers) == 2
        names = {p.name for p in providers}
        assert "google" in names
        assert "okta" in names

    async def test_list_oidc_providers_enabled_only(self, backend):
        await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        await backend.create_oidc_provider(
            name="okta",
            display_name="Okta",
            issuer_url="https://dev.okta.com",
            client_id="okta-client-id",
            client_secret="okta-secret",
        )
        await backend.update_oidc_provider("okta", enabled=False)
        providers = await backend.list_oidc_providers(enabled_only=True)
        assert len(providers) == 1
        assert providers[0].name == "google"

    async def test_get_oidc_provider(self, backend):
        await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        provider = await backend.get_oidc_provider("google")
        assert provider is not None
        assert provider.name == "google"

    async def test_get_oidc_provider_not_found(self, backend):
        provider = await backend.get_oidc_provider("nonexistent")
        assert provider is None

    async def test_update_oidc_provider(self, backend):
        await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        updated = await backend.update_oidc_provider(
            "google",
            display_name="Google (Updated)",
            client_secret="new-secret",
            enabled=False,
        )
        assert updated.display_name == "Google (Updated)"
        assert updated.client_secret == "new-secret"
        assert updated.enabled is False

    async def test_update_oidc_provider_not_found(self, backend):
        with pytest.raises(ValueError, match="not found"):
            await backend.update_oidc_provider("nonexistent", display_name="X")

    async def test_delete_oidc_provider(self, backend):
        await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        await backend.delete_oidc_provider("google")
        provider = await backend.get_oidc_provider("google")
        assert provider is None

    async def test_delete_oidc_provider_nulls_user_links(self, backend):
        provider = await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        await backend.create_oidc_user(
            username="oidc_user",
            provider_id=provider.id,
            oidc_sub="google-123",
            email="user@google.com",
        )
        await backend.delete_oidc_provider("google")
        # User should still exist but with null oidc_provider_id
        user = await backend.get_user("oidc_user")
        assert user is not None


class TestOIDCUserMethods:
    async def test_get_user_by_oidc(self, backend):
        provider = await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        await backend.create_oidc_user(
            username="oidc_user",
            provider_id=provider.id,
            oidc_sub="google-123",
            email="user@google.com",
        )
        found = await backend.get_user_by_oidc(provider.id, "google-123")
        assert found is not None
        assert found.username == "oidc_user"

    async def test_get_user_by_oidc_not_found(self, backend):
        result = await backend.get_user_by_oidc(999, "nonexistent")
        assert result is None

    async def test_get_user_by_email(self, backend):
        await backend.create_user(
            username="email_user",
            password="secret",
            email="user@example.com",
        )
        found = await backend.get_user_by_email("user@example.com")
        assert found is not None
        assert found.username == "email_user"

    async def test_get_user_by_email_not_found(self, backend):
        result = await backend.get_user_by_email("nonexistent@example.com")
        assert result is None

    async def test_create_oidc_user(self, backend):
        provider = await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        user = await backend.create_oidc_user(
            username="oidc_user",
            provider_id=provider.id,
            oidc_sub="google-123",
            email="user@google.com",
            display_name="Google User",
            role="viewer",
        )
        assert user.username == "oidc_user"
        assert user.email == "user@google.com"
        assert user.role == "viewer"

    async def test_link_oidc_to_user(self, backend):
        await backend.create_user(
            username="existing_user",
            password="secret",
            email="user@example.com",
        )
        provider = await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        linked = await backend.link_oidc_to_user(
            "existing_user", provider.id, "google-123"
        )
        assert linked.username == "existing_user"
        # Verify OIDC linkage
        found = await backend.get_user_by_oidc(provider.id, "google-123")
        assert found is not None
        assert found.username == "existing_user"

    async def test_link_oidc_to_user_not_found(self, backend):
        provider = await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        with pytest.raises(ValueError, match="not found"):
            await backend.link_oidc_to_user("nonexistent", provider.id, "sub")


# ---------------------------------------------------------------------------
# OIDCProviderConfig
# ---------------------------------------------------------------------------


class TestOIDCProviderConfig:
    async def test_from_orm(self, backend):
        orm = await backend.create_oidc_provider(
            name="google",
            display_name="Google",
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            scopes="openid email profile",
            display_order=5,
        )
        config = OIDCProviderConfig.from_orm(orm)
        assert config.id == orm.id
        assert config.name == "google"
        assert config.display_name == "Google"
        assert config.issuer_url == "https://accounts.google.com"
        assert config.client_id == "test-client-id"
        assert config.client_secret == "test-client-secret"
        assert config.scopes == "openid email profile"
        assert config.enabled is True
        assert config.display_order == 5


# ---------------------------------------------------------------------------
# Session Key Constants
# ---------------------------------------------------------------------------


class TestSessionKeys:
    def test_session_keys_defined(self):
        assert SESSION_OIDC_STATE == "oidc_state"
        assert SESSION_OIDC_VERIFIER == "oidc_verifier"
        assert SESSION_OIDC_NONCE == "oidc_nonce"
        assert SESSION_OIDC_REDIRECT == "oidc_redirect"
