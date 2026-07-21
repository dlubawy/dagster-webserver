"""OIDC client wrapper using joserfc + httpx.

Handles OIDC discovery, authorization URL building (with PKCE),
token exchange, and ID token verification.

Requires the optional ``auth-oidc`` dependency group
(``pip install dagster-webserver[auth-oidc]``).
"""

from __future__ import annotations

import hashlib
import logging
import secrets
import time
from base64 import urlsafe_b64encode
from typing import Any

from dagster_webserver.auth.oidc.models import OIDCProviderConfig

logger = logging.getLogger("dagster-webserver.auth.oidc")

# ── PKCE helpers ───────────────────────────────────────────────────


def generate_code_verifier() -> str:
    """Generate a random code verifier (43–128 chars, RFC 7636)."""
    return secrets.token_urlsafe(64)  # 86 chars


def generate_code_challenge(verifier: str) -> str:
    """S256 code challenge: BASE64URL(SHA256(verifier))."""
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def generate_state(provider_name: str) -> str:
    """Generate a state parameter: ``{random_hex}:{provider_name}``."""
    random_hex = secrets.token_hex(32)
    return f"{random_hex}:{provider_name}"


def parse_state(state: str) -> tuple[str, str]:
    """Parse state into ``(random_hex, provider_name)``."""
    try:
        random_hex, provider_name = state.rsplit(":", 1)
        return random_hex, provider_name
    except ValueError as exc:
        raise ValueError(f"Invalid OIDC state parameter: {state!r}") from exc


# ── OIDC Client ────────────────────────────────────────────────────


class OIDCClient:
    """Async OIDC client for a single provider.

    Wraps joserfc's JWT verification and httpx for HTTP calls.
    """

    def __init__(self, provider: OIDCProviderConfig) -> None:
        self._provider = provider
        self._discovery: dict[str, Any] | None = None
        self._jwks: dict[str, Any] | None = None

    # ── Discovery ────────────────────────────────────────────────

    async def discover(self) -> dict[str, Any]:
        """Fetch ``.well-known/openid-configuration`` and cache it."""
        from httpx import AsyncClient, HTTPStatusError

        if self._discovery is not None:
            return self._discovery

        url = f"{self._provider.issuer_url}/.well-known/openid-configuration"
        async with AsyncClient(timeout=10) as http:
            try:
                resp = await http.get(url)
                resp.raise_for_status()
            except HTTPStatusError as exc:
                raise RuntimeError(
                    f"OIDC discovery failed for {self._provider.name}: {exc}"
                ) from exc
        self._discovery = resp.json()
        return self._discovery

    # ── Authorization URL ────────────────────────────────────────

    def get_authorization_url(
        self,
        redirect_uri: str,
        state: str,
        code_challenge: str,
        nonce: str,
    ) -> str:
        """Build the OIDC authorization endpoint URL.

        Uses the cached discovery document (or the issuer URL with a
        standard path as fallback).
        """
        from urllib.parse import urlencode

        discovery = self._discovery or {}
        auth_url = discovery.get(
            "authorization_endpoint",
            f"{self._provider.issuer_url}/authorize",
        )

        params = {
            "response_type": "code",
            "client_id": self._provider.client_id,
            "redirect_uri": redirect_uri,
            "scope": self._provider.scopes,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "nonce": nonce,
        }
        return f"{auth_url}?{urlencode(params)}"

    # ── Token Exchange ───────────────────────────────────────────

    async def exchange_code(
        self, code: str, redirect_uri: str, code_verifier: str
    ) -> dict[str, str]:
        """Exchange authorization code for tokens (with PKCE).

        Returns ``{"id_token": ..., "access_token": ...}``.
        """
        from httpx import AsyncClient, HTTPStatusError

        discovery = await self.discover()
        token_url = discovery.get(
            "token_endpoint",
            f"{self._provider.issuer_url}/token",
        )

        async with AsyncClient(timeout=10) as http:
            try:
                resp = await http.post(
                    token_url,
                    data={
                        "grant_type": "authorization_code",
                        "code": code,
                        "redirect_uri": redirect_uri,
                        "client_id": self._provider.client_id,
                        "client_secret": self._provider.client_secret,
                        "code_verifier": code_verifier,
                    },
                )
                resp.raise_for_status()
            except HTTPStatusError as exc:
                raise RuntimeError(
                    f"OIDC token exchange failed for {self._provider.name}: {exc}"
                ) from exc

        tokens = resp.json()
        if "id_token" not in tokens:
            raise RuntimeError(
                f"OIDC token response for {self._provider.name} missing id_token"
            )
        return tokens

    # ── ID Token Verification ────────────────────────────────────

    async def verify_id_token(
        self, id_token: str, nonce: str | None = None
    ) -> dict[str, Any]:
        """Verify the ID token and return its claims.

        Checks:
        - Signature (via JWKS)
        - ``iss`` matches configured issuer
        - ``aud`` contains our client_id
        - ``exp`` is in the future
        - ``nonce`` matches (if provided)

        Raises ``ValueError`` on any verification failure.
        """
        from joserfc import jwt
        from joserfc.jwk import KeySet

        # Fetch JWKS and build KeySet
        jwks_dict = await self._fetch_jwks()
        try:
            key_set = KeySet.import_key_set(jwks_dict)
        except Exception as exc:
            raise ValueError(f"Failed to load JWKS: {exc}") from exc

        # Verify signature and decode claims
        try:
            token = jwt.decode(id_token, key_set)
        except Exception as exc:
            raise ValueError(f"ID token verification failed: {exc}") from exc

        claims = dict(token.claims)

        # Check issuer
        if claims.get("iss") != self._provider.issuer_url:
            raise ValueError(
                f"ID token issuer mismatch: expected {self._provider.issuer_url!r}, "
                f"got {claims.get('iss')!r}"
            )

        # Check audience
        aud = claims.get("aud")
        if isinstance(aud, list):
            if self._provider.client_id not in aud:
                raise ValueError(
                    f"ID token audience mismatch: {self._provider.client_id!r} not in {aud!r}"
                )
        elif aud != self._provider.client_id:
            raise ValueError(
                f"ID token audience mismatch: expected {self._provider.client_id!r}, "
                f"got {aud!r}"
            )

        # Check expiration
        exp = claims.get("exp")
        if exp is not None and exp < time.time():
            raise ValueError("ID token has expired")

        # Check nonce
        if nonce is not None and claims.get("nonce") != nonce:
            raise ValueError("ID token nonce mismatch")

        return claims

    # ── JWKS ─────────────────────────────────────────────────────

    async def _fetch_jwks(self) -> dict[str, Any]:
        """Fetch and cache the JWKS from the provider."""
        from httpx import AsyncClient, HTTPStatusError

        if self._jwks is not None:
            return self._jwks

        discovery = await self.discover()
        jwks_url = discovery.get(
            "jwks_uri",
            f"{self._provider.issuer_url}/.well-known/jwks.json",
        )

        async with AsyncClient(timeout=10) as http:
            try:
                resp = await http.get(jwks_url)
                resp.raise_for_status()
            except HTTPStatusError as exc:
                raise RuntimeError(
                    f"Failed to fetch JWKS from {self._provider.name}: {exc}"
                ) from exc
        self._jwks = resp.json()
        return self._jwks
