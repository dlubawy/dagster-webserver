"""OIDC provider configuration dataclass and session key constants."""

from __future__ import annotations

from dataclasses import dataclass

# Session keys used during the OIDC authorization flow
SESSION_OIDC_STATE = "oidc_state"
SESSION_OIDC_VERIFIER = "oidc_verifier"
SESSION_OIDC_NONCE = "oidc_nonce"
SESSION_OIDC_REDIRECT = "oidc_redirect"


@dataclass(frozen=True)
class OIDCProviderConfig:
    """Immutable snapshot of an OIDC provider configuration.

    Built from the ORM ``OIDCProvider`` model and passed to the
    ``OIDCClient`` and ``HybridSessionAuthProvider``.
    """

    id: int
    name: str
    display_name: str
    issuer_url: str
    client_id: str
    client_secret: str
    scopes: str
    enabled: bool
    display_order: int

    @classmethod
    def from_orm(cls, provider: object) -> OIDCProviderConfig:  # type: ignore[override]
        """Build from an ORM ``OIDCProvider`` instance."""
        return cls(
            id=provider.id,
            name=provider.name,
            display_name=provider.display_name,
            issuer_url=provider.issuer_url,
            client_id=provider.client_id,
            client_secret=provider.client_secret,
            scopes=provider.scopes,
            enabled=provider.enabled,
            display_order=provider.display_order,
        )
