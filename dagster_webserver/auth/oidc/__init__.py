"""OIDC support for dagster-webserver.

Public API
----------
- ``OIDCProviderConfig`` — dataclass holding an OIDC provider's configuration
- ``OIDCClient`` — async client for OIDC discovery, token exchange, and ID token verification
"""  # noqa: D205, D400

from dagster_webserver.auth.oidc.client import OIDCClient
from dagster_webserver.auth.oidc.models import OIDCProviderConfig

__all__ = [
    "OIDCClient",
    "OIDCProviderConfig",
]
