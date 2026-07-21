"""Authentication providers.

Provides:
- ``AuthConfig`` — configuration dataclass for auth behavior
- ``BaseAuthProvider`` — abstract base class for custom auth providers
- ``SessionAuthProvider`` — session-based username/password auth
- ``ApiKeyAuthProvider`` — Bearer token auth for programmatic access
"""

from __future__ import annotations

import logging
import secrets
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from dagster._core.workspace.permissions import PermissionResult
from starlette.requests import Request

if TYPE_CHECKING:
    from starlette.responses import RedirectResponse

    from dagster_webserver.auth.oidc.models import OIDCProviderConfig

from dagster_webserver.auth.roles import (
    Role,
    get_custom_permissions,
    get_role_permissions,
)
from dagster_webserver.auth.users import AuthUser, UserBackend

logger = logging.getLogger("dagster-webserver.auth")


@dataclass
class AuthConfig:
    """Configuration for the auth system.

    Attributes:
        login_path: URL path for the login endpoint.
        logout_path: URL path for the logout endpoint.
        allowed_routes: Route names or paths that bypass authentication.
        session_max_age: Session lifetime in seconds (default 24 hours).
        default_role: Fallback role when a user has no explicit role.
        require_auth: Whether to enforce authentication (default True).
    """

    login_path: str = "/login"
    logout_path: str = "/logout"
    allowed_routes: list[str] = field(default_factory=list)
    session_max_age: int = 86400
    default_role: str = Role.VIEWER.value
    require_auth: bool = True


class BaseAuthProvider(ABC):
    """Abstract authentication provider.

    Subclass this to integrate custom authentication mechanisms
    (OAuth2, LDAP, SAML, etc.).

    The key contract is ``authenticate_request`` which is called by
    the middleware on every request.
    """

    def __init__(self, config: AuthConfig | None = None) -> None:
        self.config = config or AuthConfig()

    @abstractmethod
    async def authenticate_request(self, request: Request) -> AuthUser | None:
        """Check if the request is authenticated.

        If authenticated, set ``request.state.user`` and return the user.
        If not, return ``None``.
        """
        ...

    def get_user_permissions(self, user: AuthUser) -> dict[str, PermissionResult]:
        """Resolve a user's permissions from their role.

        Override this for custom permission resolution logic.
        """
        if user.role == "custom" and user.custom_permissions:
            return get_custom_permissions(user.custom_permissions)
        try:
            role = Role(user.role)
            return get_role_permissions(role)
        except ValueError:
            logger.warning(
                "Unknown role '%s' for user '%s', falling back to VIEWER permissions",
                user.role,
                user.username,
            )
            return get_role_permissions(Role.VIEWER)


class SessionAuthProvider(BaseAuthProvider):
    """Session-based authentication with username/password login.

    Uses Starlette's session middleware (``request.session``) to
    persist the authenticated user across requests.
    """

    def __init__(
        self,
        user_backend: UserBackend,
        config: AuthConfig | None = None,
    ) -> None:
        super().__init__(config)
        self._user_backend = user_backend

    async def authenticate_request(self, request: Request) -> AuthUser | None:
        session: dict = getattr(request, "session", {}) or {}
        username = session.get("username")
        if not username:
            return None
        user = await self._user_backend.get_user(username)
        if user:
            request.state.user = user
            return user
        return None

    async def login(
        self, username: str, password: str, request: Request
    ) -> AuthUser | None:
        """Authenticate credentials and create a session.

        Returns the ``AuthUser`` on success, ``None`` on failure.
        """
        user = await self._user_backend.authenticate(username, password)
        if user:
            request.session["username"] = username
            logger.info("User '%s' logged in", username)
            return user
        return None

    async def logout(self, request: Request) -> None:
        """Clear the session."""
        username = request.session.get("username")
        request.session.pop("username", None)
        if username:
            logger.info("User '%s' logged out", username)


class HybridSessionAuthProvider(SessionAuthProvider):
    """Session-based auth supporting both password and OIDC login.

    Extends ``SessionAuthProvider`` with OIDC capabilities.  The
    ``authenticate_request()`` method checks sessions first (works for
    both password and OIDC logins), and additional routes handle the
    OIDC authorization flow.

    Requires a ``DatabaseUserBackend`` (OIDC config is stored in the DB).
    """

    def __init__(
        self,
        user_backend: UserBackend,
        config: AuthConfig | None = None,
    ) -> None:
        super().__init__(user_backend, config)
        from dagster_webserver.auth.db_backend import DatabaseUserBackend

        if not isinstance(user_backend, DatabaseUserBackend):
            raise TypeError("HybridSessionAuthProvider requires a DatabaseUserBackend")
        self._db_backend = user_backend

    async def get_oidc_providers(
        self,
    ) -> list[OIDCProviderConfig]:
        """Return all enabled OIDC providers from the database."""
        from dagster_webserver.auth.oidc.models import OIDCProviderConfig

        providers = await self._db_backend.list_oidc_providers(enabled_only=True)
        return [OIDCProviderConfig.from_orm(p) for p in providers]

    async def initiate_oidc_login(
        self, provider_name: str, request: Request
    ) -> RedirectResponse:
        """Start the OIDC authorization flow for a given provider."""
        from starlette.responses import RedirectResponse
        from starlette.status import HTTP_303_SEE_OTHER

        from dagster_webserver.auth.oidc.client import (
            OIDCClient,
            generate_code_challenge,
            generate_code_verifier,
            generate_state,
        )
        from dagster_webserver.auth.oidc.models import (
            SESSION_OIDC_NONCE,
            SESSION_OIDC_REDIRECT,
            SESSION_OIDC_STATE,
            SESSION_OIDC_VERIFIER,
            OIDCProviderConfig,
        )

        provider_orm = await self._db_backend.get_oidc_provider(provider_name)
        if provider_orm is None or not provider_orm.enabled:
            return RedirectResponse(
                f"{self.config.login_path}?error=provider_not_found",
                status_code=HTTP_303_SEE_OTHER,
            )

        config = OIDCProviderConfig.from_orm(provider_orm)

        # Capture and validate next_url
        next_url = request.query_params.get("next", "/")
        if not _is_safe_redirect(next_url):
            next_url = "/"

        # Generate PKCE + state + nonce
        code_verifier = generate_code_verifier()
        code_challenge = generate_code_challenge(code_verifier)
        state = generate_state(provider_name)
        nonce = secrets.token_urlsafe(32)

        # Store in session
        request.session[SESSION_OIDC_STATE] = state
        request.session[SESSION_OIDC_VERIFIER] = code_verifier
        request.session[SESSION_OIDC_NONCE] = nonce
        request.session[SESSION_OIDC_REDIRECT] = next_url

        # Build redirect URI
        redirect_uri = str(request.url_for("oidc-callback"))

        # Redirect to IdP
        client = OIDCClient(config)
        auth_url = client.get_authorization_url(
            redirect_uri, state, code_challenge, nonce
        )
        return RedirectResponse(auth_url, status_code=302)

    async def handle_oidc_callback(
        self,
        request: Request,
    ) -> AuthUser | None:
        """Handle the OIDC callback after user authenticates at IdP.

        Returns the ``AuthUser`` on success, ``None`` on failure.
        """
        from dagster_webserver.auth.oidc.client import (
            OIDCClient,
            parse_state,
        )
        from dagster_webserver.auth.oidc.models import (
            SESSION_OIDC_NONCE,
            SESSION_OIDC_STATE,
            SESSION_OIDC_VERIFIER,
            OIDCProviderConfig,
        )

        session: dict = getattr(request, "session", {}) or {}

        # Extract code and state
        code = request.query_params.get("code")
        state = request.query_params.get("state")
        if not code or not state:
            return None

        # Verify state (CSRF protection)
        stored_state = session.get(SESSION_OIDC_STATE)
        if stored_state != state:
            return None

        # Parse provider name from state
        try:
            _, provider_name = parse_state(state)
        except ValueError:
            return None

        # Retrieve PKCE verifier and nonce from session
        code_verifier = session.get(SESSION_OIDC_VERIFIER)
        nonce = session.get(SESSION_OIDC_NONCE)
        if not code_verifier:
            return None

        # Look up provider config
        provider_orm = await self._db_backend.get_oidc_provider(provider_name)
        if provider_orm is None:
            return None
        config = OIDCProviderConfig.from_orm(provider_orm)

        # Exchange code for tokens
        try:
            client = OIDCClient(config)
            redirect_uri = str(request.url_for("oidc-callback"))
            tokens = await client.exchange_code(code, redirect_uri, code_verifier)
        except RuntimeError:
            return None

        # Verify ID token
        try:
            claims = await client.verify_id_token(tokens["id_token"], nonce=nonce)
        except ValueError:
            return None

        # Extract claims
        sub = claims.get("sub")
        email = claims.get("email")
        name = claims.get("name")
        if not sub:
            return None

        # Link or create user
        user = await self._link_or_create_user(config, sub, email, name)

        # Set session
        request.session["username"] = user.username

        # Clear OIDC flow state
        for key in (
            SESSION_OIDC_STATE,
            SESSION_OIDC_VERIFIER,
            SESSION_OIDC_NONCE,
            "oidc_redirect",
        ):
            request.session.pop(key, None)

        logger.info("User '%s' logged in via OIDC (%s)", user.username, provider_name)
        return user

    async def _link_or_create_user(
        self,
        provider: OIDCProviderConfig,
        oidc_sub: str,
        email: str | None,
        name: str | None,
    ) -> AuthUser:
        """Link OIDC identity to existing user or create new user."""
        # 1. Check for existing OIDC linkage
        existing = await self._db_backend.get_user_by_oidc(provider.id, oidc_sub)
        if existing:
            return existing

        # 2. Check for email match (account linking)
        if email:
            email_user = await self._db_backend.get_user_by_email(email)
            if email_user:
                return await self._db_backend.link_oidc_to_user(
                    email_user.username, provider.id, oidc_sub
                )

        # 3. Create new user
        username = email or f"oidc_{provider.name}_{oidc_sub}"
        return await self._db_backend.create_oidc_user(
            username=username,
            provider_id=provider.id,
            oidc_sub=oidc_sub,
            email=email,
            display_name=name,
            role=self.config.default_role,
        )


def _is_safe_redirect(url: str) -> bool:
    """Check that *url* is a safe relative redirect target.

    Must start with ``/`` and must not contain ``//`` or a protocol scheme.
    """
    if not url or not url.startswith("/"):
        return False
    if "//" in url:
        return False
    if ":/" in url:
        return False
    return True


class ApiKeyAuthProvider(BaseAuthProvider):
    """Bearer token authentication for programmatic/API access.

    Expects ``Authorization: Bearer <token>`` header.
    Tokens are looked up via the user backend.
    """

    def __init__(
        self,
        user_backend: UserBackend,
        config: AuthConfig | None = None,
    ) -> None:
        super().__init__(config)
        self._user_backend = user_backend

    async def authenticate_request(self, request: Request) -> AuthUser | None:
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return None

        token = auth_header[7:].strip()
        if not token:
            return None

        # Try to find a user whose API token matches
        user = await self._user_backend.get_user(token)
        if user:
            request.state.user = user
            return user
        return None
