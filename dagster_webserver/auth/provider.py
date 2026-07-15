"""Authentication providers.

Provides:
- ``AuthConfig`` — configuration dataclass for auth behavior
- ``BaseAuthProvider`` — abstract base class for custom auth providers
- ``SessionAuthProvider`` — session-based username/password auth
- ``ApiKeyAuthProvider`` — Bearer token auth for programmatic access
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from dagster._core.workspace.permissions import PermissionResult
from starlette.requests import Request

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
