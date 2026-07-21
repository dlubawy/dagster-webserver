"""Authentication and RBAC for dagster-webserver.

Public API
----------
Roles
~~~~~
- ``Role`` — enum of built-in roles (CATALOG_VIEWER, VIEWER, LAUNCHER, EDITOR, ADMIN)
- ``get_role_permissions()`` — map a Role to Dagster PermissionResult dict
- ``get_custom_permissions()`` — build PermissionResult from arbitrary permission dict

Users
~~~~~
- ``AuthUser`` — frozen dataclass representing an authenticated user
- ``UserBackend`` — abstract base class for credential/user storage
- ``InMemoryUserBackend`` — in-memory user store
- ``FileUserBackend`` — YAML/JSON file-based store
- ``DatabaseUserBackend`` — relational database user store (SQLite / PostgreSQL)

Providers
~~~~~~~~~
- ``AuthConfig`` — configuration dataclass for auth behavior
- ``BaseAuthProvider`` — abstract base class for custom auth providers
- ``SessionAuthProvider`` — session-based username/password auth
- ``ApiKeyAuthProvider`` — Bearer token auth for programmatic access

Middleware
~~~~~~~~~~
- ``AuthMiddleware`` — Starlette middleware that enforces authentication

Routes
~~~~~~
- ``login_endpoint`` — ``/login`` GET/POST handler
- ``logout_endpoint`` — ``/logout`` GET handler
- ``me_endpoint`` — ``/api/me`` GET handler

Context
~~~~~~~
- ``AuthenticatedWorkspaceRequestContext`` — request context with per-user permissions
- ``AuthenticatedWorkspaceProcessContext`` — process context that creates authenticated request contexts
"""  # noqa: D205, D400

from dagster_webserver.auth.context import (
    AuthenticatedWorkspaceProcessContext,
    AuthenticatedWorkspaceRequestContext,
)
from dagster_webserver.auth.db_backend import DatabaseUserBackend
from dagster_webserver.auth.middleware import AuthMiddleware
from dagster_webserver.auth.provider import (
    ApiKeyAuthProvider,
    AuthConfig,
    BaseAuthProvider,
    HybridSessionAuthProvider,
    SessionAuthProvider,
)
from dagster_webserver.auth.roles import (
    Role,
    get_custom_permissions,
    get_role_permissions,
)
from dagster_webserver.auth.routes import login_endpoint, logout_endpoint, me_endpoint
from dagster_webserver.auth.users import (
    AuthUser,
    FileUserBackend,
    InMemoryUserBackend,
    UserBackend,
)

__all__ = [
    # Roles
    "Role",
    "get_custom_permissions",
    "get_role_permissions",
    # Users
    "AuthUser",
    "UserBackend",
    "InMemoryUserBackend",
    "FileUserBackend",
    "DatabaseUserBackend",
    # Providers
    "AuthConfig",
    "BaseAuthProvider",
    "SessionAuthProvider",
    "HybridSessionAuthProvider",
    "ApiKeyAuthProvider",
    # Middleware
    "AuthMiddleware",
    # Routes
    "login_endpoint",
    "logout_endpoint",
    "me_endpoint",
    # Context
    "AuthenticatedWorkspaceProcessContext",
    "AuthenticatedWorkspaceRequestContext",
]
