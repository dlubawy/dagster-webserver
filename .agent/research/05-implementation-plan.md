# Implementation Plan: User Login with RBAC for Dagster Webserver

## Summary

This document outlines a concrete implementation plan for adding user authentication and role-based access control to `dagster-webserver`. The design leverages:

- **Dagster's existing permission infrastructure** (`BaseWorkspaceRequestContext.permissions`, `has_permission()`, `PermissionResult`)
- **Starlette Admin's auth pattern** (`BaseAuthProvider` → `AuthMiddleware` → `request.state.user`)
- **Dagster+ cloud role model** (`CATALOG_VIEWER`, `VIEWER`, `LAUNCHER`, `EDITOR`, `ADMIN`, `CUSTOM`)
- **Starlette ecosystem packages** (`starsessions` for sessions, `Authlib` for OAuth2 if needed later)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     HTTP Request                            │
└────────────────────────┬────────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  AuthMiddleware (new)                                        │
│  - Check session cookie / API token / header                │
│  - Resolve user identity                                    │
│  - Set request.state.user (AuthUser dataclass)              │
│  - Redirect to /login or return 401 if not authenticated    │
└────────────────────────┬────────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  DagsterTracedCounterMiddleware (existing)                   │
└────────────────────────┬────────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Route Handler                                               │
│  - _make_request_context(conn) → injects user into context  │
│  - WorkspaceRequestContext.permissions → per-user perms     │
└────────────────────────┬────────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  GraphQL Resolver                                            │
│  - graphene_info.context.has_permission(Permissions.XXX)    │
│  - Returns PermissionResult(enabled=True/False)             │
└─────────────────────────────────────────────────────────────┘
```

## New Files to Create

```
dagster_webserver/
├── auth/
│   ├── __init__.py              # Public API exports
│   ├── provider.py              # BaseAuthProvider, AuthProvider, AuthUser
│   ├── middleware.py            # AuthMiddleware (Starlette BaseHTTPMiddleware)
│   ├── roles.py                 # Role definitions and permission maps
│   ├── users.py                 # User storage backends (file, dict, abstract)
│   └── routes.py                # /login, /logout, /api/me endpoint handlers
```

## Modified Files

```
dagster_webserver/
├── app.py              # Accept auth_provider parameter
├── webserver.py        # Inject auth middleware, routes, _make_request_context override
├── cli.py              # Add --auth-provider, --users-file, --default-role CLI options
└── graphql.py          # No changes needed (uses context.permissions)
```

## Module-by-Module Design

### 1. `dagster_webserver/auth/roles.py`

```python
from enum import Enum, unique
from dagster._core.workspace.permissions import Permissions, PermissionResult

@unique
class Role(str, Enum):
    CATALOG_VIEWER = "catalog_viewer"
    VIEWER = "viewer"
    LAUNCHER = "launcher"
    EDITOR = "editor"
    ADMIN = "admin"

# Permission maps per role
ROLE_PERMISSIONS: dict[Role, dict[Permissions, bool]] = {
    Role.CATALOG_VIEWER: {p: False for p in Permissions},
    Role.VIEWER: {p: False for p in Permissions},
    Role.LAUNCHER: {
        **{p: False for p in Permissions},
        Permissions.LAUNCH_PIPELINE_EXECUTION: True,
        Permissions.LAUNCH_PIPELINE_REEXECUTION: True,
        Permissions.TERMINATE_PIPELINE_EXECUTION: True,
        Permissions.LAUNCH_PARTITION_BACKFILL: True,
        Permissions.CANCEL_PARTITION_BACKFILL: True,
    },
    Role.EDITOR: {p: True for p in Permissions},
    Role.ADMIN: {p: True for p in Permissions},
}

def get_role_permissions(role: Role) -> dict[str, PermissionResult]:
    """Convert role enum to PermissionResult map compatible with WorkspaceRequestContext."""
    perm_map = ROLE_PERMISSIONS[role]
    return {
        perm: PermissionResult(
            enabled=enabled,
            disabled_reason=None if enabled else f"Requires {role.value} role or higher"
        )
        for perm, enabled in perm_map.items()
    }

def get_custom_permissions(perm_map: dict[str, bool]) -> dict[str, PermissionResult]:
    """Build PermissionResult map from arbitrary permission dict."""
    return {
        perm: PermissionResult(
            enabled=enabled,
            disabled_reason=None if enabled else "Disabled by your role configuration"
        )
        for perm, enabled in perm_map.items()
    }
```

### 2. `dagster_webserver/auth/users.py`

```python
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Protocol

@dataclass(frozen=True)
class AuthUser:
    """Represents an authenticated user with a role assignment."""
    username: str
    role: str  # Role enum value or "custom"
    custom_permissions: dict[str, bool] | None = None
    email: str | None = None
    display_name: str | None = None

class UserBackend(ABC):
    """Abstract user storage backend."""

    @abstractmethod
    async def authenticate(self, username: str, password: str) -> AuthUser | None:
        """Verify credentials and return user, or None if invalid."""
        ...

    @abstractmethod
    async def get_user(self, username: str) -> AuthUser | None:
        """Look up user by username."""
        ...

@dataclass
class InMemoryUserBackend(UserBackend):
    """Simple in-memory user store. Users defined at startup."""
    # {username: {password_hash, role, email, display_name, custom_permissions}}
    _users: dict[str, dict]

    def __post_init__(self):
        # Hash passwords using bcrypt
        ...

    async def authenticate(self, username: str, password: str) -> AuthUser | None:
        user_data = self._users.get(username)
        if not user_data:
            return None
        if bcrypt.checkpw(password, user_data["password_hash"]):
            return AuthUser(
                username=username,
                role=user_data["role"],
                custom_permissions=user_data.get("custom_permissions"),
                email=user_data.get("email"),
                display_name=user_data.get("display_name"),
            )
        return None

class FileUserBackend(UserBackend):
    """YAML/JSON file-based user store."""
    _file_path: str
    _users: dict[str, dict]  # Cached

    def __init__(self, file_path: str):
        self._file_path = file_path
        self._users = self._load_users()
```

### 3. `dagster_webserver/auth/provider.py`

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from dagster_webserver.auth.users import AuthUser, UserBackend
from dagster_webserver.auth.roles import get_role_permissions, get_custom_permissions, Role
from starlette.requests import Request
from starlette.responses import Response

@dataclass
class AuthConfig:
    """Configuration for the auth system."""
    login_path: str = "/login"
    logout_path: str = "/logout"
    allowed_routes: list[str] = None  # Routes that bypass auth
    session_secret: str | None = None  # For signing session cookies
    session_max_age: int = 86400  # 24 hours default
    default_role: str = Role.VIEWER.value
    require_auth: bool = True  # Whether to enforce auth at all

class BaseAuthProvider(ABC):
    """Abstract auth provider. Subclass to add custom authentication."""

    def __init__(self, config: AuthConfig | None = None):
        self.config = config or AuthConfig()

    @abstractmethod
    async def authenticate_request(self, request: Request) -> AuthUser | None:
        """Check if request is authenticated. Set request.state.user if so."""
        ...

    async def get_user_permissions(self, user: AuthUser) -> dict[str, "PermissionResult"]:
        """Resolve user's permissions from their role."""
        if user.role == "custom" and user.custom_permissions:
            return get_custom_permissions(user.custom_permissions)
        try:
            role = Role(user.role)
            return get_role_permissions(role)
        except ValueError:
            return get_role_permissions(Role.VIEWER)  # Default fallback

class SessionAuthProvider(BaseAuthProvider):
    """Session-based auth with username/password login."""

    def __init__(self, user_backend: UserBackend, config: AuthConfig | None = None):
        super().__init__(config)
        self._user_backend = user_backend

    async def authenticate_request(self, request: Request) -> AuthUser | None:
        session = request.session
        username = session.get("username")
        if not username:
            return None
        user = await self._user_backend.get_user(username)
        if user:
            request.state.user = user
            return user
        return None

    async def login(self, username: str, password: str, request: Request) -> AuthUser | None:
        user = await self._user_backend.authenticate(username, password)
        if user:
            request.session["username"] = username
            return user
        return None

    async def logout(self, request: Request):
        request.session.pop("username", None)

class ApiKeyAuthProvider(BaseAuthProvider):
    """API key / Bearer token auth for programmatic access."""

    def __init__(self, user_backend: UserBackend, config: AuthConfig | None = None):
        super().__init__(config)
        self._user_backend = user_backend

    async def authenticate_request(self, request: Request) -> AuthUser | None:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            user = await self._user_backend.get_user_by_token(token)
            if user:
                request.state.user = user
                return user
        return None
```

### 4. `dagster_webserver/auth/middleware.py`

```python
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import RedirectResponse, JSONResponse, Response
from starlette.routing import Match, Route, WebSocketRoute
from starlette.status import HTTP_303_SEE_OTHER, HTTP_401_UNAUTHORIZED
from dagster_webserver.auth.provider import BaseAuthProvider

class AuthMiddleware(BaseHTTPMiddleware):
    """Middleware that enforces authentication on all routes except allowed ones."""

    def __init__(self, app, provider: BaseAuthProvider):
        super().__init__(app)
        self.provider = provider

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Check if route is allowed without auth
        if self._is_allowed_without_auth(request):
            return await call_next(request)

        # Try to authenticate
        user = await self.provider.authenticate_request(request)

        if user:
            return await call_next(request)

        # Not authenticated
        if self._is_api_request(request):
            return JSONResponse(
                {"error": "Authentication required"},
                status_code=HTTP_401_UNAUTHORIZED,
            )
        else:
            # Browser request — redirect to login
            login_url = f"{self.provider.config.login_path}?next={request.url.path}"
            return RedirectResponse(login_url, status_code=HTTP_303_SEE_OTHER)

    def _is_allowed_without_auth(self, request: Request) -> bool:
        allowed = self.provider.config.allowed_routes or []
        for route in request.app.routes:
            match, _ = route.matches(request.scope)
            if match == Match.FULL and isinstance(route, (Route, WebSocketRoute)):
                if route.name in allowed:
                    return True
                if route.path in allowed:
                    return True
                break
        return False

    def _is_api_request(self, request: Request) -> bool:
        accept = request.headers.get("Accept", "")
        return "application/json" in accept or request.url.path.startswith("/graphql")
```

### 5. `dagster_webserver/auth/routes.py`

```python
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.routing import Route
from starlette.status import HTTP_303_SEE_OTHER, HTTP_400_BAD_REQUEST

async def login_endpoint(request: Request) -> Response:
    """Handle login: GET shows form (or SPA redirect), POST authenticates."""
    provider = request.app.state.auth_provider

    if request.method == "GET":
        # For SPA: redirect to login page with auth_required flag
        # For API: return 401
        if "text/html" in request.headers.get("Accept", ""):
            return HTMLResponse(_LOGIN_TEMPLATE, status_code=HTTP_401_UNAUTHORIZED)
        return JSONResponse({"error": "Authentication required"}, status_code=HTTP_401_UNAUTHORIZED)

    form = await request.form()
    username = form.get("username", "")
    password = form.get("password", "")

    user = await provider.login(username, password, request)
    if not user:
        return JSONResponse(
            {"error": "Invalid username or password"},
            status_code=HTTP_400_BAD_REQUEST,
        )

    next_url = request.query_params.get("next", "/")
    return RedirectResponse(next_url, status_code=HTTP_303_SEE_OTHER)

async def logout_endpoint(request: Request) -> Response:
    """Handle logout: clear session, redirect to login."""
    provider = request.app.state.auth_provider
    await provider.logout(request)
    return RedirectResponse(provider.config.login_path, status_code=HTTP_303_SEE_OTHER)

async def me_endpoint(request: Request) -> Response:
    """Return current user info for UI."""
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=HTTP_401_UNAUTHORIZED)
    return JSONResponse({
        "username": user.username,
        "role": user.role,
        "email": user.email,
        "displayName": user.display_name,
    })
```

### 6. Modifications to `dagster_webserver/webserver.py`

```python
class DagsterWebserver(...):
    def __init__(self, process_context, app_path_prefix="", live_data_poll_rate=None,
                 uses_app_path_prefix=True, auth_provider=None):
        ...
        self._auth_provider = auth_provider
        ...

    def _make_request_context(self, conn: HTTPConnection) -> TRequestContext:
        # Inject user identity from request state into the request context
        user = getattr(conn.state, "user", None)
        return self._process_context.create_request_context(conn, user=user)

    def build_middleware(self) -> list[Middleware]:
        middlewares = [Middleware(DagsterTracedCounterMiddleware)]
        if self._auth_provider:
            middlewares.append(Middleware(AuthMiddleware, provider=self._auth_provider))
        return middlewares

    def build_routes(self):
        routes = [...]  # existing routes
        if self._auth_provider:
            routes.insert(0, Route("/login", login_endpoint, methods=["GET", "POST"], name="login"))
            routes.insert(0, Route("/logout", logout_endpoint, methods=["GET"], name="logout"))
            routes.insert(0, Route("/api/me", me_endpoint, methods=["GET"], name="api-me"))
        ...
```

### 7. Modifications to `dagster_webserver/app.py`

```python
def create_app_from_workspace_process_context(
    workspace_process_context: IWorkspaceProcessContext,
    path_prefix: str = "",
    live_data_poll_rate: int | None = None,
    auth_provider: BaseAuthProvider | None = None,
    **kwargs,
) -> Starlette:
    ...
    return DagsterWebserver(
        workspace_process_context,
        path_prefix,
        live_data_poll_rate,
        auth_provider=auth_provider,
    ).create_asgi_app(**kwargs)
```

### 8. Modifications to `dagster_webserver/cli.py`

```python
@click.option(
    "--auth-provider",
    type=click.Choice(["session", "api-key", "none"]),
    default="none",
    help="Authentication provider to use.",
)
@click.option(
    "--users-file",
    type=click.Path(exists=False),
    help="Path to YAML/JSON file defining users and roles.",
)
@click.option(
    "--session-secret",
    type=click.STRING,
    envvar="DAGSTER_WEBSERVER_SESSION_SECRET",
    help="Secret key for signing session cookies.",
)
@click.option(
    "--default-role",
    type=click.Choice(["catalog_viewer", "viewer", "launcher", "editor", "admin"]),
    default="viewer",
    help="Default role for users when auth is enabled but no role is assigned.",
)
```

### 9. Custom `WorkspaceRequestContext` Subclass

The key integration point — override `permissions` to return per-user permissions:

```python
class AuthenticatedWorkspaceRequestContext(WorkspaceRequestContext):
    def __init__(self, *, user: AuthUser | None = None, **kwargs):
        super().__init__(**kwargs)
        self._user = user
        self._auth_provider = kwargs.get("_auth_provider")

    @property
    def permissions(self) -> Mapping[str, PermissionResult]:
        if self._user and self._auth_provider:
            return self._auth_provider.get_user_permissions(self._user)
        return super().permissions  # Fall back to global read_only behavior

    @property
    def user(self) -> AuthUser | None:
        return self._user
```

## Session Management

### Dependencies to Add

```toml
# pyproject.toml (optional extras)
[project.optional-dependencies]
auth = [
    "starsessions>=2.2.0",   # Server-side sessions
    "bcrypt>=4.0.0",          # Password hashing
]
auth-redis = [
    "starsessions[redis]>=2.2.0",
    "bcrypt>=4.0.0",
]
```

### Session Middleware

```python
from starsessions import SessionMiddleware, CookieSessionInterface

# In CLI or app creation:
if auth_provider:
    session_interface = CookieSessionInterface(
        secret_key=session_secret,
        max_age=86400,  # 24 hours
        secure=True,    # HTTPS only
        httponly=True,
        samesite="lax",
    )
    app.add_middleware(SessionMiddleware, session_interface=session_interface)
```

## Backward Compatibility

1. **Auth is opt-in**: `--auth-provider none` (default) — no auth, existing behavior
1. **No changes to existing APIs**: GraphQL schema, routes, and context interface unchanged
1. **`WorkspaceRequestContext.permissions`** still returns `Mapping[str, PermissionResult]`
1. **`has_permission()`** still works the same way
1. **`read_only` flag** still works as a fallback when auth is disabled

## Testing Strategy

1. **Unit tests** for role permission maps
1. **Unit tests** for user backend (in-memory, file-based)
1. **Integration tests** for auth middleware (authenticated/unauthenticated requests)
1. **Integration tests** for login/logout flow
1. **Integration tests** for GraphQL permission enforcement with different roles
1. **Test fixtures** in `dagster_webserver_tests/conftest.py` with auth-enabled app

## Future Extensions

1. **OAuth2/OIDC**: Add `OAuth2AuthProvider` using Authlib
1. **LDAP/Active Directory**: Add `LDAPUserBackend`
1. **API Tokens**: Add token generation/management via GraphQL mutations
1. **Audit Logging**: Log login/logout/permission-denied events
1. **Per-location roles**: Extend `permissions_for_location()` to use user's role
1. **Owner-based permissions**: Implement `permissions_for_owner()` for asset ownership
1. **SAML/SCIM**: Enterprise SSO integration
1. **UI Login Page**: Replace HTML template with proper React login component
