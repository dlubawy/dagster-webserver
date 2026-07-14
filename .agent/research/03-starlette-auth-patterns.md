# Starlette Authentication Patterns & Third-Party Ecosystem

## Starlette's Auth Philosophy

Starlette intentionally does **not** include built-in authentication. It provides the primitives (middleware, sessions, cookies) and leaves auth to third-party packages. This matches the dagster-webserver's current approach — no auth at all, with hooks to add it.

## Starlette Admin Auth Pattern (`starlette-admin`)

### Architecture

Starlette-admin provides a clean, composable auth pattern via `BaseAuthProvider`:

```python
class BaseAuthProvider(ABC):
    def __init__(self, login_path="/login", logout_path="/logout",
                 allow_paths=None, allow_routes=None):
        ...

    @abstractmethod
    def setup_admin(self, admin: "BaseAdmin") -> None: ...

    async def is_authenticated(self, request: Request) -> bool:
        """Validate each incoming request. Save user in request.state.user."""
        return False

    def get_middleware(self, admin: "BaseAdmin") -> Middleware:
        return Middleware(AuthMiddleware, provider=self)

    def get_admin_user(self, request: Request) -> Optional[AdminUser]: ...
    def get_admin_config(self, request: Request) -> Optional[AdminConfig]: ...
```

### AuthProvider (Concrete — Username/Password)

```python
class AuthProvider(BaseAuthProvider):
    async def login(self, username, password, remember_me, request, response):
        """Validate credentials. Save session. Return redirect or raise LoginFailed."""

    async def logout(self, request, response):
        """Clear session. Return response."""

    def setup_admin(self, admin):
        """Inject middleware + login/logout routes into the admin app."""
        admin.middlewares.append(self.get_middleware(admin))
        admin.routes.extend([self.get_login_route(admin), self.get_logout_route(admin)])
```

### AuthMiddleware

```python
class AuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, provider, allow_paths=None, allow_routes=None):
        self.provider = provider
        self.allow_routes = ["login", "statics", ...]

    async def dispatch(self, request, call_next):
        # Check if route is allowed without auth
        if (route in allow_paths or route_name in allow_routes
            or has @login_not_required decorator
            or await provider.is_authenticated(request)):
            return await call_next(request)
        # Redirect to login with ?next=original_url
        return RedirectResponse(f"/login?next={request.url}", 303)
```

### Key Pattern: `request.state.user`

The auth middleware saves the authenticated user on `request.state.user`, which is then accessible in:

- Route handlers
- `get_admin_user(request)` for UI display
- `get_admin_config(request)` for personalized config

### `@login_not_required` Decorator

```python
def login_not_required(endpoint):
    endpoint._login_not_required = True
    return endpoint
```

Applied to endpoints that should bypass auth (health checks, static assets, etc.).

## Starlette Third-Party Auth Packages

### starsessions (`alex-oleshkevich/starsessions`)

- Server-side sessions with Redis, Memcached, or in-memory backends
- Provides `SessionMiddleware` for Starlette
- Pattern: `request.session["user_id"]` → look up user → set `request.state.user`

### Authlib (`lepture/Authlib`)

- OAuth 1.0a, OAuth 2.0, OpenID Connect client
- Starlette integration: `OAuth2Client` with Starlette adapter
- Pattern: Redirect to provider → callback → save token in session

### Starlette-Login (`jockerz/Starlette-Login`)

- Flask-Login-style pattern for Starlette
- Provides `login_user()`, `logout_user()`, `current_user` proxy
- Session-based with configurable user loader

### Authlib Starlette Client

- Full OAuth2/OIDC flow for Starlette
- Supports: Authorization Code, PKCE, Client Credentials, Device Flow
- Token management with refresh

## Session Management Options

### Option 1: Server-Side Sessions (Recommended for simple deployments)

- Package: `starsessions` or Starlette's built-in `SessionMiddleware`
- Storage: Redis (production), file-based (dev)
- Pattern: session cookie → server-side session store → user lookup

### Option 2: JWT Tokens

- Package: `python-jose` or `PyJWT`
- Storage: Stateless (token contains all claims)
- Pattern: Authorization header or cookie → decode/verify JWT → extract user

### Option 3: OAuth2/OIDC

- Package: `Authlib`
- Pattern: Redirect to IdP → callback → save tokens → use tokens for API calls

### Option 4: API Key / Bearer Token

- Simple header-based auth
- Pattern: `Authorization: Bearer <token>` → look up token → resolve user

## Recommended Approach for Dagster Webserver

### Authentication Layer

```
HTTP Request
  → AuthMiddleware (new)
    → Check session/JWT/header for user identity
    → If authenticated: set request.state.user
    → If not authenticated: redirect to /login (for browser) or 401 (for API)
  → Existing middleware stack
  → Route handler
```

### Key Design Decisions

1. **Session-based auth** for browser UI (login form → session cookie)
1. **API key / Bearer token** for programmatic access (GraphQL, REST endpoints)
1. **Pluggable auth provider** pattern (like starlette-admin's `BaseAuthProvider`)
1. **Graceful degradation** — auth is opt-in, disabled by default (backward compatible)

### Middleware Integration

```python
# In DagsterWebserver.build_middleware():
def build_middleware(self) -> list[Middleware]:
    middlewares = [Middleware(DagsterTracedCounterMiddleware)]
    if self._auth_provider:
        middlewares.append(Middleware(AuthMiddleware, provider=self._auth_provider))
    return middlewares
```

### Request Context Integration

```python
# Override _make_request_context to inject user identity:
def _make_request_context(self, conn: HTTPConnection) -> TRequestContext:
    user = getattr(conn.state, "user", None)
    return self._process_context.create_request_context(conn, user=user)
```

### Routes to Add

- `POST /login` — authenticate user, set session
- `POST /logout` — clear session
- `GET /login` — render login page (or redirect for SPA)
- `GET /api/me` — return current user info (for UI)

### Routes to Allow Without Auth

- Static assets (`/assets/*`, `/favicon.png`)
- Health check (`/server_info` — or make it auth-gated)
- Login/logout pages themselves
- CSP header file (`/csp-header.txt`)
