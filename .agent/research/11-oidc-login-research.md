# OIDC Login Capabilities — Research & Architecture Design

> **Date:** 2026-07-19
> **Scope:** Adding OpenID Connect (OIDC) login capabilities to dagster-webserver, limited to the database auth provider. Multiple OIDC configurations supported. Admin-managed via the admin portal.

______________________________________________________________________

## 1. Problem Statement

The existing auth system supports username/password login via `SessionAuthProvider` with a `DatabaseUserBackend`. Organizations that use identity providers (Google, Okta, Auth0, Azure AD, Keycloak, etc.) need to authenticate users via **OpenID Connect (OIDC)** — the standard OAuth 2.0-based identity layer.

Requirements:

1. **Generic OIDC support** — works with any standards-compliant OIDC provider (Google, Okta, Auth0, Azure AD, Keycloak, etc.)
1. **Multiple configurations** — e.g., Google AND Okta simultaneously, each with its own client ID, secret, and settings
1. **Admin-managed** — an admin with appropriate permissions can add/edit/delete OIDC configurations through the admin portal
1. **Database-backed** — all OIDC configuration details stored in the auth database alongside users and roles
1. **Dynamic login form** — the `/login` page renders OIDC provider buttons dynamically based on what's configured. If Google and Okta are configured, the login form shows both "Sign in with Google" and "Sign in with Okta" buttons plus the default username/password form
1. **User provisioning** — first-time OIDC users are automatically created in the database with a default role

______________________________________________________________________

## 2. OIDC Protocol Overview

### 2.1 The Flow (Authorization Code + PKCE)

```
┌──────────┐     ┌──────────────┐     ┌──────────────┐
│  Browser  │     │  Dagster WS   │     │   OIDC IdP   │
│           │     │  (Starlette)  │     │  (Google,     │
│  User     │     │               │     │   Okta, etc.) │
└─────┬─────┘     └──────┬───────┘     └──────┬───────┘
      │                  │                    │
      │  1. GET /login   │                    │
      │─────────────────>│                    │
      │  ← HTML with     │                    │
      │     OIDC buttons │                    │
      │                  │                    │
      │  2. Click "Sign  │                    │
      │     in with X"   │                    │
      │─────────────────>│                    │
      │                  │  3. Redirect to    │
      │                  │     IdP authorize  │
      │  ← 302 ──────────────────────────────>│
      │                  │                    │
      │  4. User         │                    │
      │     authenticates│                    │
      │     at IdP       │                    │
      │                  │                    │
      │  5. Redirect     │                    │
      │     back with    │                    │
      │     code         │                    │
      │  ────────────────────────────────────>│
      │                  │                    │
      │  ← 302 to        │                    │
      │     /oidc/callback│                   │
      │─────────────────>│                    │
      │                  │  6. Exchange code  │
      │                  │     for tokens     │
      │                  │  ────────────────────────────>│
      │                  │  ← ID token +     │
      │                  │     access token   │
      │                  │                    │
      │                  │  7. Verify ID      │
      │                  │     token          │
      │                  │  8. Create/login   │
      │                  │     user           │
      │                  │  9. Set session    │
      │  ← 302 to /      │                    │
      │─────────────────>│                    │
      │  10. Logged in   │                    │
      │                  │                    │
```

### 2.2 Why Authorization Code + PKCE

| Flow | Security | Suitable for browsers |
| ------------------------- | ---------------------------- | -------------------------- |
| Implicit | Low (tokens in URL fragment) | ⚠️ Deprecated by OIDC spec |
| Authorization Code | High | ✅ Yes |
| Authorization Code + PKCE | Highest | ✅ Yes (recommended) |
| Client Credentials | N/A (no user) | ❌ Machine-to-machine only |

PKCE (Proof Key for Code Exchange, RFC 7636) prevents authorization code interception attacks. Even though our client is a confidential client (has a secret), PKCE adds defense-in-depth and is required by many providers (e.g., Google).

### 2.3 What We Extract from the ID Token

The OIDC ID token (JWT) contains:

| Claim | Purpose |
| ------- | ------------------------------------------------------- |
| `sub` | Unique subject identifier (used as our `username`) |
| `email` | User's email address |
| `name` | Display name |
| `iss` | Issuer URL (used to match the right OIDC configuration) |
| `aud` | Audience (should match our client ID) |
| `exp` | Expiration time |
| `iat` | Issued at time |
| `nonce` | Nonce value (anti-replay protection) |

We use `sub` as the unique identifier for linking OIDC users to our database `users` table.

______________________________________________________________________

## 3. Database Schema Design

### 3.1 New Table: `oidc_providers`

```sql
CREATE TABLE oidc_providers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(64) UNIQUE NOT NULL,          -- Internal identifier (e.g., "google", "okta")
    display_name VARCHAR(128) NOT NULL,         -- Human-readable name (e.g., "Google", "Okta")
    issuer_url VARCHAR(512) NOT NULL,           -- OIDC issuer (e.g., "https://accounts.google.com")
    client_id VARCHAR(256) NOT NULL,            -- OIDC client ID
    client_secret VARCHAR(1024) NOT NULL,       -- OIDC client secret (stored encrypted/hashed)
    scopes VARCHAR(512) DEFAULT "openid email profile", -- Space-separated scopes
    enabled BOOLEAN DEFAULT TRUE,               -- Toggle to enable/disable without deleting
    display_order INTEGER DEFAULT 0,            -- Sort order for login page buttons
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 3.2 Extended `users` Table

Add a nullable column to link users to their OIDC provider:

```sql
ALTER TABLE users ADD COLUMN oidc_provider_id INTEGER REFERENCES oidc_providers(id);
ALTER TABLE users ADD COLUMN oidc_sub VARCHAR(256);  -- OIDC subject identifier
```

A user can have either:

- `password_hash` set (local password auth), or
- `oidc_provider_id` + `oidc_sub` set (OIDC auth), or
- Both (user can log in either way)

### 3.3 SQLAlchemy ORM Model

```python
# dagster_webserver/database/models.py

class OIDCProvider(Base):
    __tablename__ = "oidc_providers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(128), nullable=False)
    issuer_url: Mapped[str] = mapped_column(String(512), nullable=False)
    client_id: Mapped[str] = mapped_column(String(256), nullable=False)
    client_secret: Mapped[str] = mapped_column(String(1024), nullable=False)
    scopes: Mapped[str] = mapped_column(String(512), default="openid email profile")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    display_order: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    users: Mapped[list["User"]] = relationship("User", back_populates="oidc_provider")


# Extend User model with OIDC linkage
class User(Base):
    # ... existing columns ...
    oidc_provider_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("oidc_providers.id"), nullable=True
    )
    oidc_sub: Mapped[str | None] = mapped_column(String(256), nullable=True)

    oidc_provider: Mapped["OIDCProvider | None"] = relationship(
        "OIDCProvider", back_populates="users"
    )
```

### 3.4 Alembic Migration

A new migration `002_create_oidc_providers.py`:

```python
def upgrade() -> None:
    # Create oidc_providers table
    op.create_table(
        "oidc_providers",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(64), unique=True, nullable=False),
        sa.Column("display_name", sa.String(128), nullable=False),
        sa.Column("issuer_url", sa.String(512), nullable=False),
        sa.Column("client_id", sa.String(256), nullable=False),
        sa.Column("client_secret", sa.String(1024), nullable=False),
        sa.Column("scopes", sa.String(512), server_default="openid email profile"),
        sa.Column("enabled", sa.Boolean, server_default="1"),
        sa.Column("display_order", sa.Integer, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Add OIDC columns to users table
    op.add_column("users", sa.Column("oidc_provider_id", sa.Integer, nullable=True))
    op.add_column("users", sa.Column("oidc_sub", sa.String(256), nullable=True))
    op.create_foreign_key(
        "fk_users_oidc_provider", "users", "oidc_providers",
        ["oidc_provider_id"], ["id"]
    )
    op.create_index("ix_users_oidc_sub", "users", ["oidc_sub"])


def downgrade() -> None:
    op.drop_index("ix_users_oidc_sub")
    op.drop_constraint("fk_users_oidc_provider", "users", type_="foreignkey")
    op.drop_column("users", "oidc_sub")
    op.drop_column("users", "oidc_provider_id")
    op.drop_table("oidc_providers")
```

______________________________________________________________________

## 4. Python Library Selection

### 4.1 Candidate Libraries

| Library | Stars | OIDC Support | Starlette | Async | Notes |
| ------------------- | ----- | --------------------------- | ----------------------------- | ------------------- | ----------------------------------------------------------------------- |
| **Authlib** | 8.5k | Full (RFC 6749, 7519, 7521) | Yes (via `starlette` adapter) | Partial (sync HTTP) | Most feature-complete. Has `OAuth2WebAppClient` with Starlette support. |
| **python-jose** | 3k | JWT only | N/A | N/A | JWT encoding/decoding only. No OIDC discovery. |
| **PyJWT** | 18k | JWT only | N/A | N/A | JWT only. No OIDC discovery or token exchange. |
| **oidc-python** | 1.5k | Full OIDC | No | No | Simpler API but less maintained. |
| **Authlib + httpx** | — | Full | Yes | ✅ | Authlib for OIDC logic + httpx for async HTTP calls. |

### 4.2 Recommended: Authlib + httpx

**Authlib** (`lepture/Authlib`) is the most mature Python OIDC library:

- Implements full OAuth 2.0 (RFC 6749) and OIDC (OpenID Connect Core)
- Supports OIDC discovery (`.well-known/openid-configuration`)
- Has built-in JWT verification with JWKS support
- Provides `OAuth2WebAppClient` which handles the full authorization code flow
- Starlette integration via `StarletteOAuth2App` (though we'll build our own for more control)

**httpx** is used for async HTTP requests (token exchange, JWKS fetching):

```toml
[project.optional-dependencies]
auth-oidc = ["authlib>=1.3.0", "httpx>=0.27.0"]
```

### 4.3 Why Not Use Authlib's Starlette Integration Directly

Authlib provides `StarletteOAuth2App` but it:

- Assumes a single OIDC provider
- Uses its own session management (conflicts with our existing sessions)
- Doesn't integrate with our `UserBackend` or `AuthUser` model
- Doesn't support multiple concurrent providers

We use Authlib's **core classes** (`OAuth2WebAppClient`, `jwt.decode()`, `jwk`) and build our own Starlette integration that fits our architecture.

______________________________________________________________________

## 5. Architecture Design

### 5.1 High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      dagster-webserver                           │
│                                                                  │
│  ┌─────────────────┐  ┌───────────────────┐  ┌───────────────┐  │
│  │  login_endpoint  │  │  OIDC Routes      │  │  AuthProvider │  │
│  │  (GET /login)    │  │  /oidc/authorize  │  │  (hybrid)     │  │
│  │  renders form +  │  │  /oidc/callback   │  │               │  │
│  │  OIDC buttons    │  │                   │  │ authenticate_ │  │
│  │                  │  │  - reads nonce +  │  │ request()     │  │
│  │  - queries DB    │  │    code from      │  │ checks:       │  │
│  │    for enabled   │  │    query params   │  │ 1. session    │  │
│  │    OIDC configs  │  │                   │  │ 2. OIDC flow  │  │
│  │  - renders       │  │  - looks up OIDC  │  │    (state     │  │
│  │    buttons       │  │    config by      │  │    param)     │  │
│  │                  │  │    state param    │  │               │  │
│  └─────────────────┘  │  - exchanges code  │  └───────┬───────┘  │
│                       │    for tokens      │          │          │
│  ┌─────────────────┐  │  - verifies ID     │          │          │
│  │  Admin Portal    │  │    token          │          ▼          │
│  │  /admin/oidc     │  │  - creates/logs   │  ┌───────────────┐  │
│  │  - OIDCProvider  │  │     in user       │  │  UserBackend  │  │
│  │    View          │  │  - sets session   │  │  (Database)   │  │
│  │  - CRUD for      │  │    cookie         │  │               │  │
│  │    OIDC configs  │  │                   │  │  users        │  │
│  │  - requires      │  └───────────────────┘  │  roles        │  │
│  │    ADMIN_EDIT_   │                          │  oidc_configs │  │
│  │    OIDC_PERMS    │                          └───────────────┘  │
│  └─────────────────┘                                              │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │  Database (SQLite/PostgreSQL)                                │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │  │
│  │  │   users      │  │   roles      │  │ oidc_providers   │   │  │
│  │  │  - username  │  │  - name      │  │  - name          │   │  │
│  │  │  - password  │  │  - perms     │  │  - issuer_url    │   │  │
│  │  │  - role_id   │  │  - is_built  │  │  - client_id     │   │  │
│  │  │  - oidc_sub  │  │              │  │  - client_secret  │   │  │
│  │  │  - oidc_prov │  │              │  │  - scopes         │   │  │
│  │  └──────────────┘  └──────────────┘  │  - enabled        │   │  │
│  │                                       │  - display_order  │   │  │
│  │                                       └──────────────────┘   │  │
│  └─────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

### 5.2 Module Layout

```
dagster_webserver/
├── auth/
│   ├── oidc/                          # NEW: OIDC support
│   │   ├── __init__.py               # Public exports
│   │   ├── client.py                 # OIDC client wrapper (Authlib + httpx)
│   │   ├── routes.py                 # /oidc/authorize, /oidc/callback handlers
│   │   └── models.py                 # OIDCProvider dataclass
│   ├── provider.py                   # MODIFIED: HybridSessionAuthProvider
│   ├── routes.py                     # MODIFIED: login_endpoint (OIDC buttons)
│   ├── db_backend.py                 # MODIFIED: OIDC CRUD methods
│   ├── middleware.py                 # MODIFIED: handle OIDC callback state
│   └── ...existing files...
├── admin/
│   ├── views.py                      # MODIFIED: OIDCProviderView added
│   ├── permissions.py                # MODIFIED: ADMIN_EDIT_OIDC permission
│   └── templates/
│       └── oidc.html                 # NEW: OIDC config form template
├── database/
│   ├── models.py                     # MODIFIED: OIDCProvider model, User OIDC cols
│   └── alembic/
│       └── versions/
│           └── 002_create_oidc_providers.py  # NEW migration
└── cli.py                            # MODIFIED: --enable-oidc flag
```

### 5.3 Auth Provider Design: HybridSessionAuthProvider

Instead of replacing `SessionAuthProvider`, we create a **hybrid** provider that supports both password and OIDC login:

```python
class HybridSessionAuthProvider(SessionAuthProvider):
    """Session-based auth supporting both password and OIDC login.

    Extends SessionAuthProvider with OIDC capabilities. The
    authenticate_request() method checks sessions first (works for
    both password and OIDC logins), and additional routes handle
    the OIDC authorization flow.
    """

    def __init__(
        self,
        user_backend: DatabaseUserBackend,
        config: AuthConfig | None = None,
    ) -> None:
        super().__init__(user_backend, config)
        # user_backend must be DatabaseUserBackend for OIDC support
        assert isinstance(user_backend, DatabaseUserBackend)

    async def get_oidc_providers(self) -> list["OIDCProviderConfig"]:
        """Return all enabled OIDC providers from the database."""
        return await self._user_backend.list_oidc_providers(enabled_only=True)

    async def initiate_oidc_login(
        self, provider_name: str, request: Request
    ) -> RedirectResponse:
        """Start the OIDC authorization flow for a given provider.

        1. Look up the OIDC provider config from DB
        2. Generate PKCE code verifier/challenge
        3. Generate state parameter (includes provider name for callback)
        4. Store state + verifier in session
        5. Redirect to provider's authorization endpoint
        """
        ...

    async def handle_oidc_callback(
        self, request: Request
    ) -> AuthUser | None:
        """Handle the OIDC callback after user authenticates at IdP.

        1. Extract state + code from query params
        2. Verify state matches session (anti-CSRF)
        3. Look up provider config from state
        4. Exchange authorization code for tokens (with PKCE)
        5. Verify ID token (signature, issuer, audience, expiry, nonce)
        6. Look up or create user in database
        7. Set session cookie
        8. Return AuthUser
        """
        ...
```

### 5.4 Session State for OIDC Flow

The session stores OIDC flow state to prevent CSRF and code interception:

```python
# Session keys used during OIDC flow
SESSION_OIDC_STATE = "oidc_state"          # Random state string
SESSION_OIDC_VERIFIER = "oidc_verifier"    # PKCE code verifier
SESSION_OIDC_PROVIDER = "oidc_provider"    # Provider name
SESSION_OIDC_NONCE = "oidc_nonce"          # OIDC nonce for ID token verification
SESSION_OIDC_REDIRECT = "oidc_redirect"    # Original URL before OIDC redirect
```

State parameter format: `{random_hex}:{provider_name}`

The callback extracts the provider name from the state to know which configuration to use for token exchange.

______________________________________________________________________

## 6. Routes Design

### 6.1 New Routes

| Route | Method | Purpose |
| --------------------------------- | ------ | ------------------------------------------------------------------------ |
| `/oidc/authorize/{provider_name}` | GET | Initiate OIDC authorization. Redirects to IdP. |
| `/oidc/callback` | GET | OIDC callback handler. Exchanges code, verifies tokens, creates session. |

### 6.2 Route Registration

In `DagsterWebserver._build_auth_routes()`:

```python
def _build_auth_routes(self) -> list[Route]:
    routes = [
        Route("/login", login_endpoint, methods=["GET", "POST"], name="login"),
        Route("/logout", logout_endpoint, methods=["GET", "POST"], name="logout"),
        Route("/api/me", me_endpoint, methods=["GET"], name="api-me"),
    ]
    if self._auth_provider and isinstance(self._auth_provider, HybridSessionAuthProvider):
        from dagster_webserver.auth.oidc.routes import (
            oidc_authorize_endpoint,
            oidc_callback_endpoint,
        )
        routes.extend([
            Route(
                "/oidc/authorize/{provider_name}",
                oidc_authorize_endpoint,
                methods=["GET"],
                name="oidc-authorize",
            ),
            Route(
                "/oidc/callback",
                oidc_callback_endpoint,
                methods=["GET"],
                name="oidc-callback",
            ),
        ])
    return routes
```

### 6.3 Allowed Routes in AuthMiddleware

The OIDC routes must be allowed without authentication:

```python
config = AuthConfig(
    allowed_routes=[
        "login", "static", "root_static", "favicon_static",
        "oidc-authorize", "oidc-callback",  # NEW
    ],
)
```

______________________________________________________________________

## 7. Login Page Design

### 7.1 Dynamic OIDC Buttons

The `login_endpoint` in `dagster_webserver/auth/routes.py` is modified to:

1. Query the database for enabled OIDC providers
1. Pass them to the login template
1. Render OIDC buttons alongside the password form

```python
async def login_endpoint(request: Request):
    provider = _get_provider(request)

    if request.method == "GET":
        if "text/html" in request.headers.get("Accept", ""):
            # Fetch OIDC providers for the login form
            oidc_providers = []
            if hasattr(provider, "get_oidc_providers"):
                oidc_providers = await provider.get_oidc_providers()
            return HTMLResponse(_render_login(oidc_providers=oidc_providers))
        ...
```

### 7.2 Template Changes

The login HTML template is extended with an OIDC buttons section:

```html
<!-- After the password form, before the footer -->
{% if oidc_providers %}
<div class="login-divider">
  <hr />
  <span>Or sign in with</span>
</div>
<div class="login-oidc-buttons">
  {% for p in oidc_providers %}
  <a
    href="/oidc/authorize/{{ p.name }}"
    class="login-oidc-btn login-oidc-btn--{{ p.name }}"
  >
    <img
      src="{{ p.icon_url }}"
      alt="{{ p.display_name }}"
      class="login-oidc-icon"
    />
    <span>Sign in with {{ p.display_name }}</span>
  </a>
  {% endfor %}
</div>
{% endif %}
```

### 7.3 Provider-Specific Styling

Each provider gets its own brand color for the button:

| Provider | Button Color | Icon |
| -------- | -------------------------- | --------------- |
| Google | `#4285F4` (Google Blue) | Google "G" logo |
| Okta | `#007DC1` (Okta Blue) | Okta logo |
| Auth0 | `#EB5424` (Auth0 Orange) | Auth0 logo |
| Azure AD | `#0078D4` (Microsoft Blue) | Microsoft logo |
| Keycloak | `#4A90D9` (Keycloak Blue) | Keycloak shield |
| Generic | `#4f438d` (Dagster purple) | Shield icon |

The template uses CSS classes like `.login-oidc-btn--google` for provider-specific styling. A fallback generic style is used for unknown providers.

### 7.4 Well-Known Provider Icons

For known providers, we embed SVG icons directly in the template to avoid external dependencies:

```html
{# Google icon (inline SVG) #} {% if p.name == 'google' %}
<svg class="login-oidc-icon" viewBox="0 0 48 48">
  <path fill="#EA4335" d="M24 9.5c3.54..." />
  <path fill="#4285F4" d="M48 24.6c0-1.6..." />
  <!-- ... Google logo SVG ... -->
</svg>
{% elif p.name == 'okta' %}
<svg class="login-oidc-icon" viewBox="0 0 24 24">
  <!-- Okta logo SVG -->
</svg>
{% else %}
<i class="fas fa-shield-alt login-oidc-icon"></i>
{% endif %}
```

______________________________________________________________________

## 8. User Provisioning Strategy

### 8.1 First-Time OIDC User Flow

When a user logs in via OIDC for the first time:

1. Extract `sub`, `email`, `name` from the verified ID token
1. Query the database for an existing user with `oidc_sub = <sub>` AND `oidc_provider_id = <provider_id>`
1. If found → authenticate and create session
1. If not found → **auto-provision**:
   a. Check if a user with the same `email` exists (link existing account)
   b. If email match → update existing user with OIDC linkage
   c. If no match → create new user with default role
   d. Set `oidc_provider_id` and `oidc_sub` on the user record

### 8.2 Auto-Provisioning Configuration

Add a configuration option to control provisioning behavior:

```python
@dataclass
class OIDCAutoProvisionConfig:
    """Configuration for automatic user provisioning via OIDC."""
    enabled: bool = True                          # Enable auto-provisioning
    default_role: str = "viewer"                  # Role for new users
    require_email_verified: bool = True           # Only provision if email_verified claim is true
    allowed_domains: list[str] | None = None      # Restrict to specific email domains (e.g., ["@company.com"])
```

This can be stored per-OIDC-provider or as a global setting. For the initial implementation, it's per-provider in the `oidc_providers` table.

### 8.3 Account Linking

If a user with the same email already exists (from password login), we **link** the OIDC identity to the existing account:

```python
async def _link_or_create_user(
    self,
    provider: OIDCProvider,
    oidc_sub: str,
    email: str | None,
    name: str | None,
) -> AuthUser:
    """Link OIDC identity to existing user or create new user."""
    # 1. Check for existing OIDC linkage
    existing = await self._user_backend.get_user_by_oidc(provider.id, oidc_sub)
    if existing:
        return existing

    # 2. Check for email match (account linking)
    if email:
        email_user = await self._user_backend.get_user_by_email(email)
        if email_user:
            # Link OIDC identity to existing user
            await self._user_backend.link_oidc_to_user(
                email_user.username, provider.id, oidc_sub
            )
            return email_user

    # 3. Create new user
    username = email or f"oidc_{provider.name}_{oidc_sub}"
    return await self._user_backend.create_oidc_user(
        username=username,
        provider_id=provider.id,
        oidc_sub=oidc_sub,
        email=email,
        display_name=name,
        role=self._default_role,
    )
```

______________________________________________________________________

## 9. Admin Portal Integration

### 9.1 New Admin Permission

Add a new admin permission for managing OIDC configurations:

```python
# dagster_webserver/admin/permissions.py

class AdminPermission(str, Enum):
    ADMIN_VIEW_USERS = "admin_view_users"
    ADMIN_EDIT_USERS = "admin_edit_users"
    ADMIN_VIEW_ROLES = "admin_view_roles"
    ADMIN_EDIT_ROLES = "admin_edit_roles"
    ADMIN_VIEW_OIDC = "admin_view_oidc"       # NEW
    ADMIN_EDIT_OIDC = "admin_edit_oidc"       # NEW (implies view)
```

### 9.2 New Admin View: OIDCProviderView

```python
# dagster_webserver/admin/views.py

class OIDCProviderView(BaseAdminView):
    identity = "oidc"
    label = "OIDC Provider"
    plural_label = "OIDC Providers"
    icon = "shield-alt"

    list_columns = ["display_name", "issuer_url", "enabled", "display_order", "created_at"]
    detail_fields = ["name", "display_name", "issuer_url", "client_id", "scopes", "enabled", "display_order"]
    create_fields = ["name", "display_name", "issuer_url", "client_id", "client_secret", "scopes", "display_order"]
    edit_fields = ["display_name", "issuer_url", "client_id", "client_secret", "scopes", "enabled", "display_order"]

    def is_accessible(self, request: Request) -> bool:
        return can_view_oidc(_get_admin_perms(request))

    def can_create(self, request: Request) -> bool:
        return can_edit_oidc(_get_admin_perms(request))

    # ... CRUD methods delegating to DatabaseUserBackend ...
```

### 9.3 Admin Portal Dashboard

The dashboard template (`dashboard.html`) already iterates over `views`. Adding `OIDCProviderView` to the view list automatically adds it to the sidebar and dashboard cards.

### 9.4 Secret Handling in Admin UI

The `client_secret` field needs special handling:

- **Create form**: plain text input (required)
- **Edit form**: masked input with "show" toggle; empty means "keep current value"
- **Detail view**: masked (e.g., `sk••••••••••••abc`)

______________________________________________________________________

## 10. Security Considerations

### 10.1 Token Verification

The ID token MUST be verified before trusting any claims:

1. **Signature** — verify using the provider's public key (from JWKS endpoint)
1. **Issuer** — `iss` claim must match the configured `issuer_url`
1. **Audience** — `aud` claim must contain our `client_id`
1. **Expiration** — `exp` claim must be in the future
1. **Nonce** — `nonce` claim must match the nonce stored in session (anti-replay)
1. **Auth Time** — optionally check `auth_time` to ensure recent authentication

### 10.2 CSRF Protection

The OIDC `state` parameter prevents CSRF attacks:

- Generated as a random 32-byte hex string
- Stored in the session before redirect
- Verified on callback (must match exactly)
- Includes provider name to identify the correct config

### 10.3 PKCE (Proof Key for Code Exchange)

Even for confidential clients, PKCE adds protection:

- `code_verifier`: random 43-128 char string, stored in session
- `code_challenge`: BASE64URL(SHA256(code_verifier))
- Sent in authorization request, verified in token exchange

### 10.4 Client Secret Storage

- Stored in the database as plain text (encrypted at rest via database encryption or application-level encryption)
- Never logged or exposed in API responses
- Only accessible to users with `ADMIN_EDIT_OIDC` permission
- Consider application-level encryption (e.g., using a master key from environment variable)

### 10.5 Session Fixation

After OIDC login, regenerate the session to prevent session fixation attacks.

### 10.6 Open Redirect Prevention

The `next` parameter in the login redirect must be validated:

- Must be a relative path (not an absolute URL)
- Must start with `/`
- Must not contain `//` or protocol schemes

______________________________________________________________________

## 11. Provider-Specific Configuration Examples

### 11.1 Google

```
Display Name: Google
Issuer URL: https://accounts.google.com
Client ID: <from Google Cloud Console>
Client Secret: <from Google Cloud Console>
Scopes: openid email profile
```

Console setup:

1. Go to Google Cloud Console → APIs & Services → Credentials
1. Create OAuth 2.0 Client ID (Web application)
1. Add authorized redirect URI: `http://localhost:3000/oidc/callback`
1. Copy Client ID and Client Secret

### 11.2 Okta

```
Display Name: Okta
Issuer URL: https://<your-org>.okta.com/oauth2/default
Client ID: <from Okta Admin Console>
Client Secret: <from Okta Admin Console>
Scopes: openid email profile
```

Console setup:

1. Go to Okta Admin → Applications → Add Application
1. Choose Web → Next
1. Add Redirect URI: `http://localhost:3000/oidc/callback`
1. Grant type: Authorization Code
1. Copy Client ID and Secret

### 11.3 Auth0

```
Display Name: Auth0
Issuer URL: https://<your-tenant>.auth0.com/
Client ID: <from Auth0 Dashboard>
Client Secret: <from Auth0 Dashboard>
Scopes: openid email profile
```

### 11.4 Azure AD (Microsoft Entra ID)

```
Display Name: Microsoft
Issuer URL: https://login.microsoftonline.com/<tenant-id>/v2.0
Client ID: <from Azure Portal>
Client Secret: <from Azure Portal>
Scopes: openid email profile
```

### 11.5 Keycloak

```
Display Name: Keycloak
Issuer URL: https://<keycloak-host>/realms/<realm-name>
Client ID: <from Keycloak Admin>
Client Secret: <from Keycloak Admin>
Scopes: openid email profile
```

______________________________________________________________________

## 12. Testing Strategy

### 12.1 Unit Tests

- `OIDCClient.verify_id_token()` — valid/invalid tokens, expired tokens, wrong issuer
- `HybridSessionAuthProvider.initiate_oidc_login()` — correct redirect URL, state storage
- `HybridSessionAuthProvider.handle_oidc_callback()` — valid flow, invalid state, missing code
- `DatabaseUserBackend.create_oidc_user()` — creates user with OIDC linkage
- `DatabaseUserBackend.link_oidc_to_user()` — links existing user
- `DatabaseUserBackend.list_oidc_providers()` — filters by enabled status

### 12.2 Integration Tests

- Full OIDC flow with a mock OIDC provider (using `pytest-oidc` or custom mock)
- Login page renders OIDC buttons when providers are configured
- Login page shows only password form when no OIDC providers exist
- Admin CRUD for OIDC providers
- Account linking (email match)
- Auto-provisioning with default role
- PKCE flow end-to-end

### 12.3 Mock OIDC Provider

For testing, implement a minimal OIDC provider mock:

```python
# dagster_webserver_tests/conftest.py

class MockOIDCProvider:
    """Minimal OIDC provider mock for testing."""

    def __init__(self, issuer_url, client_id, client_secret):
        self.issuer_url = issuer_url
        self.client_id = client_id
        self.client_secret = client_secret
        self._signing_key = generate_jwk()

    def get_authorization_url(self, **kwargs):
        # Returns a URL that the test client can intercept
        ...

    def exchange_code(self, code, **kwargs):
        # Returns a mock ID token and access token
        ...

    def create_id_token(self, sub, email, name, audience, nonce=None):
        # Creates a signed JWT ID token
        ...
```

______________________________________________________________________

## 13. Implementation Phases

### Phase 1: Database Schema + Models

- Add `OIDCProvider` SQLAlchemy model to `database/models.py`
- Extend `User` model with `oidc_provider_id` and `oidc_sub` columns
- Create Alembic migration `002_create_oidc_providers.py`
- Add OIDC CRUD methods to `DatabaseUserBackend`

### Phase 2: OIDC Client + Auth Provider

- Add `authlib` + `httpx` as optional dependencies
- Create `auth/oidc/client.py` — OIDC client wrapper
- Create `HybridSessionAuthProvider` extending `SessionAuthProvider`
- Implement `initiate_oidc_login()` and `handle_oidc_callback()`

### Phase 3: Routes + Login Page

- Create `auth/oidc/routes.py` — authorize and callback handlers
- Modify `login_endpoint` to query and render OIDC buttons
- Update login HTML template with OIDC button section
- Register OIDC routes in `DagsterWebserver._build_auth_routes()`

### Phase 4: Admin Portal

- Add `ADMIN_VIEW_OIDC` and `ADMIN_EDIT_OIDC` permissions
- Create `OIDCProviderView` admin view
- Wire into admin portal dashboard and sidebar
- Handle secret masking in forms

### Phase 5: User Provisioning

- Implement auto-provisioning on first OIDC login
- Implement account linking (email match)
- Add provisioning configuration options

### Phase 6: Testing + Polish

- Unit tests for all new components
- Integration tests with mock OIDC provider
- Provider-specific icon SVGs in login template
- Error handling and user-friendly messages

______________________________________________________________________

## 14. Backward Compatibility

| Aspect | Impact |
| ----------------------- | ---------------------------------------------------------- |
| Existing password login | No change — continues to work alongside OIDC |
| Existing sessions | No change — session structure unchanged |
| Existing users | No change — `oidc_provider_id` and `oidc_sub` are nullable |
| Existing admin portal | No change — OIDC view is added alongside existing views |
| Database migration | New migration is additive (new table + nullable columns) |
| Dependencies | `authlib` and `httpx` are optional (`auth-oidc` extra) |
| Auth middleware | OIDC routes added to allowed_routes list |
| `AuthUser` dataclass | No change — OIDC users are stored as regular users |

______________________________________________________________________

## 15. Future Extensions

| Feature | Description |
| --------------------------- | ------------------------------------------------------ |
| SAML support | Add SAML 2.0 as an alternative to OIDC |
| SCIM provisioning | Sync users from IdP to database automatically |
| Just-in-time provisioning | Create users on first login without admin intervention |
| Group-based role assignment | Map OIDC groups/claims to Dagster roles |
| Multi-factor authentication | Require MFA for admin users |
| OIDC logout (RP-Initiated) | Log out from the IdP when user logs out of Dagster |
| Audit logging | Log all OIDC login events |
| Custom claims mapping | Allow admin to map custom OIDC claims to user fields |
| Token introspection | Use OAuth2 introspection endpoint for token validation |
