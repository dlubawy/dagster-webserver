# Plan: OIDC Login Capabilities for Dagster Webserver

## Goal

Add OpenID Connect (OIDC) login to the existing auth system so that users can authenticate via external identity providers (Google, Okta, Auth0, Azure AD, Keycloak, etc.) in addition to the existing username/password flow. OIDC provider configurations are stored in the database, managed through the admin portal, and rendered as dynamic buttons on the login page.

## Pre-existing Infrastructure

The following already exists in `dagster_webserver/` and is **not** part of this plan:

- **Auth system** (`auth/`): `SessionAuthProvider`, `AuthMiddleware`, `login_endpoint`, `logout_endpoint`, `me_endpoint`, `AuthUser`, `UserBackend`, `DatabaseUserBackend`, `Role` enum, `get_role_permissions()`
- **Database layer** (`database/`): SQLAlchemy models (`User`, `Role`), async engine (`get_engine()`, `get_session_factory()`), Alembic migration `001_create_roles_and_users`
- **Admin portal** (`admin/`): `AdminPortal`, `BaseAdminView`, `UserView`, `RoleView`, `AdminPermission` enum (4 values), `AdminPortalMiddleware`, Jinja2 templates
- **Webserver** (`webserver.py`): `DagsterWebserver` with `_auth_provider`, `_admin_portal`, `build_middleware()`, `_build_auth_routes()`, `build_routes()`
- **Login page**: Inline HTML template in `dagster_webserver/auth/routes.py` (`_LOGIN_TEMPLATE`)
- **Permission model**: 5 built-in roles, 21 Dagster `Permissions`, 4 `AdminPermission` values

See `/.agent/research/11-oidc-login-research.md` for full OIDC protocol details, library selection rationale, security considerations, and provider configuration examples.

______________________________________________________________________

## Phase 1: Database Schema — OIDC Provider Model + Migration

**Objective**: Add the `oidc_providers` table and OIDC linkage columns to the `users` table.

### Step 1.1: Add `OIDCProvider` ORM model to `dagster_webserver/database/models.py`

- Define `OIDCProvider(Base)` with columns:
  - `id` (Integer PK, autoincrement)
  - `name` (String(64), unique, not null) — internal identifier (e.g. "google", "okta")
  - `display_name` (String(128), not null) — human-readable name (e.g. "Google")
  - `issuer_url` (String(512), not null) — OIDC issuer URL
  - `client_id` (String(256), not null)
  - `client_secret` (String(1024), not null)
  - `scopes` (String(512), default `"openid email profile"`)
  - `enabled` (Boolean, default True)
  - `display_order` (Integer, default 0)
  - `created_at`, `updated_at` (DateTime with timezone, server defaults)
- Add `users` relationship: `Mapped[list["User"]] = relationship("User", back_populates="oidc_provider")`
- Export `OIDCProvider` from `dagster_webserver/database/__init__.py`

**Outcome**: `OIDCProvider` is a first-class ORM model queryable via SQLAlchemy.

### Step 1.2: Extend `User` model with OIDC linkage columns

- In `dagster_webserver/database/models.py`, add to `User`:
  - `oidc_provider_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("oidc_providers.id"), nullable=True)`
  - `oidc_sub: Mapped[str | None] = mapped_column(String(256), nullable=True)`
  - `oidc_provider: Mapped["OIDCProvider | None"] = relationship("OIDCProvider", back_populates="users")`

**Outcome**: Users can be linked to an OIDC provider via `oidc_sub` + `oidc_provider_id`.

### Step 1.3: Create Alembic migration `002_create_oidc_providers.py`

- In `dagster_webserver/database/alembic/versions/002_create_oidc_providers.py`:
  - `upgrade()`:
    - `op.create_table("oidc_providers", ...)` with all columns from Step 1.1
    - `op.add_column("users", sa.Column("oidc_provider_id", sa.Integer, nullable=True))`
    - `op.add_column("users", sa.Column("oidc_sub", sa.String(256), nullable=True))`
    - `op.create_foreign_key("fk_users_oidc_provider", "users", "oidc_providers", ["oidc_provider_id"], ["id"])`
    - `op.create_index("ix_users_oidc_sub", "users", ["oidc_sub"])`
  - `downgrade()`: reverse all operations

**Outcome**: Migration creates the `oidc_providers` table and extends `users` with OIDC columns. Migration is additive (no data loss).

______________________________________________________________________

## Phase 2: OIDC CRUD Methods on `DatabaseUserBackend`

**Objective**: Add OIDC provider and user management methods to `DatabaseUserBackend`.

### Step 2.1: OIDC provider CRUD

Add to `dagster_webserver/auth/db_backend.py`:

- `async def list_oidc_providers(self, *, enabled_only: bool = False) -> list[OIDCProvider]`
  - Query `OIDCProvider` ordered by `display_order`, filter by `enabled` if `enabled_only`
- `async def get_oidc_provider(self, name: str) -> OIDCProvider | None`
  - Query by `name`
- `async def get_oidc_provider_by_id(self, provider_id: int) -> OIDCProvider | None`
  - Query by `id`
- `async def create_oidc_provider(self, name: str, display_name: str, issuer_url: str, client_id: str, client_secret: str, scopes: str = "openid email profile", display_order: int = 0) -> OIDCProvider`
  - Insert new `OIDCProvider`, commit, return
  - Raises `IntegrityError` if `name` already exists
- `async def update_oidc_provider(self, name: str, *, display_name: str | None = None, issuer_url: str | None = None, client_id: str | None = None, client_secret: str | None = None, scopes: str | None = None, enabled: bool | None = None, display_order: int | None = None) -> OIDCProvider`
  - Update fields, commit, return
  - Raises `ValueError` if not found
- `async def delete_oidc_provider(self, name: str) -> None`
  - Delete provider, cascade null out `oidc_provider_id` on linked users
  - Raises `ValueError` if not found

**Outcome**: Full CRUD for OIDC providers via `DatabaseUserBackend`.

### Step 2.2: OIDC user lookup and creation methods

Add to `dagster_webserver/auth/db_backend.py`:

- `async def get_user_by_oidc(self, provider_id: int, oidc_sub: str) -> AuthUser | None`
  - Query `User` where `oidc_provider_id = provider_id` AND `oidc_sub = oidc_sub`
  - Join `role` and `oidc_provider`, return `AuthUser` or `None`
- `async def get_user_by_email(self, email: str) -> AuthUser | None`
  - Query `User` where `email = email`, join `role`, return `AuthUser` or `None`
- `async def create_oidc_user(self, username: str, provider_id: int, oidc_sub: str, *, email: str | None = None, display_name: str | None = None, role: str = "viewer") -> AuthUser`
  - Create `User` with `oidc_provider_id`, `oidc_sub`, `email`, `display_name`, `role_id`
  - `password_hash` set to empty string (OIDC users don't have passwords)
  - Raises `ValueError` if role not found
- `async def link_oidc_to_user(self, username: str, provider_id: int, oidc_sub: str) -> AuthUser`
  - Update existing user's `oidc_provider_id` and `oidc_sub`
  - Raises `ValueError` if user not found

**Outcome**: OIDC users can be looked up, created, and linked to existing accounts.

______________________________________________________________________

## Phase 3: OIDC Client Library Integration

**Objective**: Create the OIDC client wrapper that handles the authorization code + PKCE flow using Authlib.

### Step 3.1: Add optional dependencies to `pyproject.toml`

- Add `[project.optional-dependencies]` group:
  ```toml
  auth-oidc = ["authlib>=1.3.0", "httpx>=0.27.0"]
  ```

**Outcome**: `authlib` and `httpx` are installable via `pip install dagster-webserver[auth-oidc]`.

### Step 3.2: Create `dagster_webserver/auth/oidc/__init__.py`

- Export: `OIDCProviderConfig`, `OIDCClient`, `HybridSessionAuthProvider`

**Outcome**: Clean public API for the OIDC module.

### Step 3.3: Create `dagster_webserver/auth/oidc/models.py`

- Define `@dataclass OIDCProviderConfig`:
  - `id: int`, `name: str`, `display_name: str`, `issuer_url: str`, `client_id: str`, `client_secret: str`, `scopes: str`, `enabled: bool`, `display_order: int`
- Define `from_orm(provider: OIDCProvider) -> OIDCProviderConfig` factory method
- Define `SESSION_OIDC_STATE`, `SESSION_OIDC_VERIFIER`, `SESSION_OIDC_PROVIDER`, `SESSION_OIDC_NONCE`, `SESSION_OIDC_REDIRECT` string constants for session keys

**Outcome**: Dataclass for OIDC provider configuration and session key constants.

### Step 3.4: Create `dagster_webserver/auth/oidc/client.py`

- Define `class OIDCClient`:
  - `__init__(self, provider: OIDCProviderConfig)` — stores provider config
  - `async def discover(self) -> dict` — fetch `.well-known/openid-configuration` from issuer, cache result. Returns dict with `authorization_endpoint`, `token_endpoint`, `jwks_uri`, etc.
  - `async def get_authorization_url(self, redirect_uri: str, state: str, code_challenge: str, nonce: str) -> str` — build authorization URL with PKCE challenge and nonce
  - `async def exchange_code(self, code: str, redirect_uri: str, code_verifier: str) -> dict` — POST to token endpoint with code + PKCE verifier. Returns `{"id_token": ..., "access_token": ...}`
  - `async def verify_id_token(self, id_token: str, nonce: str | None = None) -> dict` — decode and verify JWT: signature (via JWKS), `iss` matches `issuer_url`, `aud` contains `client_id`, `exp` not expired, `nonce` matches if provided. Returns claims dict.
  - `async def _fetch_jwks(self) -> dict` — fetch and cache JWKS from `jwks_uri`

**Outcome**: OIDC client handles discovery, authorization URL building, token exchange, and ID token verification.

### Step 3.5: PKCE helpers

- In `dagster_webserver/auth/oidc/client.py`, add module-level helpers:
  - `generate_code_verifier() -> str` — random 43-128 char string (RFC 7636)
  - `generate_code_challenge(verifier: str) -> str` — BASE64URL(SHA256(verifier))
  - `generate_state(provider_name: str) -> str` — `{random_hex}:{provider_name}`
  - `parse_state(state: str) -> tuple[str, str]` — split into `(random_hex, provider_name)`

**Outcome**: PKCE and state generation/parsing utilities.

______________________________________________________________________

## Phase 4: `HybridSessionAuthProvider`

**Objective**: Extend `SessionAuthProvider` with OIDC login capabilities.

### Step 4.1: Create `HybridSessionAuthProvider` in `dagster_webserver/auth/provider.py`

- Define `class HybridSessionAuthProvider(SessionAuthProvider)`:
  - `__init__(self, user_backend: DatabaseUserBackend, config: AuthConfig | None = None)` — calls `super().__init__()`, asserts `isinstance(user_backend, DatabaseUserBackend)`
  - Inherits `authenticate_request()` from `SessionAuthProvider` (session-based auth works for both password and OIDC logins)
  - Inherits `login()` and `logout()` from `SessionAuthProvider` (password login unchanged)

**Outcome**: `HybridSessionAuthProvider` supports both password and OIDC login through the same session mechanism.

### Step 4.2: `get_oidc_providers()` method

- `async def get_oidc_providers(self) -> list[OIDCProviderConfig]`
  - Calls `self._user_backend.list_oidc_providers(enabled_only=True)`
  - Converts ORM objects to `OIDCProviderConfig` dataclass instances

**Outcome**: Returns list of enabled OIDC providers for the login page.

### Step 4.3: `initiate_oidc_login()` method

- `async def initiate_oidc_login(self, provider_name: str, request: Request) -> RedirectResponse`
  1. Look up provider by `provider_name` via `self._user_backend.get_oidc_provider(provider_name)`
  1. If not found or not enabled → `RedirectResponse` to `/login` with error query param
  1. Capture `next_url` from `request.query_params.get("next", "/")` — validate it is a relative path starting with `/` (open redirect prevention)
  1. Generate PKCE `code_verifier` and `code_challenge`
  1. Generate `state` = `{random_hex}:{provider_name}`
  1. Generate `nonce` = `secrets.token_urlsafe(32)`
  1. Store in session: `SESSION_OIDC_STATE → state`, `SESSION_OIDC_VERIFIER → code_verifier`, `SESSION_OIDC_NONCE → nonce`, `SESSION_OIDC_REDIRECT → next_url`
  1. Create `OIDCClient(provider_config)` and call `client.get_authorization_url(redirect_uri, state, code_challenge, nonce)`
  1. Return `RedirectResponse(authorization_url, status_code=302)`

**Outcome**: Initiates the OIDC authorization flow with PKCE, state, and the original destination URL preserved for post-login redirect.

### Step 4.4: `handle_oidc_callback()` method

- `async def handle_oidc_callback(self, request: Request) -> AuthUser | None`
  1. Extract `code` and `state` from query params. If missing → return `None`
  1. Verify `state` matches `request.session.get(SESSION_OIDC_STATE)`. If mismatch → return `None` (CSRF protection)
  1. Parse `state` to get `provider_name`
  1. Retrieve `code_verifier` and `nonce` from session
  1. Look up provider config by `provider_name`
  1. Create `OIDCClient(provider_config)` and call `client.exchange_code(code, redirect_uri, code_verifier)`
  1. Call `client.verify_id_token(id_token, nonce)` — raises on verification failure
  1. Extract `sub`, `email`, `name` from verified claims
  1. Call `self._link_or_create_user(provider, sub, email, name)` (see Step 4.5)
  1. Set `request.session["username"] = user.username`
  1. Clear OIDC session state keys
  1. Return `AuthUser`

**Outcome**: Handles the OIDC callback, exchanges code, verifies token, creates/logs in user.

### Step 4.5: `_link_or_create_user()` method

- `async def _link_or_create_user(self, provider: OIDCProviderConfig, oidc_sub: str, email: str | None, name: str | None) -> AuthUser`
  1. `existing = await self._user_backend.get_user_by_oidc(provider.id, oidc_sub)` — if found, return it
  1. If `email`: `email_user = await self._user_backend.get_user_by_email(email)` — if found, call `self._user_backend.link_oidc_to_user(email_user.username, provider.id, oidc_sub)` and return updated user
  1. Otherwise: `username = email or f"oidc_{provider.name}_{oidc_sub}"` — call `self._user_backend.create_oidc_user(username, provider.id, oidc_sub, email=email, display_name=name, role=self.config.default_role)`

**Outcome**: OIDC users are auto-provisioned with account linking via email match.

### Step 4.6: Export from `dagster_webserver/auth/__init__.py`

- Add `HybridSessionAuthProvider` to imports and `__all__`

**Outcome**: `HybridSessionAuthProvider` is importable from the auth package.

______________________________________________________________________

## Phase 5: OIDC Routes

**Objective**: Add `/oidc/authorize/{provider_name}` and `/oidc/callback` route handlers.

### Step 5.1: Create `dagster_webserver/auth/oidc/routes.py`

- `async def oidc_authorize_endpoint(request: Request) -> RedirectResponse`

  - Extract `provider_name` from path params
  - Get `HybridSessionAuthProvider` from `request.app.state.auth_provider`
  - Call `provider.initiate_oidc_login(provider_name, request)`
  - If provider doesn't support OIDC (not `HybridSessionAuthProvider`) → 404

- `async def oidc_callback_endpoint(request: Request) -> RedirectResponse | HTMLResponse`

  - Get `HybridSessionAuthProvider` from `request.app.state.auth_provider`
  - Call `provider.handle_oidc_callback(request)`
  - If `AuthUser` returned → retrieve `next_url` from `request.session.get(SESSION_OIDC_REDIRECT, "/")`, validate it is a relative path starting with `/` (no `//`, no protocol schemes), redirect to `next_url`
  - If `None` → redirect to `/login` with error query param
  - Handle error cases (invalid state, missing code, token verification failure) with user-friendly error messages

**Outcome**: OIDC authorize and callback endpoints are functional. After successful OIDC login, the user is redirected to the page they were originally trying to reach.

### Step 5.2: Register OIDC routes in `DagsterWebserver._build_auth_routes()`

- In `dagster_webserver/webserver.py`, modify `_build_auth_routes()`:
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
              Route("/oidc/authorize/{provider_name}", oidc_authorize_endpoint, methods=["GET"], name="oidc-authorize"),
              Route("/oidc/callback", oidc_callback_endpoint, methods=["GET"], name="oidc-callback"),
          ])
      return routes
  ```

**Outcome**: OIDC routes are registered when `HybridSessionAuthProvider` is used.

### Step 5.3: Add OIDC routes to allowed routes in `AuthMiddleware`

- OIDC routes (`oidc-authorize`, `oidc-callback`) must be accessible without authentication
- The `AuthMiddleware._is_allowed_without_auth()` already checks `route.name` against `self.provider.config.allowed_routes`
- Default `AuthConfig.allowed_routes` is `[]` — callers must include `"oidc-authorize"` and `"oidc-callback"` when using `HybridSessionAuthProvider`
- **Action**: In the CLI or app creation code, when `HybridSessionAuthProvider` is used, ensure `allowed_routes` includes `"oidc-authorize"` and `"oidc-callback"`

**Outcome**: OIDC routes bypass authentication check in middleware.

______________________________________________________________________

## Phase 6: Login Page — Dynamic OIDC Buttons

**Objective**: Modify the login page to render OIDC provider buttons alongside the password form.

### Step 6.1: Modify `login_endpoint` in `dagster_webserver/auth/routes.py`

- In the GET branch, after getting the provider:
  ```python
  oidc_providers = []
  if hasattr(provider, "get_oidc_providers"):
      oidc_providers = await provider.get_oidc_providers()
  return HTMLResponse(_render_login(oidc_providers=oidc_providers))
  ```

**Outcome**: Login endpoint fetches OIDC providers and passes them to the template.

### Step 6.2: Modify `_render_login()` and `_LOGIN_TEMPLATE`

- Change `_render_login(error: str | None = None)` signature to `_render_login(error: str | None = None, oidc_providers: list[OIDCProviderConfig] | None = None)`
- In `_LOGIN_TEMPLATE`, after the password form's closing `</form>` and before the footer, inject an OIDC buttons section:
  - Divider: `<hr/>` with "Or sign in with" text
  - For each provider: `<a href="/oidc/authorize/{name}" class="login-oidc-btn">` with provider-specific styling
  - CSS for OIDC buttons: flex row, branded colors per provider (`--google: #4285F4`, `--okta: #007DC1`, `--auth0: #EB5424`, generic fallback `#4f438d`)
  - Inline SVG icons for known providers (Google, Okta)
  - Fallback shield icon for unknown providers
- Use string formatting (not Jinja2) since the template is an inline string: build the OIDC buttons HTML in `_render_login()` and inject via `{oidc_buttons}` placeholder

**Outcome**: Login page dynamically renders OIDC provider buttons.

### Step 6.3: CSS for OIDC buttons

- Add to the `<style>` block in `_LOGIN_TEMPLATE`:
  - `.login-divider`: horizontal rule with centered text
  - `.login-oidc-buttons`: flex container, gap between buttons
  - `.login-oidc-btn`: button styling (height, padding, border-radius, hover states, focus states matching existing button styles)
  - `.login-oidc-btn--google`, `.login-oidc-btn--okta`, etc.: provider-specific background colors
  - `.login-oidc-icon`: inline SVG sizing
  - Dark mode support matching existing theme tokens

**Outcome**: OIDC buttons are styled consistently with the Dagster login page.

______________________________________________________________________

## Phase 7: Admin Portal — OIDC Provider Management

**Objective**: Add OIDC provider CRUD to the admin portal.

### Step 7.1: Add OIDC admin permissions to `dagster_webserver/admin/permissions.py`

- Add to `AdminPermission` enum:
  - `ADMIN_VIEW_OIDC = "admin_view_oidc"`
  - `ADMIN_EDIT_OIDC = "admin_edit_oidc"` (implies view)
- Add resolution helpers:
  - `def can_view_oidc(perms) -> bool` — checks `ADMIN_EDIT_OIDC` or `ADMIN_VIEW_OIDC`
  - `def can_edit_oidc(perms) -> bool` — checks `ADMIN_EDIT_OIDC`
- Update `has_any_admin_permission()` to include OIDC permissions
- Update `_admin_permissions_for_role()` in `dagster_webserver/auth/roles.py` to include `ADMIN_VIEW_OIDC` and `ADMIN_EDIT_OIDC` for the `ADMIN` role (enabled) and all other roles (disabled)

**Outcome**: OIDC admin permissions are defined and wired into the role system.

### Step 7.2: Create `OIDCProviderView` in `dagster_webserver/admin/views.py`

- Inherit from `BaseAdminView`
- Class attributes:
  - `identity = "oidc"`
  - `label = "OIDC Provider"`
  - `plural_label = "OIDC Providers"`
  - `icon = "shield-alt"`
  - `list_columns = ["display_name", "issuer_url", "user_count", "enabled", "display_order", "created_at"]`
  - `detail_fields = ["name", "display_name", "issuer_url", "client_id", "scopes", "enabled", "display_order", "user_count"]`
  - `create_fields = ["name", "display_name", "issuer_url", "client_id", "client_secret", "scopes", "display_order"]`
  - `edit_fields = ["display_name", "issuer_url", "client_id", "client_secret", "scopes", "enabled", "display_order"]`
- Permission hooks:
  - `is_accessible()` → `can_view_oidc(perms)`
  - `can_create()`, `can_edit()`, `can_delete()` → `can_edit_oidc(perms)`
- CRUD methods delegate to `DatabaseUserBackend`:
  - `find_all()` → `backend.list_oidc_providers()` (with eager-loaded `users` relationship to count linked users)
  - `count()` → `len(list_oidc_providers())`
  - `find_by_pk(pk)` → `backend.get_oidc_provider(pk)` (pk = name, with eager-loaded `users`)
  - `create(data)` → `backend.create_oidc_provider(...)`
  - `edit(pk, data)` → `backend.update_oidc_provider(...)`
  - `delete(pks)` → `backend.delete_oidc_provider(name)` for each
- `serialize()` method:
  - Mask `client_secret` in list/detail views (show `••••••••` + last 4 chars)
  - Populate `user_count` from `len(provider.users)` for each provider

**Outcome**: OIDC providers can be listed, viewed, created, edited, and deleted through the portal.

### Step 7.3: Wire `OIDCProviderView` into `AdminPortal`

- In `dagster_webserver/admin/portal.py`, modify `_init_views()`:
  - Create `OIDCProviderView(self._backend)` alongside `UserView` and `RoleView`
  - Add to `self._views` dict

**Outcome**: OIDC provider view appears in the admin portal sidebar and dashboard.

### Step 7.4: Secret handling in admin forms

- In `create.html`/`edit.html` templates (or `OIDCProviderView.serialize()`):
  - `client_secret` in create form: plain text password input (required)
  - `client_secret` in edit form: masked input with "show" toggle; empty string means "keep current value"
  - `client_secret` in detail view: masked (`••••••••••••abc`)
- In `OIDCProviderView.edit()`: if `client_secret` is empty string, don't pass it to `backend.update_oidc_provider()` (keep existing value)

**Outcome**: Client secrets are protected in the admin UI.

______________________________________________________________________

## Phase 8: Testing

**Objective**: Verify all OIDC functionality works correctly.

### Step 8.1: Unit tests for `OIDCClient`

- `test_discover()` — fetches `.well-known/openid-configuration`, caches result
- `test_get_authorization_url()` — correct URL with PKCE challenge and nonce
- `test_exchange_code()` — POSTs to token endpoint, returns tokens
- `test_verify_id_token_valid()` — valid token passes all checks
- `test_verify_id_token_expired()` — expired token raises
- `test_verify_id_token_wrong_issuer()` — wrong `iss` raises
- `test_verify_id_token_wrong_audience()` — wrong `aud` raises
- `test_verify_id_token_invalid_signature()` — bad signature raises
- `test_verify_id_token_nonce_mismatch()` — wrong nonce raises
- `test_generate_code_challenge()` — correct BASE64URL(SHA256) output

**Outcome**: OIDC client is thoroughly tested in isolation.

### Step 8.2: Unit tests for `HybridSessionAuthProvider`

- `test_initiate_oidc_login()` — correct redirect URL, state stored in session
- `test_initiate_oidc_login_disabled_provider()` — redirects to login with error
- `test_initiate_oidc_login_unknown_provider()` — redirects to login with error
- `test_handle_oidc_callback_valid()` — full flow: code exchange, token verify, user login
- `test_handle_oidc_callback_invalid_state()` — returns None (CSRF protection)
- `test_handle_oidc_callback_missing_code()` — returns None
- `test_handle_oidc_callback_token_verification_failure()` — returns None
- `test_get_oidc_providers()` — returns only enabled providers

**Outcome**: Auth provider OIDC methods are tested.

### Step 8.3: Unit tests for `DatabaseUserBackend` OIDC methods

- `test_create_oidc_provider()` — creates provider, returns ORM object
- `test_list_oidc_providers_enabled_only()` — filters by enabled status
- `test_update_oidc_provider()` — updates fields correctly
- `test_delete_oidc_provider()` — deletes provider, nulls out user links
- `test_get_user_by_oidc()` — finds user by provider_id + sub
- `test_get_user_by_email()` — finds user by email
- `test_create_oidc_user()` — creates user with OIDC linkage
- `test_link_oidc_to_user()` — links existing user to OIDC provider

**Outcome**: Database backend OIDC methods are tested.

### Step 8.4: Integration tests

- `test_full_oidc_login_flow()` — mock OIDC provider, full authorize → callback → session flow
- `test_login_page_renders_oidc_buttons()` — login page includes OIDC buttons when providers configured
- `test_login_page_no_oidc_buttons()` — login page shows only password form when no providers
- `test_oidc_auto_provisioning()` — first-time OIDC user is created with default role
- `test_oidc_account_linking()` — existing user with matching email gets OIDC linked
- `test_admin_oidc_crud()` — admin can create/edit/delete OIDC providers via portal
- `test_oidc_routes_allowed_without_auth()` — `/oidc/authorize` and `/oidc/callback` bypass auth middleware

**Outcome**: End-to-end OIDC flow is verified.

### Step 8.5: Mock OIDC provider for tests

- In `dagster_webserver_tests/conftest.py`, create `MockOIDCProvider`:
  - Generates RSA key pair for JWT signing
  - Serves `.well-known/openid-configuration` (in-memory or via `httpx` mock)
  - Accepts authorization requests, returns mock authorization code
  - Accepts token exchange, returns signed ID token
  - Configurable claims (`sub`, `email`, `name`)

**Outcome**: Tests can simulate a full OIDC provider without external dependencies.

______________________________________________________________________

## File Inventory

### New Files

```
dagster_webserver/
├── auth/
│   └── oidc/
│       ├── __init__.py           # Public exports
│       ├── models.py             # OIDCProviderConfig dataclass, session key constants
│       ├── client.py             # OIDCClient (discovery, PKCE, token exchange, verification)
│       └── routes.py             # oidc_authorize_endpoint, oidc_callback_endpoint
├── database/
│   └── alembic/
│       └── versions/
│           └── 002_create_oidc_providers.py  # Migration: oidc_providers table + user OIDC cols
```

### Modified Files

```
dagster_webserver/
├── pyproject.toml                        # Add auth-oidc optional dependency group
├── database/
│   ├── __init__.py                       # Export OIDCProvider
│   └── models.py                         # Add OIDCProvider model, extend User with OIDC cols
├── auth/
│   ├── __init__.py                       # Export HybridSessionAuthProvider
│   ├── provider.py                       # Add HybridSessionAuthProvider class
│   ├── routes.py                         # Modify login_endpoint, _render_login for OIDC buttons
│   └── db_backend.py                     # Add OIDC CRUD methods
├── admin/
│   ├── permissions.py                    # Add ADMIN_VIEW_OIDC, ADMIN_EDIT_OIDC + helpers
│   ├── portal.py                         # Wire OIDCProviderView into AdminPortal
│   └── views.py                          # Add OIDCProviderView class
├── auth/roles.py                         # Include OIDC admin permissions in _admin_permissions_for_role()
└── webserver.py                          # Register OIDC routes in _build_auth_routes()
```

______________________________________________________________________

## Design Decisions

| Question | Decision |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Library** | Authlib + httpx (optional `auth-oidc` extra). Authlib provides OIDC discovery, JWT verification, JWKS. httpx provides async HTTP. See `/.agent/research/11-oidc-login-research.md §4` for rationale. |
| **Provider class** | `HybridSessionAuthProvider` extends `SessionAuthProvider`. Password login unchanged. OIDC login adds two new methods + two new routes. |
| **Session state** | OIDC flow state (state, verifier, nonce, redirect) stored in `request.session` using defined key constants. Cleared after successful callback. |
| **User provisioning** | Auto-provision on first login with `config.default_role`. Account linking via email match. See `/.agent/research/11-oidc-login-research.md §8`. |
| **Username for OIDC users** | Prefer `email` from ID token. Fallback: `oidc_{provider_name}_{sub}`. |
| **Password for OIDC users** | `password_hash` set to empty string. Once a user logs in via OIDC, that becomes their only login method. No dual-auth. Existing password users can link OIDC via email match. |
| **Post-login redirect** | The `next` parameter from the authorize URL is stored in session and used after successful callback. Validated as a relative path (open redirect prevention). |
| **Logout** | Logout is scoped to Dagster webserver only. No RP-initiated OIDC logout. |
| **Default role** | Single `config.default_role` used for both password and OIDC auto-provisioned users. No separate OIDC role flag. |
| **Multiple providers** | Each provider has a unique `name`. Login page shows buttons for all enabled providers. State parameter encodes provider name for callback routing. |
| **Client secret storage** | Plain text in database (encrypted at rest via DB-level encryption or env-level secrets). Masked in admin UI. See `/.agent/research/11-oidc-login-research.md §10.4`. |
| **OIDC admin permissions** | `ADMIN_VIEW_OIDC` and `ADMIN_EDIT_OIDC` added to `AdminPermission` enum. Only `ADMIN` role gets them enabled. |
| **Admin user count** | `OIDCProviderView` list and detail views show `user_count` (number of users linked to each provider) via eager-loaded `users` relationship. |
| **Backward compatibility** | All changes are additive. OIDC routes only registered when `HybridSessionAuthProvider` is used. `authlib`/`httpx` are optional. Existing password login unchanged. |
| **Open redirect prevention** | The `next` parameter in OIDC redirect is validated: must be relative path starting with `/`, no `//` or protocol schemes. |

______________________________________________________________________

## Design Decisions (Resolved)

| Question | Decision |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **Dual-auth (OIDC + password)** | Not supported. Once a user logs in via OIDC, that becomes their only login method. Existing password users can link OIDC via email match. |
| **Post-login redirect** | The `next` parameter from the authorize URL is preserved in the session and used after successful callback. Validated as a relative path. |
| **RP-initiated logout** | Not implemented. Logout is scoped to Dagster webserver only. |
| **Default role** | Single `config.default_role` for all users (password and OIDC). No separate OIDC role. |
| **Admin user count** | `OIDCProviderView` shows `user_count` per provider via eager-loaded `users` relationship. |
