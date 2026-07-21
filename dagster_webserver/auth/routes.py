"""Auth route handlers: /login, /logout, /api/me.

These endpoints are injected into the DagsterWebserver route table
when an auth provider is configured.
"""

from __future__ import annotations

import logging

from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.status import (
    HTTP_303_SEE_OTHER,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
)

logger = logging.getLogger("dagster-webserver.auth")

# Branded login page styled to match the Dagster UI.
# Uses the same Geist font, core color palette, and layout conventions
# as the rest of the app (ui-components theme).
_LOGIN_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no"/>
<title>Dagster — Sign in</title>
<style>
/* ── Reset & base ─────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: 'Geist', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
               'Helvetica Neue', Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  background-color: var(--bg);
  color: var(--text-default);
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
}

/* ── Theme tokens (light / dark) ──────────────────────── */
:root, .themeLight {
  --bg: #ffffff;
  --bg-card: #ffffff;
  --bg-input: #f5f6f8;
  --bg-input-hover: #ebecef;
  --bg-input-focus: #ffffff;
  --text-default: #171c2c;
  --text-light: #3f485b;
  --text-lighter: #6f7a91;
  --border-default: #d2d6de;
  --border-focus: #4f438d;
  --accent-primary: #4f438d;
  --accent-primary-hover: #4037b5;
  --accent-reversed: #ffffff;
  --error-bg: #fef0ed;
  --error-text: #c72a1c;
  --error-border: #f5c2b8;
  --shadow-card: 0 1px 3px rgba(3,6,21,.08), 0 4px 12px rgba(3,6,21,.04);
  --focus-ring: #4f438d;
}
@media (prefers-color-scheme: dark) {
  :root:not(.themeLight) {
    --bg: #030615;
    --bg-card: #171c2c;
    --bg-input: #1d2237;
    --bg-input-hover: #232844;
    --bg-input-focus: #171c2c;
    --text-default: #ffffff;
    --text-light: #9ea7b9;
    --text-lighter: #6f7a91;
    --border-default: #2b3244;
    --border-focus: #4f438d;
    --accent-primary: #4f438d;
    --accent-primary-hover: #7269e4;
    --accent-reversed: #ffffff;
    --error-bg: #2d1519;
    --error-text: #e5a0a0;
    --error-border: #5c2a30;
    --shadow-card: 0 1px 3px rgba(0,0,0,.2), 0 4px 12px rgba(0,0,0,.12);
    --focus-ring: #7269e4;
  }
}

/* ── Card layout ──────────────────────────────────────── */
.login-card {
  width: 100%;
  max-width: 400px;
  padding: 32px;
  background: var(--bg-card);
  border: 1px solid var(--border-default);
  border-radius: 8px;
  box-shadow: var(--shadow-card);
}

/* ── Logo / branding ──────────────────────────────────── */
.login-brand {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 24px;
}
.login-brand svg {
  flex-shrink: 0;
}
.login-brand h1 {
  font-size: 20px;
  font-weight: 600;
  letter-spacing: -0.01em;
  color: var(--text-default);
}

/* ── Form fields ──────────────────────────────────────── */
.login-form { display: flex; flex-direction: column; gap: 16px; }
.login-field { display: flex; flex-direction: column; gap: 6px; }
.login-field label {
  font-size: 12px;
  font-weight: 500;
  color: var(--text-light);
  letter-spacing: 0.02em;
  text-transform: uppercase;
}
.login-field input {
  width: 100%;
  height: 36px;
  padding: 0 12px;
  font-size: 14px;
  font-family: inherit;
  color: var(--text-default);
  background: var(--bg-input);
  border: 1px solid var(--border-default);
  border-radius: 6px;
  outline: none;
  transition: border-color .15s, background .15s, box-shadow .15s;
}
.login-field input::placeholder { color: var(--text-lighter); }
.login-field input:hover { background: var(--bg-input-hover); }
.login-field input:focus {
  background: var(--bg-input-focus);
  border-color: var(--border-focus);
  box-shadow: 0 0 0 2px color-mix(in srgb, var(--border-focus) 25%, transparent);
}

/* ── Submit button ────────────────────────────────────── */
.login-submit {
  margin-top: 4px;
  height: 36px;
  font-size: 14px;
  font-weight: 500;
  font-family: inherit;
  color: var(--accent-reversed);
  background: var(--accent-primary);
  border: none;
  border-radius: 6px;
  cursor: pointer;
  transition: background .15s;
}
.login-submit:hover { background: var(--accent-primary-hover); }
.login-submit:focus-visible {
  outline: 2px solid var(--focus-ring);
  outline-offset: 2px;
}

/* ── Error banner ─────────────────────────────────────── */
.login-error {
  padding: 10px 12px;
  border-radius: 6px;
  font-size: 13px;
  color: var(--error-text);
  background: var(--error-bg);
  border: 1px solid var(--error-border);
  margin-bottom: 16px;
}

/* ── Divider (between password form and OIDC buttons) ─── */
.login-divider {
  display: flex;
  align-items: center;
  gap: 12px;
  margin: 20px 0;
  color: var(--text-lighter);
  font-size: 12px;
}
.login-divider hr {
  flex: 1;
  border: none;
  border-top: 1px solid var(--border-default);
}

/* ── OIDC buttons ──────────────────────────────────────── */
.login-oidc-buttons {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.login-oidc-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  height: 36px;
  padding: 0 12px;
  font-size: 14px;
  font-weight: 500;
  font-family: inherit;
  color: var(--text-default);
  background: var(--bg-input);
  border: 1px solid var(--border-default);
  border-radius: 6px;
  cursor: pointer;
  text-decoration: none;
  transition: background .15s, border-color .15s;
}
.login-oidc-btn:hover {
  background: var(--bg-input-hover);
  border-color: var(--text-lighter);
}
.login-oidc-btn:focus-visible {
  outline: 2px solid var(--focus-ring);
  outline-offset: 2px;
}
.login-oidc-icon {
  width: 18px;
  height: 18px;
  flex-shrink: 0;
}

/* Provider-specific colors */
.login-oidc-btn--google { border-left: 3px solid #4285F4; }
.login-oidc-btn--okta { border-left: 3px solid #007DC1; }
.login-oidc-btn--auth0 { border-left: 3px solid #EB5424; }
.login-oidc-btn--azure { border-left: 3px solid #0078D4; }
.login-oidc-btn--keycloak { border-left: 3px solid #4A90D9; }

/* ── Footer ───────────────────────────────────────────── */
.login-footer {
  margin-top: 24px;
  text-align: center;
  font-size: 12px;
  color: var(--text-lighter);
}
.login-footer a {
  color: var(--text-light);
  text-decoration: none;
}
.login-footer a:hover { text-decoration: underline; }
</style>
</head>
<body>
<div class="login-card">
  <div class="login-brand">
    <img src="/favicon.png" alt="Dagster" width="32" height="32">
    <h1>Sign in to Dagster</h1>
  </div>
  {error_banner}
  <form class="login-form" method="post" action="/login">
    <div class="login-field">
      <label for="username">Username</label>
      <input id="username" name="username" type="text" required autofocus autocomplete="username" placeholder="Enter your username"/>
    </div>
    <div class="login-field">
      <label for="password">Password</label>
      <input id="password" name="password" type="password" required autocomplete="current-password" placeholder="Enter your password"/>
    </div>
    <button class="login-submit" type="submit">Sign in</button>
  </form>
  {oidc_buttons}
  <div class="login-footer">
    <a href="https://docs.dagster.io" target="_blank" rel="noopener">Documentation</a>
  </div>
</div>
</body>
</html>
"""


def _render_login(
    error: str | None = None,
    oidc_providers: list[object] | None = None,
) -> str:
    """Render the login HTML template, optionally with an error banner
    and OIDC provider buttons."""
    if error:
        error_html = f'<div class="login-error">{error}</div>'
    else:
        error_html = ""

    # Build OIDC buttons HTML
    oidc_html = ""
    if oidc_providers:
        buttons = []
        for p in oidc_providers:
            display_name = getattr(p, "display_name", "OIDC")
            name = getattr(p, "name", "generic")
            icon_svg = _get_oidc_icon_svg(name)
            buttons.append(
                f'<a href="/oidc/authorize/{name}" '
                f'class="login-oidc-btn login-oidc-btn--{name}" '
                f'title="Sign in with {display_name}"> '
                f"{icon_svg} "
                f"<span>Sign in with {display_name}</span>"
                "</a>"
            )
        if buttons:
            oidc_html = (
                '<div class="login-divider"><hr/><span>Or sign in with</span><hr/></div>'
                f'<div class="login-oidc-buttons">{"".join(buttons)}</div>'
            )

    return _LOGIN_TEMPLATE.replace("{error_banner}", error_html).replace(
        "{oidc_buttons}", oidc_html
    )


def _get_oidc_icon_svg(provider_name: str) -> str:
    """Return an inline SVG icon for a known OIDC provider.

    Falls back to a generic shield icon for unknown providers.
    """
    icons: dict[str, str] = {
        "google": (
            '<svg class="login-oidc-icon" viewBox="0 0 48 48">'
            '<path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.97 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.22C12.43 13.72 17.74 9.5 24 9.5z"/>'
            '<path fill="#4285F4" d="M46.98 24.55c0-1.65-.15-3.16-.42-4.55H24v9h12.74c-.55 2.93-2.3 5.38-4.93 6.98l7.62 5.94c4.46-3.97 7.05-9.83 7.05-17.41z"/>'
            '<path fill="#FBBC05" d="M10.53 28.57c-.48-1.45-.76-2.99-.76-4.57s.28-3.12.76-4.57l-7.98-6.22C.92 16.45 0 20.12 0 24s.92 7.55 2.56 10.78l7.97-6.21z"/>'
            '<path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.62-5.94c-2.11 1.42-4.76 2.25-8.27 2.25-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.22C6.51 42.62 14.62 48 24 48z"/>'
            "</svg>"
        ),
        "okta": (
            '<svg class="login-oidc-icon" viewBox="0 0 24 24" fill="none">'
            '<path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2z" fill="#007DC1"/>'
            '<path d="M12 6c-3.31 0-6 2.69-6 6s2.69 6 6 6 6-2.69 6-6-2.69-6-6-6z" fill="white"/>'
            '<path d="M12 9c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z" fill="#007DC1"/>'
            "</svg>"
        ),
    }
    return icons.get(
        provider_name,
        '<svg class="login-oidc-icon" viewBox="0 0 24 24" fill="none"><path d="M12 2L3 7v5c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V7l-9-5z" fill="var(--text-lighter)"/></svg>',
    )


def _get_provider(request: Request) -> object:
    """Retrieve the auth provider from the app state."""
    return request.app.state.auth_provider


async def login_endpoint(
    request: Request,
) -> HTMLResponse | JSONResponse | RedirectResponse:
    """Handle login: GET renders form, POST authenticates."""
    provider = _get_provider(request)

    if request.method == "GET":
        # GET — render the login form (or return 401 for API clients)
        if "text/html" in request.headers.get("Accept", ""):
            oidc_providers = []
            if hasattr(provider, "get_oidc_providers"):
                oidc_providers = await provider.get_oidc_providers()
            return HTMLResponse(_render_login(oidc_providers=oidc_providers))
        return JSONResponse(
            {"error": "Authentication required"},
            status_code=HTTP_401_UNAUTHORIZED,
        )

    # POST — authenticate
    form = await request.form()
    username = form.get("username", "")
    password = form.get("password", "")

    # Only SessionAuthProvider has a login method
    if hasattr(provider, "login"):
        user = await provider.login(username, password, request)
    else:
        return JSONResponse(
            {"error": "Login not supported by this auth provider"},
            status_code=HTTP_400_BAD_REQUEST,
        )

    if not user:
        if "text/html" in request.headers.get("Accept", ""):
            oidc_providers = []
            if hasattr(provider, "get_oidc_providers"):
                oidc_providers = await provider.get_oidc_providers()
            return HTMLResponse(
                _render_login(
                    "Invalid username or password", oidc_providers=oidc_providers
                ),
                status_code=HTTP_400_BAD_REQUEST,
            )
        return JSONResponse(
            {"error": "Invalid username or password"},
            status_code=HTTP_400_BAD_REQUEST,
        )

    next_url = request.query_params.get("next", "/")
    logger.info("User '%s' logged in via form", username)
    return RedirectResponse(next_url, status_code=HTTP_303_SEE_OTHER)


async def logout_endpoint(request: Request) -> RedirectResponse | JSONResponse:
    """Handle logout: clear session, redirect to login (GET) or return 200 (POST/XHR)."""
    provider = _get_provider(request)
    if hasattr(provider, "logout"):
        await provider.logout(request)
    # POST / XHR requests from the SPA expect a JSON response
    if request.method == "POST" or "application/json" in request.headers.get(
        "Accept", ""
    ):
        return JSONResponse({"ok": True})
    return RedirectResponse(
        provider.config.login_path,
        status_code=HTTP_303_SEE_OTHER,
    )


async def me_endpoint(request: Request) -> JSONResponse:
    """Return current user info for the UI.

    Returns 401 if not authenticated.

    Includes ``hasAnyAdminPermission`` so the UI can decide whether to
    show the admin portal navigation button.
    """
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse(
            {"error": "Not authenticated"},
            status_code=HTTP_401_UNAUTHORIZED,
        )

    # Check if user has ANY admin portal permission
    auth_provider = getattr(request.app.state, "auth_provider", None)
    has_any_admin = False
    if auth_provider:
        from dagster_webserver.admin.permissions import has_any_admin_permission

        perms = auth_provider.get_user_permissions(user)
        has_any_admin = has_any_admin_permission(perms)

    return JSONResponse(
        {
            "username": user.username,
            "role": user.role,
            "email": user.email,
            "displayName": user.display_name,
            "hasAnyAdminPermission": has_any_admin,
        }
    )
