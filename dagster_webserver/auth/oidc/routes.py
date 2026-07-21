"""OIDC route handlers: /oidc/authorize, /oidc/callback.

These endpoints are injected into the DagsterWebserver route table
when a ``HybridSessionAuthProvider`` is configured.
"""

from __future__ import annotations

import logging

from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.status import HTTP_303_SEE_OTHER

from dagster_webserver.auth.oidc.models import SESSION_OIDC_REDIRECT
from dagster_webserver.auth.provider import HybridSessionAuthProvider, _is_safe_redirect

logger = logging.getLogger("dagster-webserver.auth.oidc")


def _get_provider(request: Request) -> HybridSessionAuthProvider | None:
    """Retrieve the HybridSessionAuthProvider from the app state."""
    provider = getattr(request.app.state, "auth_provider", None)
    if isinstance(provider, HybridSessionAuthProvider):
        return provider
    return None


async def oidc_authorize_endpoint(request: Request) -> RedirectResponse:
    """Initiate OIDC authorization: redirect to the IdP."""
    provider = _get_provider(request)
    if provider is None:
        return RedirectResponse(
            "/login?error=oidc_not_configured",
            status_code=HTTP_303_SEE_OTHER,
        )

    provider_name: str = request.path_params["provider_name"]
    return await provider.initiate_oidc_login(provider_name, request)


async def oidc_callback_endpoint(request: Request) -> RedirectResponse:
    """Handle OIDC callback: exchange code, verify token, set session."""
    provider = _get_provider(request)
    if provider is None:
        return RedirectResponse(
            "/login?error=oidc_not_configured",
            status_code=HTTP_303_SEE_OTHER,
        )

    user = await provider.handle_oidc_callback(request)
    if user:
        session: dict = getattr(request, "session", {}) or {}
        next_url = session.get(SESSION_OIDC_REDIRECT, "/")
        if not _is_safe_redirect(next_url):
            next_url = "/"
        logger.info(
            "OIDC login successful for '%s', redirecting to %s", user.username, next_url
        )
        return RedirectResponse(next_url, status_code=HTTP_303_SEE_OTHER)

    return RedirectResponse(
        "/login?error=oidc_callback_failed",
        status_code=HTTP_303_SEE_OTHER,
    )
