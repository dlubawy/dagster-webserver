"""Auth middleware for Starlette.

Wraps every request and enforces authentication via the configured
``BaseAuthProvider``.  Unauthenticated browser requests are redirected
to ``/login``; unauthenticated API requests receive a 401 response.
"""

from __future__ import annotations

import logging
from urllib.parse import urlencode

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.routing import Match, Route, WebSocketRoute
from starlette.status import HTTP_303_SEE_OTHER, HTTP_401_UNAUTHORIZED

from dagster_webserver.auth.provider import BaseAuthProvider

logger = logging.getLogger("dagster-webserver.auth")


class AuthMiddleware(BaseHTTPMiddleware):
    """Middleware that enforces authentication on all routes except
    those explicitly allowed without auth.
    """

    def __init__(self, app: object, provider: BaseAuthProvider) -> None:
        super().__init__(app)
        self.provider = provider

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Skip if auth is disabled
        if not self.provider.config.require_auth:
            # Still try to resolve user for permission purposes
            await self.provider.authenticate_request(request)
            return await call_next(request)

        # Check if route is allowed without auth
        if self._is_allowed_without_auth(request):
            return await call_next(request)

        # Try to authenticate
        user = await self.provider.authenticate_request(request)
        if user:
            return await call_next(request)

        # Not authenticated — return appropriate response
        if self._is_api_request(request):
            return JSONResponse(
                {"error": "Authentication required"},
                status_code=HTTP_401_UNAUTHORIZED,
            )
        # Browser request — redirect to login
        login_url = self._build_login_url(request)
        return RedirectResponse(login_url, status_code=HTTP_303_SEE_OTHER)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_allowed_without_auth(self, request: Request) -> bool:
        """Check if the current route is in the allow list."""
        allowed = self.provider.config.allowed_routes or []
        for route in request.app.routes:
            match, _ = route.matches(request.scope)
            if match == Match.FULL and isinstance(route, (Route, WebSocketRoute)):
                if route.name and route.name in allowed:
                    return True
                if route.path in allowed:
                    return True
                break
        return False

    def _is_api_request(self, request: Request) -> bool:
        """Heuristic: API requests ask for JSON or hit known API paths."""
        accept = request.headers.get("Accept", "")
        path = request.url.path
        return (
            "application/json" in accept
            or path.startswith("/graphql")
            or path.startswith("/api/")
            or path.startswith("/server_info")
            or path.startswith("/dagit_info")
            or path.startswith("/report_")
        )

    def _build_login_url(self, request: Request) -> str:
        """Build the login redirect URL with a ``next`` query param."""
        next_path = request.url.path
        query = urlencode({"next": next_path})
        return f"{self.provider.config.login_path}?{query}"
