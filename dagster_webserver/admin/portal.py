"""Admin portal — routes, middleware, and mounting.

Provides:
- ``AdminPortal`` — CRUD views mounted at ``/admin``
- ``AdminPortalMiddleware`` — enforces admin permission checks
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from jinja2 import Environment, FileSystemLoader
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.routing import Mount, Route
from starlette.status import (
    HTTP_303_SEE_OTHER,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_404_NOT_FOUND,
)
from starlette.status import HTTP_403_FORBIDDEN as HTTP_403
from starlette.templating import Jinja2Templates

from dagster_webserver.admin.permissions import has_any_admin_permission
from dagster_webserver.admin.views import BaseAdminView, RoleView, UserView

if TYPE_CHECKING:
    from dagster_webserver.auth.db_backend import DatabaseUserBackend

logger = logging.getLogger("dagster-webserver.admin")


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


class AdminPortalMiddleware(BaseHTTPMiddleware):
    """Enforce that the user has at least one admin portal permission.

    Relies on the top-level ``AuthMiddleware`` (on the parent app) having
    already resolved the user and set ``request.state.user``.
    """

    def __init__(self, app: Any) -> None:
        super().__init__(app)

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        user = getattr(request.state, "user", None)
        if not user:
            # AuthMiddleware should have already redirected to /login
            return JSONResponse(
                {"error": "Authentication required"},
                status_code=HTTP_401_UNAUTHORIZED,
            )

        # Resolve permissions directly from the AuthUser rather than
        # reaching into request.app.state.auth_provider (which is the
        # child admin sub-app, not the parent webserver where the
        # auth_provider lives).
        from dagster_webserver.admin.views import _resolve_authuser_permissions

        perms = _resolve_authuser_permissions(user)

        if not has_any_admin_permission(perms):
            return JSONResponse(
                {"error": "Forbidden — no admin portal permissions"},
                status_code=HTTP_403,
            )

        # Store resolved permissions for downstream views
        request.state.admin_permissions = perms
        return await call_next(request)


# ---------------------------------------------------------------------------
# AdminPortal
# ---------------------------------------------------------------------------


class AdminPortal:
    """Admin portal for managing users and roles.

    Mounts at ``/admin`` with its own route table and middleware.
    """

    def __init__(self, backend: DatabaseUserBackend) -> None:
        self.backend = backend
        self._views: list[BaseAdminView] = []
        self.routes: list[Route] = []
        self._templates: Jinja2Templates | None = None
        self._init_templates()
        self._init_views()
        self._init_routes()

    def _init_templates(self) -> None:
        templates_dir = __package__.replace(".", "/") + "/templates"
        import importlib

        pkg = importlib.import_module("dagster_webserver.admin")
        pkg_dir = getattr(pkg, "__path__", [__file__])[0]
        templates_dir = f"{pkg_dir}/templates"

        env = Environment(
            loader=FileSystemLoader(templates_dir),
            autoescape=True,
        )
        self._templates = Jinja2Templates(env=env)

    def _init_views(self) -> None:
        self._views = [
            UserView(self.backend),
            RoleView(self.backend),
        ]

    def _find_view(self, identity: str) -> BaseAdminView:
        for view in self._views:
            if view.identity == identity:
                return view
        raise HTTPException(HTTP_404_NOT_FOUND, f"Unknown view: {identity}")

    def _init_routes(self) -> None:
        self.routes = [
            Route("/", self._render_index, methods=["GET"], name="admin-index"),
            Route(
                "/{identity}/list",
                self._render_list,
                methods=["GET"],
                name="admin-list",
            ),
            Route(
                "/{identity}/detail/{pk}",
                self._render_detail,
                methods=["GET"],
                name="admin-detail",
            ),
            Route(
                "/{identity}/create",
                self._render_create,
                methods=["GET", "POST"],
                name="admin-create",
            ),
            Route(
                "/{identity}/edit/{pk}",
                self._render_edit,
                methods=["GET", "POST"],
                name="admin-edit",
            ),
            Route(
                "/{identity}/action",
                self.handle_action,
                methods=["POST"],
                name="admin-action",
            ),
            Route(
                "/{identity}/row-action",
                self.handle_row_action,
                methods=["POST"],
                name="admin-row-action",
            ),
        ]

    def mount_to(self, routes: list[Route]) -> None:
        """Mount admin routes into the main webserver route table."""
        admin_app = Starlette(
            routes=self.routes,
            middleware=[Middleware(AdminPortalMiddleware)],
            debug=False,
            exception_handlers={HTTPException: self._render_error},
        )
        admin_app.state.ROUTE_NAME = "admin"
        # Redirect /admin (no trailing slash) → /admin/ before the mount
        routes.insert(0, Route("/admin", self._redirect_to_index, methods=["GET"]))
        routes.insert(1, Mount("/admin", app=admin_app, name="admin"))

    # -- Error handler --

    async def _render_error(self, request: Request, exc: HTTPException) -> HTMLResponse:
        return HTMLResponse(
            f"<html><body><h1>{exc.status_code}</h1><p>{exc.detail}</p>"
            f'<a href="/">Back to Dagster</a></body></html>',
            status_code=exc.status_code,
        )

    # -- Route handlers --

    async def _redirect_to_index(self, request: Request) -> RedirectResponse:
        """Redirect /admin (no trailing slash) to /admin/."""
        return RedirectResponse(url="/admin/", status_code=302)

    async def _render_index(self, request: Request) -> HTMLResponse:
        views = [v for v in self._views if v.is_accessible(request)]
        return self._templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "views": views,
                "user": getattr(request.state, "user", None),
            },
        )

    async def _render_list(self, request: Request) -> HTMLResponse:
        identity = request.path_params.get("identity")
        view = self._find_view(identity)
        if not view.is_accessible(request):
            raise HTTPException(HTTP_403)

        items = await view.find_all(request)
        total = await view.count(request)
        serialized = [await view.serialize(item, request, "list") for item in items]
        actions = await view.get_all_actions(request)
        row_actions = await view.get_all_row_actions(request)

        return self._templates.TemplateResponse(
            request=request,
            name="list.html",
            context={
                "view": view,
                "views": self._views,
                "items": serialized,
                "total": total,
                "actions": actions,
                "row_actions": row_actions,
                "user": getattr(request.state, "user", None),
            },
        )

    async def _render_detail(self, request: Request) -> HTMLResponse:
        identity = request.path_params.get("identity")
        pk = request.path_params.get("pk")
        view = self._find_view(identity)
        if not view.is_accessible(request):
            raise HTTPException(HTTP_403)

        obj = await view.find_by_pk(request, pk)
        if obj is None:
            raise HTTPException(HTTP_404_NOT_FOUND)

        serialized = await view.serialize(obj, request, "detail")
        row_actions = await view.get_all_row_actions(request)

        return self._templates.TemplateResponse(
            request=request,
            name="detail.html",
            context={
                "view": view,
                "views": self._views,
                "obj": serialized,
                "pk": pk,
                "row_actions": row_actions,
                "user": getattr(request.state, "user", None),
            },
        )

    async def _render_create(self, request: Request) -> HTMLResponse | RedirectResponse:
        identity = request.path_params.get("identity")
        view = self._find_view(identity)
        if not view.is_accessible(request) or not view.can_create(request):
            raise HTTPException(HTTP_403)

        if request.method == "GET":
            # Fetch available roles for the role dropdown (users view only)
            roles = []
            if view.identity == "users":
                roles = [r.name for r in await self.backend.list_roles()]
            return self._templates.TemplateResponse(
                request=request,
                name="create.html",
                context={
                    "view": view,
                    "views": self._views,
                    "roles": roles,
                    "user": getattr(request.state, "user", None),
                },
            )

        form = await request.form()
        try:
            await view.create(request, dict(form))
        except ValueError as e:
            roles = []
            if view.identity == "users":
                roles = [r.name for r in await self.backend.list_roles()]
            return self._templates.TemplateResponse(
                request=request,
                name="create.html",
                context={
                    "view": view,
                    "views": self._views,
                    "form": dict(form),
                    "roles": roles,
                    "error": str(e),
                    "user": getattr(request.state, "user", None),
                },
                status_code=HTTP_400_BAD_REQUEST,
            )

        url = str(request.url_for("admin:admin-list", identity=view.identity))
        return RedirectResponse(url, status_code=HTTP_303_SEE_OTHER)

    async def _render_edit(self, request: Request) -> HTMLResponse | RedirectResponse:
        identity = request.path_params.get("identity")
        pk = request.path_params.get("pk")
        view = self._find_view(identity)
        if not view.is_accessible(request) or not view.can_edit(request):
            raise HTTPException(HTTP_403)

        obj = await view.find_by_pk(request, pk)
        if obj is None:
            raise HTTPException(HTTP_404_NOT_FOUND)

        if request.method == "GET":
            serialized = await view.serialize(obj, request, "edit")
            # Fetch available roles for the role dropdown (users view only)
            roles = []
            if view.identity == "users":
                roles = [r.name for r in await self.backend.list_roles()]
            return self._templates.TemplateResponse(
                request=request,
                name="edit.html",
                context={
                    "view": view,
                    "views": self._views,
                    "obj": serialized,
                    "pk": pk,
                    "roles": roles,
                    "user": getattr(request.state, "user", None),
                },
            )

        form = await request.form()
        try:
            await view.edit(request, pk, dict(form))
        except ValueError as e:
            serialized = await view.serialize(obj, request, "edit")
            roles = []
            if view.identity == "users":
                roles = [r.name for r in await self.backend.list_roles()]
            return self._templates.TemplateResponse(
                request=request,
                name="edit.html",
                context={
                    "view": view,
                    "views": self._views,
                    "obj": serialized,
                    "pk": pk,
                    "form": dict(form),
                    "roles": roles,
                    "error": str(e),
                    "user": getattr(request.state, "user", None),
                },
                status_code=HTTP_400_BAD_REQUEST,
            )

        url = str(request.url_for("admin:admin-list", identity=view.identity))
        return RedirectResponse(url, status_code=HTTP_303_SEE_OTHER)

    # -- Action handlers --

    async def handle_action(self, request: Request) -> Response:
        identity = request.path_params.get("identity")
        view = self._find_view(identity)
        if not view.is_accessible(request):
            raise HTTPException(HTTP_403)

        form = await request.form()
        name = form.get("action_name", "")
        pks = form.get("pks", "[]")
        try:
            pks = json.loads(pks)
        except json.JSONDecodeError:
            pks = []

        try:
            result = await view.handle_action(request, pks, name)
        except ValueError as e:
            url = str(request.url_for("admin:admin-list", identity=identity))
            return RedirectResponse(f"{url}?error={e}", status_code=HTTP_303_SEE_OTHER)
        if isinstance(result, Response):
            return result
        # Redirect back to list with flash message
        url = str(request.url_for("admin:admin-list", identity=identity))
        return RedirectResponse(f"{url}?msg={result}", status_code=HTTP_303_SEE_OTHER)

    async def handle_row_action(self, request: Request) -> Response:
        identity = request.path_params.get("identity")
        view = self._find_view(identity)
        if not view.is_accessible(request):
            raise HTTPException(HTTP_403)

        form = await request.form()
        name = form.get("action_name", "")
        pk = form.get("pk", "")

        try:
            result = await view.handle_row_action(request, pk, name)
        except ValueError as e:
            url = str(request.url_for("admin:admin-list", identity=identity))
            return RedirectResponse(f"{url}?error={e}", status_code=HTTP_303_SEE_OTHER)
        if isinstance(result, Response):
            return result
        url = str(request.url_for("admin:admin-list", identity=identity))
        return RedirectResponse(f"{url}?msg={result}", status_code=HTTP_303_SEE_OTHER)
