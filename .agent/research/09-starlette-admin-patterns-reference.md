# Starlette-Admin Patterns Reference

## Overview

This document provides a detailed reference of the starlette-admin patterns we borrow for the Dagster admin portal. We do **not** embed starlette-admin as a dependency; instead, we build a lightweight implementation inspired by its architecture.

## Source Files Studied

| File | Purpose |
| -------------------------------- | ------------------------------------------------------------ |
| `starlette_admin/base.py` | `BaseAdmin` — route setup, template rendering, CRUD handlers |
| `starlette_admin/auth.py` | `BaseAuthProvider`, `AuthProvider`, `AuthMiddleware` |
| `starlette_admin/views.py` | `BaseModelView` — CRUD view class with permission hooks |
| `starlette_admin/actions.py` | `@action`, `@row_action`, `@link_row_action` decorators |
| `starlette_admin/fields.py` | Field definitions (we skip this — simpler fields suffice) |
| `examples/auth/provider.py` | Concrete auth provider with session-based login |
| `examples/auth/view.py` | Permission-gated view with `can_create`, `can_edit`, etc. |
| `examples/custom_actions/app.py` | Custom batch actions with `@action` decorator |

## Core Architecture Pattern

### BaseAdmin — Route Bootstrap

```python
# starlette_admin/base.py

class BaseAdmin:
    def __init__(
        self,
        title: str = "Admin",
        base_url: str = "/admin",
        route_name: str = "admin",
        auth_provider: Optional[BaseAuthProvider] = None,
        middlewares: Optional[Sequence[Middleware]] = None,
        # ...
    ):
        self._views: List[BaseView] = []
        self._models: List[BaseModelView] = []
        self.routes: List[Union[Route, Mount]] = []
        self.init_routes()

    def init_routes(self):
        self.routes.extend([
            Route(self.index_view.path, self._render_custom_view(self.index_view), name="index"),
            Route("/api/{identity}", self._render_api, methods=["GET"], name="api"),
            Route("/api/{identity}/action", self.handle_action, methods=["GET", "POST"], name="action"),
            Route("/api/{identity}/row-action", self.handle_row_action, methods=["GET", "POST"], name="row-action"),
            Route("/{identity}/list", self._render_list, methods=["GET"], name="list"),
            Route("/{identity}/detail/{pk}", self._render_detail, methods=["GET"], name="detail"),
            Route("/{identity}/create", self._render_create, methods=["GET", "POST"], name="create"),
            Route("/{identity}/edit/{pk}", self._render_edit, methods=["GET", "POST"], name="edit"),
        ])

    def mount_to(self, app: Starlette, redirect_slashes: bool = True):
        admin_app = Starlette(
            routes=self.routes,
            middleware=self.middlewares,
            exception_handlers={HTTPException: self._render_error},
        )
        app.mount(self.base_url, app=admin_app, name=self.route_name)
```

**Key insight**: Starlette-admin creates a separate `Starlette` sub-app and mounts it at `/admin`. This isolates admin routes from the main app. We follow this same pattern.

### BaseModelView — CRUD View Class

```python
# starlette_admin/views.py

class BaseModelView(BaseView):
    identity: Optional[str] = None           # URL identity
    name: Optional[str] = None               # Display name
    fields: Sequence[BaseField] = []         # Field definitions
    pk_attr: Optional[str] = None            # Primary key field

    # Permission hooks
    def is_accessible(self, request: Request) -> bool:
        return True

    def can_view_details(self, request: Request) -> bool:
        return True

    def can_create(self, request: Request) -> bool:
        return True

    def can_edit(self, request: Request) -> bool:
        return True

    def can_delete(self, request: Request) -> bool:
        return True

    # Abstract CRUD methods
    @abstractmethod
    async def find_all(self, request, skip=0, limit=100, where=None, order_by=None): ...

    @abstractmethod
    async def count(self, request, where=None): ...

    @abstractmethod
    async def find_by_pk(self, request, pk): ...

    @abstractmethod
    async def create(self, request, data: Dict) -> Any: ...

    @abstractmethod
    async def edit(self, request, pk, data: Dict[str, Any]) -> Any: ...

    @abstractmethod
    async def delete(self, request, pks: List[Any]) -> Optional[int]: ...

    # Lifecycle hooks
    async def before_create(self, request, data, obj): ...
    async def after_create(self, request, obj): ...
    async def before_edit(self, request, data, obj): ...
    async def after_edit(self, request, obj): ...
    async def before_delete(self, request, obj): ...
    async def after_delete(self, request, obj): ...
```

**Key insight**: The permission hooks (`can_create`, `can_edit`, etc.) are called at every layer — route handler, template rendering, and API endpoint. This ensures consistent enforcement.

### CRUD Route Handlers

```python
# starlette_admin/base.py — _render_list

async def _render_list(self, request: Request) -> Response:
    request.state.action = RequestAction.LIST
    identity = request.path_params.get("identity")
    model = self._find_model_from_identity(identity)
    if not model.is_accessible(request):
        raise HTTPException(HTTP_403_FORBIDDEN)
    return self.templates.TemplateResponse(
        request=request,
        name=model.list_template,
        context={"model": model, "title": model.title(request), ...},
    )

# starlette_admin/base.py — _render_create

async def _render_create(self, request: Request) -> Response:
    request.state.action = RequestAction.CREATE
    identity = request.path_params.get("identity")
    model = self._find_model_from_identity(identity)
    if not model.is_accessible(request) or not model.can_create(request):
        raise HTTPException(HTTP_403_FORBIDDEN)

    if request.method == "GET":
        return self.templates.TemplateResponse(request=request, name=model.create_template, ...)

    form = await request.form()
    dict_obj = await self.form_to_dict(request, form, model, RequestAction.CREATE)
    try:
        obj = await model.create(request, dict_obj)
    except FormValidationError as exc:
        return self.templates.TemplateResponse(..., status_code=HTTP_422)

    pk = await model.get_pk_value(request, obj)
    url = request.url_for(self.route_name + ":list", identity=model.identity)
    return RedirectResponse(url, status_code=HTTP_303_SEE_OTHER)
```

**Key insight**: GET renders the form, POST processes it. On success, redirect to list (POST-redirect-GET pattern). On validation error, re-render form with errors.

### API Endpoint (AJAX Data Loading)

```python
# starlette_admin/base.py — _render_api

async def _render_api(self, request: Request) -> Response:
    identity = request.path_params.get("identity")
    model = self._find_model_from_identity(identity)
    if not model.is_accessible(request):
        return JSONResponse(None, status_code=HTTP_403_FORBIDDEN)

    skip = int(request.query_params.get("skip") or "0")
    limit = int(request.query_params.get("limit") or "100")
    order_by = request.query_params.getlist("order_by")
    where = request.query_params.get("where")

    items = await model.find_all(request=request, skip=skip, limit=limit, where=where, order_by=order_by)
    total = await model.count(request=request, where=where)

    serialized_items = [await model.serialize(item, request, RequestAction.LIST) for item in items]

    return JSONResponse({"items": serialized_items, "total": total})
```

**Key insight**: The API endpoint returns JSON for AJAX-powered DataTables. We can use a simpler approach (server-side rendering) for the MVP.

### Actions — Batch and Row Operations

```python
# starlette_admin/actions.py

def action(
    name: str,
    text: str,
    confirmation: Optional[str] = None,
    submit_btn_class: Optional[str] = "btn-primary",
    submit_btn_text: Optional[str] = "Yes, Proceed",
    icon_class: Optional[str] = None,
    form: Optional[str] = None,
    custom_response: Optional[bool] = False,
) -> Callable:
    """Decorator for batch actions."""
    def wrap(f):
        f._action = {
            "name": name, "text": text, "confirmation": confirmation,
            "submit_btn_text": submit_btn_text, "submit_btn_class": submit_btn_class,
            "icon_class": icon_class, "form": form or "", "custom_response": custom_response,
        }
        return f
    return wrap

def row_action(
    name: str,
    text: str,
    confirmation: Optional[str] = None,
    icon_class: Optional[str] = None,
    exclude_from_list: bool = False,
    exclude_from_detail: bool = False,
) -> Callable:
    """Decorator for per-row actions."""
    def wrap(f):
        f._row_action = {
            "name": name, "text": text, "confirmation": confirmation,
            "icon_class": icon_class, "exclude_from_list": exclude_from_list,
            "exclude_from_detail": exclude_from_detail,
        }
        return f
    return wrap
```

**Key insight**: Actions are registered via decorators and collected in `_init_actions()`. This pattern is clean and extensible.

### Auth Middleware

```python
# starlette_admin/auth.py

class AuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, provider, allow_paths=None, allow_routes=None):
        super().__init__(app)
        self.provider = provider
        self.allow_routes = list(allow_routes or []) + ["login", "statics"]

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Find current route
        for route in request.app.routes:
            match, _ = route.matches(request.scope)
            if match == Match.FULL:
                current_route = route
                break

        # Check if route is allowed without auth
        if (current_route.path in self.allow_paths
            or current_route.name in self.allow_routes
            or getattr(current_route.endpoint, "_login_not_required", False)
            or await self.provider.is_authenticated(request)):
            return await call_next(request)

        # Redirect to login
        return RedirectResponse(
            f"{login_url}?next={request.url}",
            status_code=HTTP_303_SEE_OTHER,
        )
```

**Key insight**: The middleware checks route-level allowlists AND calls `provider.is_authenticated()`. We adapt this to check our admin-specific permissions.

### Permission-Gated View Example

```python
# examples/auth/view.py

class ArticleView(ModelView):
    def can_view_details(self, request: Request) -> bool:
        return "read" in request.state.user["roles"]

    def can_create(self, request: Request) -> bool:
        return "create" in request.state.user["roles"]

    def can_edit(self, request: Request) -> bool:
        return "edit" in request.state.user["roles"]

    def can_delete(self, request: Request) -> bool:
        return "delete" in request.state.user["roles"]

    async def is_action_allowed(self, request: Request, name: str) -> bool:
        if name == "make_published":
            return "action_make_published" in request.state.user["roles"]
        return await super().is_action_allowed(request, name)

    @action(
        name="make_published",
        text="Mark selected articles as published",
        confirmation="Are you sure?",
        submit_btn_class="btn-success",
    )
    async def make_published_action(self, request: Request, pks: List[Any]) -> str:
        session: Session = request.state.session
        for article in await self.find_by_pks(request, pks):
            article.status = Status.Published
            session.add(article)
        session.commit()
        return f"{len(pks)} articles were successfully marked as published"
```

**Key insight**: Permission checks read from `request.state.user` (set by auth middleware). We adapt this to read from `request.state.admin_permissions`.

## What We Adapt vs. What We Skip

### Adapted (Core Patterns)

| Pattern | How We Use It |
| ------------------------------------- | ------------------------------------------------- |
| `BaseAdmin` route bootstrap | `AdminPortal` class with same route patterns |
| `BaseModelView` CRUD interface | `BaseAdminView` with same abstract methods |
| Permission hooks (`can_create`, etc.) | Same hooks, checking `admin_permissions` dict |
| `@action` / `@row_action` decorators | Same pattern for batch/row operations |
| Mount-as-sub-app | Mount admin at `/admin` as separate Starlette app |
| Auth middleware pattern | `AdminAuthMiddleware` checking admin permissions |
| POST-redirect-GET | Same pattern for create/edit forms |
| Template rendering | Jinja2 templates (simpler than starlette-admin's) |

### Skipped (Not Needed)

| Feature | Why We Skip It |
| ------------------------------------- | -------------------------------------------------- |
| `BaseField` system | We use simple form fields, not a field abstraction |
| `serialize()` method | We render data directly in templates |
| `Select2` integration | Not needed for users/roles |
| Export (CSV/Excel/PDF) | Out of scope for MVP |
| i18n/l10n | Out of scope for MVP |
| Search builder | Simple text search suffices |
| File upload fields | Not needed for users/roles |
| Relation fields | User↔Role is a simple FK, handled by backend |
| DataTables.js AJAX | Server-side rendering for MVP; can add AJAX later |
| Custom views with arbitrary templates | We only need list/detail/create/edit |
| DropDown menu grouping | Flat sidebar navigation |
| Column visibility toggles | Fixed columns for now |
| State saving (localStorage) | Not needed for MVP |

## Template Architecture

Starlette-admin uses Jinja2 templates with a base layout:

```
starlette_admin/templates/
├── base.html              # Base layout: sidebar, header, content
├── login.html             # Login page
├── list.html              # List view with DataTables
├── detail.html            # Detail view
├── create.html            # Create form
├── edit.html              # Edit form
├── row-actions.html       # Per-row action buttons
└── error.html             # Error page
```

We follow the same structure but with simpler templates:

```
dagster_webserver/admin/templates/
├── base.html              # Base layout: sidebar nav, header, content area
├── dashboard.html         # Admin dashboard with summary stats
├── list.html              # List view: table with search, sort, paginate
├── detail.html            # Detail view: field display, row actions
├── create.html            # Create form
├── edit.html              # Edit form
└── error.html             # Error page
```

## Comparison Summary

| Aspect | Starlette-Admin | Our Admin Portal |
| ----------- | ----------------------------------------- | ----------------------------------- |
| Framework | Full-featured admin framework | Lightweight, purpose-built |
| ORM support | SQLAlchemy, MongoEngine, ODMantic, Beanie | Our own `DatabaseUserBackend` |
| Views | Generic (any ORM model) | Specific (users, roles) |
| Auth | Built-in `BaseAuthProvider` | Reuses Dagster's auth system |
| Templates | Jinja2 + DataTables.js | Jinja2 + simple HTML tables |
| Permissions | `can_create`, `can_edit`, etc. | Same pattern + admin-specific perms |
| Actions | `@action`, `@row_action` | Same pattern |
| i18n | Built-in with Babel | Not in MVP |
| Export | CSV, Excel, PDF, Print | Not in MVP |
| Mounting | `admin.mount_to(app)` | `portal.mount_to(routes)` |
