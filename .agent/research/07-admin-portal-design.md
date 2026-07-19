# Admin Portal Design — Adding CRUD Operations for Admin Users

## Overview

This document researches adding an **admin portal** to dagster-webserver that exposes CRUD operations for managing the entities that power the auth/RBAC system: **users**, **roles**, and **custom permissions**. The portal is only accessible to users whose role grants at least one admin permission.

## Problem Statement

The existing auth system (`dagster_webserver/auth/`) provides:

- Authentication (login/logout/session management)
- Role-based permission resolution
- User backends (in-memory, file-based, database)

However, there is **no UI for administering these entities at runtime**. Currently, users and roles must be configured:

- At startup via CLI flags / config files
- By editing YAML/JSON files and restarting
- By directly manipulating the database

An admin portal solves this by providing a web-based interface for:

1. **User management**: Create, edit, deactivate users; assign roles
1. **Role management**: Create custom roles; edit permissions; delete unused roles
1. **Permission management**: Fine-grained control over which permissions each role has

## Design Goals

1. **Minimal footprint** — no heavy admin framework dependency; build lightweight on top of Starlette
1. **Consistent with Dagster UI** — same design tokens, fonts, and feel as the main UI
1. **Permission-gated with a flat model** — no separate "portal access" permission. Having any admin capability (even just viewing users) grants portal entry. Inside, each view and operation is gated by its own specific permission.
1. **Database-backed** — operates on `DatabaseUserBackend` (SQLite/PostgreSQL); file/in-memory backends are read-only
1. **Extensible** — easy to add new admin views (e.g., API tokens, audit log)

## Architecture

### Where the Admin Portal Lives

```
dagster_webserver/
├── admin/                          # NEW: Admin portal package
│   ├── __init__.py
│   ├── portal.py                   # AdminPortal class (routes, middleware, mount)
│   ├── views.py                    # UserView, RoleView (CRUD view classes)
│   ├── permissions.py              # AdminPermission enum + helper functions
│   ├── templates/                  # Jinja2 templates
│   │   ├── base.html               # Base layout with sidebar
│   │   ├── dashboard.html          # Admin dashboard
│   │   ├── list.html               # Generic list view
│   │   ├── detail.html             # Generic detail view
│   │   ├── create.html             # Generic create form
│   │   └── edit.html               # Generic edit form
│   └── static/                     # Admin-specific static files
│       └── admin.css
```

### Request Flow

```
GET /admin/users
  → DagsterWebserver route matching
    → Mount("/admin", admin_app)
      → AdminPortalMiddleware (checks: does user have ANY admin permission?)
        → If no admin permissions → 403 Forbidden
        → If yes → proceed, store perms on request.state.admin_permissions
      → UserView._render_list()
        → UserView.is_accessible() → checks ADMIN_VIEW_USERS
          → If no → 403 (user can enter portal but can't see this view)
          → If yes → proceed
        → UserView.find_all() → DatabaseUserBackend.list_users()
        → Render list.html with user data
        → "Create User" button? → can_create() → ADMIN_EDIT_USERS
        → "Edit" row action? → can_edit() → ADMIN_EDIT_USERS
        → "Delete" row action? → can_delete() → ADMIN_EDIT_USERS
```

### Integration Points

1. **Route mounting** — `DagsterWebserver.build_routes()` conditionally mounts the admin portal at `/admin`
1. **Auth middleware** — reuses existing `AuthMiddleware` for authentication; adds `AdminPortalMiddleware` for authorization
1. **User backend** — operates on `DatabaseUserBackend` which already has CRUD methods
1. **UI navigation** — adds an "Admin" nav item in the bottom nav (only visible to users with any admin permission)
1. **Permission model** — flat admin permissions, no gateway. Each view/operation has its own permission.

## Starlette-Admin Patterns (Adapted)

Starlette-admin provides a comprehensive admin framework. We borrow its patterns but build a lighter implementation:

### What We Borrow

| Pattern | Starlette-Admin | Our Adaptation |
| ------------------ | ----------------------------------------------------------------- | ------------------------------------------------------------ |
| View classes | `BaseModelView` with `can_create`, `can_edit`, `can_delete` | `BaseAdminView` with same permission methods |
| Actions | `@action`, `@row_action` decorators | Same pattern for batch/row operations |
| Route structure | `/admin/{identity}/list`, `/detail/{pk}`, `/create`, `/edit/{pk}` | Same URL patterns |
| Auth middleware | `AuthMiddleware` with `is_authenticated()` | Reuse existing `AuthMiddleware` + add admin permission check |
| Template rendering | Jinja2 with DataTables | Jinja2 with simpler table rendering |
| API endpoints | `/admin/api/{identity}` for AJAX | Same pattern for dynamic table loading |

### What We Don't Use

| Starlette-Admin Feature | Why We Skip It |
| ------------------------------------------------------- | ------------------------------------------- |
| Generic ORM integration (SQLAlchemy, MongoEngine, etc.) | We operate on our own `DatabaseUserBackend` |
| i18n/l10n | Out of scope for MVP |
| Export (CSV, Excel, PDF) | Out of scope for MVP |
| Search builder | Simple text search is sufficient |
| File upload fields | Not needed for users/roles |
| Relation fields (HasOne, HasMany) | User↔Role is a simple foreign key |
| Custom views with arbitrary templates | We only need list/detail/create/edit |

## Admin Portal View Classes

### BaseAdminView

```python
class BaseAdminView:
    """Base class for admin portal views.

    Each view manages one entity type (users, roles) and provides
    list, detail, create, edit, and delete operations.
    """

    identity: str = ""              # URL segment (e.g., "users")
    label: str = ""                 # Singular label (e.g., "User")
    plural_label: str = ""          # Plural label (e.g., "Users")
    icon: str = ""                  # Icon name for sidebar

    # Field definitions
    list_columns: list[str] = []    # Columns shown in list view
    detail_fields: list[str] = []   # Fields shown in detail view
    create_fields: list[str] = []   # Fields in create form
    edit_fields: list[str] = []     # Fields in edit form

    def is_accessible(self, request: Request) -> bool:
        """Can the user access this view at all? (checks VIEW permission)"""
        return True

    def can_create(self, request: Request) -> bool:
        """Can the user create new items?"""
        return True

    def can_edit(self, request: Request) -> bool:
        """Can the user edit existing items?"""
        return True

    def can_delete(self, request: Request) -> bool:
        """Can the user delete items?"""
        return True

    # Abstract methods to implement
    @abstractmethod
    async def find_all(self, request, skip=0, limit=100, where=None, order_by=None):
        ...

    @abstractmethod
    async def count(self, request, where=None):
        ...

    @abstractmethod
    async def find_by_pk(self, request, pk):
        ...

    @abstractmethod
    async def create(self, request, data):
        ...

    @abstractmethod
    async def edit(self, request, pk, data):
        ...

    @abstractmethod
    async def delete(self, request, pks):
        ...
```

### UserView

```python
class UserView(BaseAdminView):
    identity = "users"
    label = "User"
    plural_label = "Users"
    icon = "users"

    list_columns = ["username", "display_name", "email", "role", "is_active", "created_at"]
    detail_fields = ["username", "display_name", "email", "role", "is_active", "created_at", "updated_at"]
    create_fields = ["username", "password", "role", "email", "display_name"]
    edit_fields = ["username", "password", "role", "email", "display_name", "is_active"]

    def __init__(self, backend: DatabaseUserBackend):
        self._backend = backend

    def is_accessible(self, request: Request) -> bool:
        # EDIT_USERS implies VIEW_USERS
        return can_view_users(getattr(request.state, "admin_permissions", {}))

    def can_create(self, request: Request) -> bool:
        return can_edit_users(getattr(request.state, "admin_permissions", {}))

    def can_edit(self, request: Request) -> bool:
        return can_edit_users(getattr(request.state, "admin_permissions", {}))

    def can_delete(self, request: Request) -> bool:
        return can_edit_users(getattr(request.state, "admin_permissions", {}))

    async def find_all(self, request, skip=0, limit=100, where=None, order_by=None):
        users = await self._backend.list_users()
        return self._apply_query(users, where, order_by, skip, limit)

    async def count(self, request, where=None):
        users = await self._backend.list_users()
        return len(self._filter_users(users, where))

    async def find_by_pk(self, request, pk):
        return await self._backend.get_user(pk)  # pk = username

    async def create(self, request, data):
        return await self._backend.create_user(
            username=data["username"],
            password=data["password"],
            role=data.get("role", "viewer"),
            email=data.get("email"),
            display_name=data.get("display_name"),
        )

    async def edit(self, request, pk, data):
        return await self._backend.update_user(
            pk,
            password=data.get("password") or None,
            role=data.get("role"),
            email=data.get("email"),
            display_name=data.get("display_name"),
            is_active=data.get("is_active"),
        )

    async def delete(self, request, pks):
        for username in pks:
            await self._backend.delete_user(username)
        return len(pks)
```

### RoleView

```python
class RoleView(BaseAdminView):
    identity = "roles"
    label = "Role"
    plural_label = "Roles"
    icon = "shield"

    list_columns = ["name", "is_builtin", "user_count", "created_at"]
    detail_fields = ["name", "is_builtin", "permissions", "created_at", "updated_at"]
    create_fields = ["name", "permissions"]
    edit_fields = ["permissions"]  # name is read-only

    def __init__(self, backend: DatabaseUserBackend):
        self._backend = backend

    def is_accessible(self, request: Request) -> bool:
        # EDIT_ROLES implies VIEW_ROLES
        return can_view_roles(getattr(request.state, "admin_permissions", {}))

    def can_create(self, request: Request) -> bool:
        return can_edit_roles(getattr(request.state, "admin_permissions", {}))

    def can_edit(self, request: Request) -> bool:
        return can_edit_roles(getattr(request.state, "admin_permissions", {}))

    def can_delete(self, request: Request) -> bool:
        return can_edit_roles(getattr(request.state, "admin_permissions", {}))

    async def find_all(self, request, skip=0, limit=100, where=None, order_by=None):
        roles = await self._backend.list_roles()
        return self._apply_query(roles, where, order_by, skip, limit)

    async def count(self, request, where=None):
        roles = await self._backend.list_roles()
        return len(roles)

    async def find_by_pk(self, request, pk):
        return await self._backend.get_role(pk)  # pk = role name

    async def create(self, request, data):
        return await self._backend.create_role(
            name=data["name"],
            permissions=data["permissions"],
        )

    async def edit(self, request, pk, data):
        role = await self._backend.get_role(pk)
        if role.is_builtin:
            raise ValueError("Built-in roles cannot be modified")
        return await self._backend.update_role(pk, permissions=data.get("permissions"))

    async def delete(self, request, pks):
        for name in pks:
            await self._backend.delete_role(name)
        return len(pks)
```

## Admin Portal Permissions (Consolidated Model)

### Permission Keys

4 permissions total — 2 per entity. **Edit implies view**.

```python
# dagster_webserver/admin/permissions.py

from enum import Enum, unique

@unique
class AdminPermission(str, Enum):
    ADMIN_VIEW_USERS = "admin_view_users"
    ADMIN_EDIT_USERS = "admin_edit_users"  # implies VIEW_USERS
    ADMIN_VIEW_ROLES = "admin_view_roles"
    ADMIN_EDIT_ROLES = "admin_edit_roles"  # implies VIEW_ROLES

# Resolution helpers — EDIT implies VIEW
def can_view_users(perms) -> bool:
    return _has(perms, "ADMIN_EDIT_USERS") or _has(perms, "ADMIN_VIEW_USERS")

def can_edit_users(perms) -> bool:
    return _has(perms, "ADMIN_EDIT_USERS")

def can_view_roles(perms) -> bool:
    return _has(perms, "ADMIN_EDIT_ROLES") or _has(perms, "ADMIN_VIEW_ROLES")

def can_edit_roles(perms) -> bool:
    return _has(perms, "ADMIN_EDIT_ROLES")

def has_any_admin_permission(perms) -> bool:
    return any(_has(perms, p.value) for p in AdminPermission)
```

### Default Role Assignments

| Permission | CATALOG_VIEWER | VIEWER | LAUNCHER | EDITOR | ADMIN |
| ------------------ | -------------- | ------ | -------- | ------ | ----- |
| `ADMIN_EDIT_USERS` | ✗ | ✗ | ✗ | ✗ | ✓ |
| `ADMIN_EDIT_ROLES` | ✗ | ✗ | ✗ | ✗ | ✓ |

Only **ADMIN** gets admin permissions — and it gets the two edit permissions, which implicitly grant view. Custom roles can grant any subset.

### Inspiration from Dagster+ Cloud

| Dagster+ Permission | Our Admin Portal Equivalent |
| ---------------------- | ---------------------------------- |
| `EDIT_USERS_AND_TEAMS` | `ADMIN_EDIT_USERS` (implies view) |
| `EDIT_CUSTOM_ROLES` | `ADMIN_EDIT_ROLES` (implies view) |
| `MANAGE_SERVICE_USERS` | Future: `ADMIN_EDIT_SERVICE_USERS` |
| `READ_AUDIT_LOG` | Future: `ADMIN_VIEW_AUDIT_LOG` |

## UI Integration — Admin Portal Button

### Navigation Button (Inspired by Logout Button Patch)

The `patches/add-logout-button.patch` shows the pattern for adding a nav item. We follow the same pattern but **conditionally render** based on whether the user has **any** admin permission:

```tsx
const AdminPortalItem = () => {
  const { isCollapsed } = useContext(NavCollapseContext);
  const { data: user } = useCurrentUser();

  // Only show if user has ANY admin permission
  if (!user?.hasAnyAdminPermission) {
    return null;
  }

  return (
    <NavItemWithLink
      icon={<Icon name="settings_admin" />}
      label="Admin"
      href="/admin"
      isActive={(_, currentLocation) =>
        currentLocation.pathname.startsWith("/admin")
      }
    />
  );
};
```

### Backend Support — `/api/me` Response

Extended to include `hasAnyAdminPermission`:

```python
async def me_endpoint(request: Request) -> JSONResponse:
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"error": "Not authenticated"}, status_code=401)

    auth_provider = getattr(request.app.state, "auth_provider", None)
    has_any_admin = False
    if auth_provider:
        perms = auth_provider.get_user_permissions(user)
        has_any_admin = has_any_admin_permission(perms)  # checks ANY AdminPermission

    return JSONResponse({
        "username": user.username,
        "role": user.role,
        "email": user.email,
        "displayName": user.display_name,
        "hasAnyAdminPermission": has_any_admin,  # NEW
    })
```

### Navigation Placement

The admin portal button goes in the **bottom navigation group**, between Collapse and Settings:

```tsx
const adminGroup = {
  key: "support",
  items: [
    { key: "collapse", label: "Collapse", element: <CollapseItem /> },
    { key: "admin", label: "Admin", element: <AdminPortalItem /> }, // NEW
    { key: "settings", label: "Settings", element: <SettingsItem /> },
    { key: "support", label: "Support", element: <SupportItem /> },
  ],
};
```

The `AdminPortalItem` component returns `null` for users with no admin permissions.

## Safety Rules

### Preventing Admin Lockout

1. **Cannot delete last admin**: If a deletion would leave zero users with any admin permission, block it
1. **Cannot self-demote**: A user cannot change their own role to one with no admin permissions
1. **Cannot delete self**: A user cannot delete their own account
1. **Built-in roles are immutable**: Cannot edit or delete built-in roles

## Implementation Plan

### Phase 1: Core Portal Infrastructure

1. Create `dagster_webserver/admin/` package
1. Define `AdminPermission` enum and `has_any_admin_permission()` helper
1. Implement `AdminPortal` class with route mounting
1. Implement `AdminPortalMiddleware` (checks any admin permission)
1. Create base Jinja2 templates

### Phase 2: User Management

6. Implement `UserView` with full CRUD
1. Wire up to `DatabaseUserBackend`
1. Add safety rules (no self-delete, no last-admin-delete)

### Phase 3: Role Management

9. Implement `RoleView` with CRUD (protecting built-in roles)
1. Build permission checkbox form for role editing

### Phase 4: UI Integration

11. Extend `/api/me` to include `hasAnyAdminPermission`
01. Add `AdminPortalItem` to navigation (conditional rendering)
01. Style admin templates to match Dagster UI design tokens

### Phase 5: Polish

14. Add search/filter to list views
01. Add batch delete action
01. Add audit logging for admin actions
01. Add error handling and user-friendly messages
