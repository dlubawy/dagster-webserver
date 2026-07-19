# Admin Portal Permissions — Consolidated Model

## Overview

The admin portal uses **4 permissions total** — 2 per managed entity (users, roles). Each entity has a **view** permission and an **edit** permission. **Edit implies view**: if a user can edit users, they can also list and view them.

## Permission Keys

```python
# dagster_webserver/admin/permissions.py

from enum import Enum, unique

@unique
class AdminPermission(str, Enum):
    """Permissions for the admin portal.

    Two permissions per entity:
      - VIEW  — list and read
      - EDIT  — create, edit, delete (implicitly also grants VIEW)

    Having ANY of these permissions grants entry to /admin.
    """

    ADMIN_VIEW_USERS = "admin_view_users"
    ADMIN_EDIT_USERS = "admin_edit_users"  # implies ADMIN_VIEW_USERS

    ADMIN_VIEW_ROLES = "admin_view_roles"
    ADMIN_EDIT_ROLES = "admin_edit_roles"  # implies ADMIN_VIEW_ROLES
```

### Permission Descriptions

| Permission | Grants | Controls |
| ------------------ | ------------------------------------------------ | ----------------------------------------------------------------- |
| `ADMIN_VIEW_USERS` | List users, view user details | `/admin/users` list, `/admin/users/{id}` detail, "Users" nav link |
| `ADMIN_EDIT_USERS` | Create, edit, delete users **+** view (implicit) | All user mutations + everything `ADMIN_VIEW_USERS` grants |
| `ADMIN_VIEW_ROLES` | List roles, view role details | `/admin/roles` list, `/admin/roles/{id}` detail, "Roles" nav link |
| `ADMIN_EDIT_ROLES` | Create, edit, delete roles **+** view (implicit) | All role mutations + everything `ADMIN_VIEW_ROLES` grants |

### Implication Rules

```
ADMIN_EDIT_USERS  ──implies──>  ADMIN_VIEW_USERS
ADMIN_EDIT_ROLES  ──implies──>  ADMIN_VIEW_ROLES
```

These implications are enforced at the **resolution layer**, not the storage layer. When checking permissions, the code always checks edit first, then falls back to view:

```python
def can_view_users(perms: dict[str, PermissionResult]) -> bool:
    """Can the user view users? EDIT implies VIEW."""
    return _has(perms, "ADMIN_EDIT_USERS") or _has(perms, "ADMIN_VIEW_USERS")

def can_edit_users(perms: dict[str, PermissionResult]) -> bool:
    """Can the user create/edit/delete users?"""
    return _has(perms, "ADMIN_EDIT_USERS")

def can_view_roles(perms: dict[str, PermissionResult]) -> bool:
    """Can the user view roles? EDIT implies VIEW."""
    return _has(perms, "ADMIN_EDIT_ROLES") or _has(perms, "ADMIN_VIEW_ROLES")

def can_edit_roles(perms: dict[str, PermissionResult]) -> bool:
    """Can the user create/edit/delete roles?"""
    return _has(perms, "ADMIN_EDIT_ROLES")

def has_any_admin_permission(perms: dict[str, PermissionResult]) -> bool:
    """Portal entry: user must have ANY admin permission."""
    return any(_has(perms, p.value) for p in AdminPermission)
```

### What This Enables

| Role Type | Permissions Set | What They See |
| ----------------- | -------------------------------------- | -------------------------------------------------------------- |
| Full admin | `ADMIN_EDIT_USERS`, `ADMIN_EDIT_ROLES` | Everything — lists, details, create/edit/delete buttons |
| User manager | `ADMIN_EDIT_USERS` | Users: full CRUD. Roles: nothing. |
| User viewer | `ADMIN_VIEW_USERS` | Users: list + detail only (no action buttons). Roles: nothing. |
| Role designer | `ADMIN_EDIT_ROLES` | Roles: full CRUD. Users: nothing. |
| Read-only auditor | `ADMIN_VIEW_USERS`, `ADMIN_VIEW_ROLES` | Both lists + details. No action buttons anywhere. |
| No admin perms | _(none)_ | No admin button in nav. `/admin` → 403. |

## Integration with Existing Permission System

### How Admin Permissions Are Stored

Admin permissions are stored alongside the existing 21 Dagster `Permissions` in the `PermissionResult` map returned by `BaseAuthProvider.get_user_permissions()`:

```python
def get_user_permissions(self, user: AuthUser) -> dict[str, PermissionResult]:
    perms = get_role_permissions(Role(user.role))
    admin_perms = self._get_admin_permissions(user)
    perms.update(admin_perms)
    return perms
```

### Built-in Roles

```python
ADMIN_ROLE_PERMISSIONS: dict[Role, dict[str, bool]] = {
    Role.ADMIN: {
        "ADMIN_EDIT_USERS": True,
        "ADMIN_EDIT_ROLES": True,
    },
    # All other roles: no admin permissions by default
    Role.CATALOG_VIEWER: {},
    Role.VIEWER: {},
    Role.LAUNCHER: {},
    Role.EDITOR: {},
}
```

Only **ADMIN** gets admin permissions — and it gets the two edit permissions, which implicitly grant view as well.

### Custom Roles

```python
# User viewer — can list users but not modify
custom_permissions = {
    "admin_view_users": True,
}

# Full user manager — can do everything with users
custom_permissions = {
    "admin_edit_users": True,  # implies view
}

# Read-only auditor — can view both users and roles
custom_permissions = {
    "admin_view_users": True,
    "admin_view_roles": True,
}
```

## Permission Enforcement Points

### 1. AdminPortalMiddleware — Portal Entry

```python
class AdminPortalMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        user = getattr(request.state, "user", None)
        if not user:
            return JSONResponse({"error": "Authentication required"}, status_code=401)

        auth_provider = getattr(request.app.state, "auth_provider", None)
        if not auth_provider:
            return JSONResponse({"error": "Forbidden"}, status_code=403)

        perms = auth_provider.get_user_permissions(user)

        # Portal entry: user must have ANY admin permission
        if not has_any_admin_permission(perms):
            return JSONResponse(
                {"error": "Forbidden — no admin portal permissions"},
                status_code=403,
            )

        request.state.admin_permissions = perms
        return await call_next(request)
```

### 2. View.is_accessible() — View Visibility

```python
class UserView(BaseAdminView):
    def is_accessible(self, request: Request) -> bool:
        perms = getattr(request.state, "admin_permissions", {})
        return can_view_users(perms)  # checks EDIT_USERS or VIEW_USERS

class RoleView(BaseAdminView):
    def is_accessible(self, request: Request) -> bool:
        perms = getattr(request.state, "admin_permissions", {})
        return can_view_roles(perms)  # checks EDIT_ROLES or VIEW_ROLES
```

### 3. View.can_create/edit/delete() — Operation-Level

```python
class UserView(BaseAdminView):
    def can_create(self, request: Request) -> bool:
        return can_edit_users(getattr(request.state, "admin_permissions", {}))

    def can_edit(self, request: Request) -> bool:
        return can_edit_users(getattr(request.state, "admin_permissions", {}))

    def can_delete(self, request: Request) -> bool:
        return can_edit_users(getattr(request.state, "admin_permissions", {}))
```

All three mutation operations check the same `ADMIN_EDIT_USERS` permission. A user who can edit can also create and delete — the granularity is at the entity level, not the operation level.

### 4. Template-Level Visibility

```html
{# dashboard.html — sidebar nav links #}
<nav class="admin-sidebar">
  {% if user_view.is_accessible(request) %}
  <a href="{{ url_for('admin-list', identity='users') }}">Users</a>
  {% endif %} {% if role_view.is_accessible(request) %}
  <a href="{{ url_for('admin-list', identity='roles') }}">Roles</a>
  {% endif %}
</nav>

{# list.html — "Create" button (only for editors) #} {% if
view.can_create(request) %}
<a
  href="{{ url_for('admin-create', identity=view.identity) }}"
  class="btn btn-primary"
>
  Create {{ view.label }}
</a>
{% endif %} {# list.html — per-row actions #} {% for action in row_actions %} {%
if action.name == 'edit' and not view.can_edit(request) %} {# Skip #} {% elif
action.name == 'delete' and not view.can_delete(request) %} {# Skip #} {% else
%}
<button data-action="{{ action.name }}">{{ action.text }}</button>
{% endif %} {% endfor %}
```

### 5. API-Level Enforcement

```python
async def _render_create(self, request: Request) -> Response:
    view = self._find_view(request.path_params.get("identity"))
    if not view.is_accessible(request) or not view.can_create(request):
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    # ... render or process form
```

## Mapping to Dagster+ Cloud Permissions

| Admin Portal Permission | Dagster+ Cloud Permission | Notes |
| ----------------------- | ------------------------- | -------------------- |
| `ADMIN_VIEW_USERS` | `EDIT_USERS_AND_TEAMS` | Read access to users |
| `ADMIN_EDIT_USERS` | `EDIT_USERS_AND_TEAMS` | Full user management |
| `ADMIN_VIEW_ROLES` | `EDIT_CUSTOM_ROLES` | Read access to roles |
| `ADMIN_EDIT_ROLES` | `EDIT_CUSTOM_ROLES` | Full role management |

Dagster+ uses coarse-grained permissions. Our model splits view vs. edit but collapses create/edit/delete into one — a reasonable balance for OSS.

### Future Mappings

| Future Permission | Dagster+ Cloud Permission |
| -------------------------- | ---------------------------- |
| `ADMIN_VIEW_SERVICE_USERS` | `MANAGE_SERVICE_USERS` |
| `ADMIN_EDIT_SERVICE_USERS` | `MANAGE_SERVICE_USERS` |
| `ADMIN_VIEW_API_TOKENS` | `READ_AND_EDIT_AGENT_TOKENS` |
| `ADMIN_EDIT_API_TOKENS` | `READ_AND_EDIT_AGENT_TOKENS` |
| `ADMIN_VIEW_AUDIT_LOG` | `READ_AUDIT_LOG` |

## Audit Trail

```python
ADMIN_AUDIT_EVENTS = {
    "user_created": "ADMIN_USER_CREATED",
    "user_updated": "ADMIN_USER_UPDATED",
    "user_deleted": "ADMIN_USER_DELETED",
    "role_created": "ADMIN_ROLE_CREATED",
    "role_updated": "ADMIN_ROLE_UPDATED",
    "role_deleted": "ADMIN_ROLE_DELETED",
}
```

## Self-Protection Rules

### Rule 1: Cannot Delete Last Admin

```python
async def _can_delete_user(self, username: str) -> tuple[bool, str | None]:
    current_user = getattr(self._current_request.state, "user", None)
    if current_user and current_user.username == username:
        return False, "Cannot delete your own account"

    all_users = await self._backend.list_users()
    admin_count = sum(
        1 for u in all_users
        if u.username != username and has_any_admin_permission(self._resolve_user_permissions(u))
    )
    if admin_count == 0:
        return False, "Cannot delete the last user with admin permissions"
    return True, None
```

### Rule 2: Cannot Self-Demote

```python
async def _can_change_own_role(self, new_role_name: str) -> tuple[bool, str | None]:
    current_user = getattr(self._current_request.state, "user", None)
    if not current_user:
        return False, "Not authenticated"

    new_role = await self._backend.get_role(new_role_name)
    if not new_role:
        return False, f"Role '{new_role_name}' not found"

    new_perms = self._resolve_role_permissions(new_role)
    if not has_any_admin_permission(new_perms):
        return False, "Cannot change your role to one with no admin permissions"
    return True, None
```

### Rule 3: Built-in Roles Are Immutable

```python
async def edit(self, request, pk, data):
    role = await self._backend.get_role(pk)
    if role and role.is_builtin:
        raise ValueError(f"Built-in role '{pk}' cannot be modified")
    return await self._backend.update_role(pk, permissions=data.get("permissions"))
```
