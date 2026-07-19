# Plan: Admin Portal for Dagster Webserver

## Goal

Add a web-based admin portal at `/admin` that provides CRUD operations for **users** and **roles** — the entities that power the auth/RBAC system. The portal is only accessible to users whose role grants at least one admin permission, and every view and operation is gated by its own specific permission.

## Pre-existing Infrastructure

The following already exists in `dagster_webserver/` and is **not** part of this plan:

- **Auth system** (`auth/`): `SessionAuthProvider`, `AuthMiddleware`, `login_endpoint`, `logout_endpoint`, `me_endpoint`, `AuthUser`, `UserBackend`, `InMemoryUserBackend`, `FileUserBackend`, `DatabaseUserBackend`, `Role` enum, `get_role_permissions()`, `get_custom_permissions()`
- **Database layer** (`database/`): SQLAlchemy models (`User`, `Role`), async engine, Alembic migration `001_create_roles_and_users`
- **Permission model** (`auth/roles.py`): 5 built-in roles (CATALOG_VIEWER → ADMIN), 21 Dagster `Permissions` enum values, `PermissionResult` maps
- **Context classes** (`auth/context.py`): `AuthenticatedWorkspaceRequestContext`, `AuthenticatedWorkspaceProcessContext`
- **Webserver** (`webserver.py`): `DagsterWebserver` with `_auth_provider`, auth middleware injection, `_build_auth_routes()`

## Permission Model

4 admin permissions, 2 per entity. **Edit implies view**. Having **any** admin permission grants portal entry.

| Permission | Grants |
| ------------------ | ------------------------------------------------ |
| `ADMIN_VIEW_USERS` | List users, view user details |
| `ADMIN_EDIT_USERS` | Create, edit, delete users **+** view (implicit) |
| `ADMIN_VIEW_ROLES` | List roles, view role details |
| `ADMIN_EDIT_ROLES` | Create, edit, delete roles **+** view (implicit) |

Only the built-in `ADMIN` role gets admin permissions (`ADMIN_EDIT_USERS`, `ADMIN_EDIT_ROLES`). Custom roles can grant any subset. See `/.agent/research/08-admin-portal-permissions.md` for full details.

______________________________________________________________________

## Phase 1: Admin Permission Infrastructure

**Objective**: Define the 4 admin permissions and integrate them into the existing role/permission resolution system.

### Step 1.1: Create `dagster_webserver/admin/permissions.py`

- Define `AdminPermission` enum with 4 values: `ADMIN_VIEW_USERS`, `ADMIN_EDIT_USERS`, `ADMIN_VIEW_ROLES`, `ADMIN_EDIT_ROLES`
- Implement resolution helpers: `can_view_users()`, `can_edit_users()`, `can_view_roles()`, `can_edit_roles()`, `has_any_admin_permission()`
- Implement `_has(perms, key)` helper that checks `PermissionResult.enabled`
- Export all symbols from `dagster_webserver/admin/__init__.py`

**Outcome**: Any code can import `AdminPermission`, `can_view_users()`, etc. and check permissions against a `dict[str, PermissionResult]`.

### Step 1.2: Wire admin permissions into built-in roles

- Modify `dagster_webserver/auth/roles.py` — `get_role_permissions(Role.ADMIN)` must also return the 4 admin permissions as `PermissionResult(enabled=True)`
- All other built-in roles return admin permissions as `PermissionResult(enabled=False)`
- `get_custom_permissions()` already handles arbitrary permission dicts, so custom roles that include admin permission keys will work automatically

**Outcome**: An ADMIN user's `permissions` map contains the 4 admin permissions as enabled. A VIEWER user's map does not.

### Step 1.3: Extend `/api/me` with `hasAnyAdminPermission`

- Modify `dagster_webserver/auth/routes.py` — `me_endpoint()` calls `has_any_admin_permission(perms)` and includes `hasAnyAdminPermission` in the JSON response

**Outcome**: The UI can call `/api/me` and know whether the current user should see the admin portal nav button.

______________________________________________________________________

## Phase 2: Admin Portal Core

**Objective**: Build the portal infrastructure — middleware, route mounting, view base class, and templates.

### Step 2.1: Add `jinja2` as a dependency

- Add `jinja2` to `dagster-webserver/pyproject.toml` dependencies

**Outcome**: Jinja2 is available for template rendering.

### Step 2.2: Create `dagster_webserver/admin/portal.py`

- Define `AdminPortal` class:
  - `__init__(self, backend: DatabaseUserBackend)` — stores backend, instantiates views
  - `_init_views()` — creates `UserView` and `RoleView` instances
  - `_init_routes()` — builds route table:
    - `GET /` → dashboard
    - `GET /{identity}/list` → list view
    - `GET /{identity}/detail/{pk}` → detail view
    - `GET, POST /{identity}/create` → create form
    - `GET, POST /{identity}/edit/{pk}` → edit form
    - `POST /{identity}/action` → batch action handler
    - `POST /{identity}/row-action` → row action handler
  - `mount_to(routes: list[Route])` — inserts `Mount("/admin", admin_app)` at front of routes
  - `_find_view(identity)` — resolves identity to view instance
  - `_render_index()`, `_render_list()`, `_render_detail()`, `_render_create()`, `_render_edit()` — route handlers that delegate to views
  - `handle_action()`, `handle_row_action()` — dispatch to view action handlers
- Define `AdminPortalMiddleware`:
  - Checks `has_any_admin_permission(perms)` → 403 if no access
  - Stores `request.state.admin_permissions = perms` for downstream views

**Middleware ordering**: The top-level `AuthMiddleware` (on `DagsterWebserver`) runs first because it's on the parent app. It resolves the user from session and sets `request.state.user`. Then, when the request hits the `/admin` mount, the `AdminPortalMiddleware` (on the child admin sub-app) runs next. It reads `request.state.user` (already set by the parent) and checks admin permissions. This ordering is confirmed by Starlette's `Mount` behavior: parent middleware wraps child middleware.

**Outcome**: The portal can be instantiated with a `DatabaseUserBackend` and mounted into the webserver's route table.

### Step 2.3: Create `dagster_webserver/admin/views.py`

- Define `BaseAdminView` with:
  - Class attributes: `identity`, `label`, `plural_label`, `icon`, `list_columns`, `detail_fields`, `create_fields`, `edit_fields`
  - Permission hooks: `is_accessible()`, `can_create()`, `can_edit()`, `can_delete()` — all return `True` by default
  - Abstract CRUD methods: `find_all()`, `count()`, `find_by_pk()`, `create()`, `edit()`, `delete()`
  - Serialization: `serialize(obj, action)` — converts ORM objects to display-friendly dicts
  - Row actions: `view_action()`, `edit_action()`, `delete_action()` — decorated with `@row_action`
  - Batch actions: `delete_action()` — decorated with `@action`
- Define `@action` and `@row_action` decorators (mirroring starlette-admin pattern from `/.agent/research/09-starlette-admin-patterns-reference.md`)

**Outcome**: A reusable view base class that concrete views inherit from.

### Step 2.4: Create Jinja2 templates

Create `dagster_webserver/admin/templates/` with:

- `base.html` — Base layout: sidebar with portal nav links (conditional on `is_accessible()`), header with back-to-Dagster link, content area, flash message area
- `dashboard.html` — Summary: user count, role count, links to accessible views
- `list.html` — Table with columns from `list_columns`, client-side pagination, search input, "Create" button (conditional on `can_create()`), per-row action buttons (conditional on `can_edit()`/`can_delete()`)
- `detail.html` — Field-value display from `detail_fields`, "Edit" button (conditional on `can_edit()`)
- `create.html` — Form with fields from `create_fields`, CSRF token, submit button
- `edit.html` — Form with fields from `edit_fields`, CSRF token, submit button, "Delete" button (conditional on `can_delete()`)

**Outcome**: All templates render correctly with any `BaseAdminView` subclass.

### Step 2.5: Wire into `DagsterWebserver`

- Modify `dagster_webserver/webserver.py`:
  - Add `admin_portal: AdminPortal | None = None` parameter to `DagsterWebserver.__init__()`
  - In `build_routes()`, if `self._admin_portal` is set, call `self._admin_portal.mount_to(routes)` to insert the `/admin` mount
- Modify `dagster_webserver/app.py`:
  - Add `admin_portal` parameter to `create_app_from_workspace_process_context()`
  - Pass through to `DagsterWebserver`

**Outcome**: When `admin_portal` is configured, `/admin/*` routes are available.

### Step 2.6: Add CLI flags

- Modify `dagster_webserver/cli.py`:
  - `--enable-admin-portal` flag — when set, creates `AdminPortal(DatabaseUserBackend(database_url))` and passes it to `create_app_from_workspace_process_context()`
  - `--admin-database-url` flag (env: `DAGSTER_ADMIN_DATABASE_URL`) — database URL for the admin portal (default: `sqlite+aiosqlite:///dagster_admin.db`)
  - **Requirement**: The admin portal is only enabled when using `DatabaseUserBackend`. If `--enable-admin-portal` is set without database auth, the CLI should error with a clear message: "Admin portal requires database-backed auth. Use --auth-provider session with --users-database-url."

**Outcome**: `dagster-webserver --enable-admin-portal` enables the portal (only with database auth).

______________________________________________________________________

## Phase 3: User Management

**Objective**: Implement `UserView` with full CRUD wired to `DatabaseUserBackend`.

### Step 3.1: Implement `UserView`

- Inherit from `BaseAdminView`
- Set class attributes: `identity = "users"`, `label = "User"`, `plural_label = "Users"`, `icon = "users"`
- Define `list_columns`, `detail_fields`, `create_fields`, `edit_fields`
- Implement permission hooks:
  - `is_accessible()` → `can_view_users(perms)`
  - `can_create()`, `can_edit()`, `can_delete()` → `can_edit_users(perms)`
- Implement CRUD methods by delegating to `DatabaseUserBackend`:
  - `find_all()` → `backend.list_users()` with filtering/sorting (client-side pagination, load all)
  - `count()` → `len(list_users())`
  - `find_by_pk(pk)` → `backend.get_user(pk)` (pk = username)
  - `create(data)` → `backend.create_user(...)`
  - `edit(pk, data)` → `backend.update_user(...)`
  - `delete(pks)` → `backend.delete_user(username)` for each
- Implement row actions: `view_action()`, `edit_action()`, `delete_action()`
- Implement batch action: `delete_action(pks)`

**Outcome**: Users can be listed, viewed, created, edited, and deleted through the portal.

### Step 3.2: Add self-protection rules

- In `UserView.delete()`:
  - Block self-deletion: `if username == current_user.username → raise ValueError`
  - Block last-admin deletion: count remaining users with `has_any_admin_permission(perms)`; if 0 → raise ValueError
- In `UserView.edit()`:
  - Block self-demotion: if changing own role and new role has no admin permissions → raise ValueError

**Outcome**: Admin lockout is prevented.

### Step 3.3: Add password reset flow

- Add a "Reset Password" row action on the user list and detail page
- When triggered, generate a random password (e.g., 16 chars alphanumeric), set it on the user, and display it in a one-time flash message
- The password is shown only once — after navigating away, the user must be edited again to reset it
- Only available to users with `ADMIN_EDIT_USERS` permission

**Outcome**: Admins can reset user passwords without knowing the current password.

______________________________________________________________________

## Phase 4: Role Management

**Objective**: Implement `RoleView` with CRUD for roles, protecting built-in roles.

### Step 4.1: Implement `RoleView`

- Inherit from `BaseAdminView`
- Set class attributes: `identity = "roles"`, `label = "Role"`, `plural_label = "Roles"`, `icon = "shield"`
- Define `list_columns`, `detail_fields`, `create_fields`, `edit_fields`
- Implement permission hooks:
  - `is_accessible()` → `can_view_roles(perms)`
  - `can_create()`, `can_edit()`, `can_delete()` → `can_edit_roles(perms)`
- Implement CRUD methods by delegating to `DatabaseUserBackend`:
  - `find_all()` → `backend.list_roles()` with filtering/sorting (client-side pagination, load all)
  - `count()` → `len(list_roles())`
  - `find_by_pk(pk)` → `backend.get_role(pk)` (pk = role name)
  - `create(data)` → `backend.create_role(...)`
  - `edit(pk, data)` → `backend.update_role(...)` with built-in role check
  - `delete(pks)` → `backend.delete_role(name)` for each with built-in role check
- Implement row actions: `view_action()`, `edit_action()`, `delete_action()`
- Implement batch action: `delete_action(pks)`

**Outcome**: Roles can be listed, viewed, created, edited, and deleted through the portal.

### Step 4.2: Build permission checkbox form for role editing

- In `edit.html` (or a role-specific template), render a checklist of all available permissions:
  - 21 Dagster `Permissions` enum values
  - 4 `AdminPermission` enum values
  - Each checkbox labeled with the permission name and description
  - Pre-checked based on current role's `permissions` dict
  - On submit, serialize checked permissions to `dict[str, bool]` and pass to `backend.update_role()`
- In `create.html`, same checklist for new roles

**Outcome**: Admins can create and edit custom roles with fine-grained permission control.

### Step 4.3: Protect built-in roles

- `RoleView.edit()`: if `role.is_builtin` → raise `ValueError("Built-in roles cannot be modified")`
- `RoleView.delete()`: if `role.is_builtin` → raise `ValueError("Built-in roles cannot be deleted")`
- In templates: show "Built-in" badge on built-in roles, hide edit/delete buttons for built-in roles

**Outcome**: Built-in roles are read-only in the portal.

______________________________________________________________________

## Phase 5: UI Integration

**Objective**: Add the admin portal button to the Dagster UI navigation, visible only to users with admin permissions.

### Step 5.1: Add `AdminPortalItem` component

- Create `js_modules/ui-core/src/app/navigation/AdminPortalItem.tsx`:
  - Uses `useCurrentUser()` hook to get `hasAnyAdminPermission`
  - Returns `null` if `!user?.hasAnyAdminPermission`
  - Renders `NavItemWithLink` with icon `admin` (confirmed available in `ui-components/src/icon-svgs/admin.svg`), label "Admin", href `/admin`
  - `isActive` checks if pathname starts with `/admin`

**Outcome**: Admin portal button appears in the nav for users with admin permissions.

### Step 5.2: Wire into navigation

- Modify `js_modules/ui-core/src/app/navigation/mainNavigationItems.tsx`:
  - Import `AdminPortalItem`
  - Add `{ key: 'admin', label: 'Admin', element: <AdminPortalItem /> }` to `getBottomGroups()` between collapse and settings

**Outcome**: The admin portal button is in the bottom nav group.

### Step 5.3: Handle `/admin` as external URL

- The admin portal is a separate Jinja2-rendered app, not part of the React SPA
- Navigation to `/admin` is a full page load (not a SPA route)
- The admin portal's `base.html` includes a "Back to Dagster" link that navigates back to the SPA

**Outcome**: Users can navigate between the SPA and the admin portal seamlessly.

______________________________________________________________________

## Phase 6: Testing

**Objective**: Verify all functionality works correctly.

### Step 6.1: Unit tests for permissions

- Test `can_view_users()`, `can_edit_users()`, `can_view_roles()`, `can_edit_roles()`, `has_any_admin_permission()`
- Test edit-implies-view implication
- Test `get_role_permissions(Role.ADMIN)` includes admin permissions

### Step 6.2: Integration tests for portal

- Test `/admin` returns 403 for non-admin users
- Test `/admin` returns 200 for admin users
- Test `/admin/users` returns 403 for users without `ADMIN_VIEW_USERS` or `ADMIN_EDIT_USERS`
- Test user CRUD operations (create, read, update, delete)
- Test role CRUD operations
- Test self-protection rules (self-delete, last-admin-delete, self-demotion)
- Test built-in role protection
- Test password reset flow

### Step 6.3: Integration tests for `/api/me`

- Test `hasAnyAdminPermission` is `true` for ADMIN role
- Test `hasAnyAdminPermission` is `false` for VIEWER role

### Step 6.4: UI tests

- Test `AdminPortalItem` renders for users with `hasAnyAdminPermission: true`
- Test `AdminPortalItem` returns `null` for users with `hasAnyAdminPermission: false`

______________________________________________________________________

## File Inventory

### New Files

```
dagster_webserver/
├── admin/
│   ├── __init__.py
│   ├── permissions.py          # AdminPermission enum + resolution helpers
│   ├── portal.py               # AdminPortal class + AdminPortalMiddleware
│   ├── views.py                # BaseAdminView, UserView, RoleView + decorators
│   └── templates/
│       ├── base.html
│       ├── dashboard.html
│       ├── list.html
│       ├── detail.html
│       ├── create.html
│       └── edit.html
```

### Modified Files

```
dagster_webserver/
├── pyproject.toml              # Add jinja2 dependency
├── auth/
│   ├── roles.py                # ADMIN role gets admin permissions
│   └── routes.py               # me_endpoint includes hasAnyAdminPermission
├── webserver.py                # Accept admin_portal, mount in build_routes()
├── app.py                      # Accept admin_portal parameter
└── cli.py                      # --enable-admin-portal, --admin-database-url flags
```

### UI Files (in full Dagster repo)

```
js_modules/ui-core/src/app/navigation/
├── AdminPortalItem.tsx          # NEW: Admin portal nav button
└── mainNavigationItems.tsx      # MODIFIED: Add AdminPortalItem to bottom group
```

______________________________________________________________________

## Design Decisions (Resolved)

| Question | Decision |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Jinja2 dependency** | Add `jinja2` to `pyproject.toml` dependencies |
| **CSRF protection** | Out of scope for now |
| **Middleware ordering** | Confirmed: parent `AuthMiddleware` runs first (sets `request.state.user`), then child `AdminPortalMiddleware` runs inside the `/admin` mount (checks admin perms). Starlette's `Mount` wraps child middleware inside parent middleware. |
| **Backend requirement** | Admin portal requires `DatabaseUserBackend`. CLI errors if `--enable-admin-portal` is used without database auth. |
| **Nav icon** | Use `admin` — confirmed available at `ui-components/src/icon-svgs/admin.svg` |
| **Password reset** | Separate "Reset Password" row action that generates a random password and displays it once |
| **Pagination** | Client-side (load all, paginate in JS). Auth database is small. Server-side pagination reserved for future audit log. |
