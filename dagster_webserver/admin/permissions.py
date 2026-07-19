"""Admin portal permissions.

Defines four admin-specific permissions — two per managed entity (users,
roles).  Each entity has a **view** permission and an **edit** permission.
**Edit implies view**: if a user can edit users they can also list and view
them.

Having **any** of these permissions grants entry to the admin portal at
``/admin``.  There is no separate gateway permission.
"""

from __future__ import annotations

from enum import Enum, unique

from dagster._core.workspace.permissions import PermissionResult


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


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


def _has(perms: dict[str, PermissionResult], key: str) -> bool:
    """Return True if *key* is present and enabled in *perms*."""
    result = perms.get(key)
    if result is None:
        return False
    return result.enabled


# ---------------------------------------------------------------------------
# User permissions
# ---------------------------------------------------------------------------


def can_view_users(perms: dict[str, PermissionResult]) -> bool:
    """Can the user view users?  EDIT implies VIEW."""
    return _has(perms, AdminPermission.ADMIN_EDIT_USERS.value) or _has(
        perms, AdminPermission.ADMIN_VIEW_USERS.value
    )


def can_edit_users(perms: dict[str, PermissionResult]) -> bool:
    """Can the user create / edit / delete users?"""
    return _has(perms, AdminPermission.ADMIN_EDIT_USERS.value)


# ---------------------------------------------------------------------------
# Role permissions
# ---------------------------------------------------------------------------


def can_view_roles(perms: dict[str, PermissionResult]) -> bool:
    """Can the user view roles?  EDIT implies VIEW."""
    return _has(perms, AdminPermission.ADMIN_EDIT_ROLES.value) or _has(
        perms, AdminPermission.ADMIN_VIEW_ROLES.value
    )


def can_edit_roles(perms: dict[str, PermissionResult]) -> bool:
    """Can the user create / edit / delete roles?"""
    return _has(perms, AdminPermission.ADMIN_EDIT_ROLES.value)


# ---------------------------------------------------------------------------
# Portal entry
# ---------------------------------------------------------------------------


def has_any_admin_permission(perms: dict[str, PermissionResult]) -> bool:
    """Portal entry check — user must have ANY admin permission."""
    return any(_has(perms, p.value) for p in AdminPermission)
