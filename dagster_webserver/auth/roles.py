"""Role definitions and permission maps for RBAC.

Defines the built-in roles (CATALOG_VIEWER, VIEWER, LAUNCHER, EDITOR, ADMIN)
and maps each role to the Dagster Permissions enum values.
"""

from enum import Enum, unique

from dagster._core.workspace.permissions import PermissionResult, Permissions

from dagster_webserver.admin.permissions import (
    AdminPermission,
)


@unique
class Role(str, Enum):
    """Built-in user roles, modeled after Dagster+ cloud PermissionGrant enum."""

    CATALOG_VIEWER = "catalog_viewer"
    VIEWER = "viewer"
    LAUNCHER = "launcher"
    EDITOR = "editor"
    ADMIN = "admin"


def _all_false() -> dict[Permissions, bool]:
    return {p: False for p in Permissions}


def _all_true() -> dict[Permissions, bool]:
    return {p: True for p in Permissions}


# Permission maps per role: {Permissions: enabled}
ROLE_PERMISSIONS: dict[Role, dict[Permissions, bool]] = {
    Role.CATALOG_VIEWER: _all_false(),
    Role.VIEWER: _all_false(),
    Role.LAUNCHER: {
        **_all_false(),
        Permissions.LAUNCH_PIPELINE_EXECUTION: True,
        Permissions.LAUNCH_PIPELINE_REEXECUTION: True,
        Permissions.TERMINATE_PIPELINE_EXECUTION: True,
        Permissions.LAUNCH_PARTITION_BACKFILL: True,
        Permissions.CANCEL_PARTITION_BACKFILL: True,
    },
    Role.EDITOR: _all_true(),
    Role.ADMIN: _all_true(),
}


def _admin_permissions_for_role(role: Role) -> dict[str, PermissionResult]:
    """Return admin portal permissions for a built-in role.

    Only ADMIN gets admin permissions (both edit permissions, which
    implicitly grant view).  All other roles get them disabled.

    Keys use the lowercase enum values (e.g. ``admin_edit_users``) so
    they match what ``has_any_admin_permission`` and the ``can_*`` helpers
    look for.
    """
    if role == Role.ADMIN:
        return {
            AdminPermission.ADMIN_EDIT_USERS.value: PermissionResult(
                enabled=True, disabled_reason=None
            ),
            AdminPermission.ADMIN_EDIT_ROLES.value: PermissionResult(
                enabled=True, disabled_reason=None
            ),
        }
    return {
        AdminPermission.ADMIN_VIEW_USERS.value: PermissionResult(
            enabled=False,
            disabled_reason=f"Role {role.value} missing admin_view_users permission",
        ),
        AdminPermission.ADMIN_EDIT_USERS.value: PermissionResult(
            enabled=False,
            disabled_reason=f"Role {role.value} missing admin_edit_users permission",
        ),
        AdminPermission.ADMIN_VIEW_ROLES.value: PermissionResult(
            enabled=False,
            disabled_reason=f"Role {role.value} missing admin_view_roles permission",
        ),
        AdminPermission.ADMIN_EDIT_ROLES.value: PermissionResult(
            enabled=False,
            disabled_reason=f"Role {role.value} missing admin_edit_roles permission",
        ),
    }


def get_role_permissions(role: Role) -> dict[str, PermissionResult]:
    """Convert a Role enum to a PermissionResult map compatible with
    ``WorkspaceRequestContext.permissions``.

    Includes both the 21 Dagster Permissions and the 4 admin portal
    permissions.
    """
    perm_map = ROLE_PERMISSIONS[role]
    result: dict[str, PermissionResult] = {
        perm: PermissionResult(
            enabled=enabled,
            disabled_reason=None
            if enabled
            else f"Role {role.value} missing {perm} permission",
        )
        for perm, enabled in perm_map.items()
    }
    # Add admin portal permissions
    result.update(_admin_permissions_for_role(role))
    return result


def get_custom_permissions(perm_map: dict[str, bool]) -> dict[str, PermissionResult]:
    """Build a PermissionResult map from an arbitrary permission dict.

    Used for custom roles where the user explicitly specifies which
    permissions are enabled.  Admin portal permission keys are handled
    alongside Dagster permission keys.
    """
    return {
        perm: PermissionResult(
            enabled=enabled,
            disabled_reason=None if enabled else "Disabled by your role configuration",
        )
        for perm, enabled in perm_map.items()
    }
