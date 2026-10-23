"""Role definitions and permission maps for RBAC.

Defines the built-in roles (CATALOG_VIEWER, VIEWER, LAUNCHER, EDITOR, ADMIN)
and maps each role to the Dagster Permissions enum values.
"""

from enum import Enum, unique
from typing import Any

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
            AdminPermission.ADMIN_EDIT_OIDC.value: PermissionResult(
                enabled=True, disabled_reason=None
            ),
            AdminPermission.ADMIN_EDIT_CODE_LOCATIONS.value: PermissionResult(
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
        AdminPermission.ADMIN_VIEW_OIDC.value: PermissionResult(
            enabled=False,
            disabled_reason=f"Role {role.value} missing admin_view_oidc permission",
        ),
        AdminPermission.ADMIN_EDIT_OIDC.value: PermissionResult(
            enabled=False,
            disabled_reason=f"Role {role.value} missing admin_edit_oidc permission",
        ),
        AdminPermission.ADMIN_VIEW_CODE_LOCATIONS.value: PermissionResult(
            enabled=False,
            disabled_reason=f"Role {role.value} missing admin_view_code_locations permission",
        ),
        AdminPermission.ADMIN_EDIT_CODE_LOCATIONS.value: PermissionResult(
            enabled=False,
            disabled_reason=f"Role {role.value} missing admin_edit_code_locations permission",
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


def resolve_permissions_for_location(
    roles: list[Any],
    location_name: str | None = None,
) -> dict[str, PermissionResult]:
    """Merge permissions from multiple role assignments for a given location.

    Args:
        roles: List of Role ORM objects applicable to the user (already filtered
            by location in the query layer — only include roles where
            ``code_location_name IS NULL`` or matches *location_name*).
        location_name: The target code location (for error messages).

    Returns:
        A merged PermissionResult map. When multiple roles grant the same
        permission, OR logic applies (if any role enables it, it's enabled).

    If no roles are applicable, returns an all-disabled map.
    """
    if not roles:
        # No applicable roles — return all-disabled
        all_perms: dict[str, PermissionResult] = {}
        for perm in Permissions:
            all_perms[perm.name] = PermissionResult(
                enabled=False,
                disabled_reason=f"No role assigned for location '{location_name}'",
            )
        return all_perms

    # Start with all permissions disabled
    merged: dict[str, bool] = {perm.name: False for perm in Permissions}

    for role in roles:
        if role.is_builtin:
            # Use the built-in permission map
            try:
                role_enum = Role(role.name)
                perm_map = ROLE_PERMISSIONS[role_enum]
                for perm, enabled in perm_map.items():
                    if enabled:
                        merged[perm.name] = True
            except ValueError:
                continue
        else:
            # Custom role — use its permissions JSON
            raw = role.permissions
            if isinstance(raw, dict):
                perms = raw
            elif isinstance(raw, str):
                import json as _json

                try:
                    perms = _json.loads(raw) if raw else {}
                except _json.JSONDecodeError:
                    continue
            else:
                continue
            for perm_name, enabled in perms.items():
                if enabled and perm_name in merged:
                    merged[perm_name] = True

    # Build PermissionResult map
    result: dict[str, PermissionResult] = {}
    for perm_name, enabled in merged.items():
        result[perm_name] = PermissionResult(
            enabled=enabled,
            disabled_reason=None
            if enabled
            else f"No applicable role grants {perm_name} for location '{location_name}'",
        )

    # Add admin portal permissions (only if any applicable role is ADMIN)
    has_admin = any(
        (r.is_builtin and r.name == "admin")
        or (not r.is_builtin and _role_has_perm(r, "ADMIN_EDIT_USERS"))
        for r in roles
    )
    if has_admin:
        result.update(_admin_permissions_for_role(Role.ADMIN))
    else:
        result.update(
            {
                AdminPermission.ADMIN_VIEW_USERS.value: PermissionResult(
                    enabled=False,
                    disabled_reason="No admin role for this location",
                ),
                AdminPermission.ADMIN_EDIT_USERS.value: PermissionResult(
                    enabled=False,
                    disabled_reason="No admin role for this location",
                ),
            }
        )

    return result


def _role_has_perm(role: Any, perm_name: str) -> bool:
    """Check if a custom role's permissions dict includes a given permission."""
    raw = role.permissions
    if isinstance(raw, dict):
        return bool(raw.get(perm_name, False))
    elif isinstance(raw, str):
        import json as _json

        try:
            perms = _json.loads(raw) if raw else {}
            return bool(perms.get(perm_name, False))
        except _json.JSONDecodeError:
            return False
    return False
