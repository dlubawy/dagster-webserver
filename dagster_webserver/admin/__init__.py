"""Admin portal for managing users and roles.

Public API
----------
Permissions
~~~~~~~~~~~
- ``AdminPermission`` — enum of admin portal permissions
- ``can_view_users()`` / ``can_edit_users()`` — user permission checks
- ``can_view_roles()`` / ``can_edit_roles()`` — role permission checks
- ``has_any_admin_permission()`` — portal entry check

Portal
~~~~~~
- ``AdminPortal`` — mounts CRUD views at ``/admin``
- ``AdminPortalMiddleware`` — enforces admin permission checks

Views
~~~~~
- ``BaseAdminView`` — abstract base for CRUD views
- ``UserView`` — user management
- ``RoleView`` — role management
"""  # noqa: D205, D400

from dagster_webserver.admin.permissions import (
    AdminPermission,
    can_edit_roles,
    can_edit_users,
    can_view_roles,
    can_view_users,
    has_any_admin_permission,
)
from dagster_webserver.admin.portal import AdminPortal, AdminPortalMiddleware
from dagster_webserver.admin.views import BaseAdminView, RoleView, UserView

__all__ = [
    # Permissions
    "AdminPermission",
    "can_view_users",
    "can_edit_users",
    "can_view_roles",
    "can_edit_roles",
    "has_any_admin_permission",
    # Portal
    "AdminPortal",
    "AdminPortalMiddleware",
    # Views
    "BaseAdminView",
    "UserView",
    "RoleView",
]
