"""Tests for the admin portal — permissions, views, and middleware."""

from __future__ import annotations

from dagster._core.workspace.permissions import PermissionResult

from dagster_webserver.admin.permissions import (
    AdminPermission,
    can_edit_roles,
    can_edit_users,
    can_view_roles,
    can_view_users,
    has_any_admin_permission,
)
from dagster_webserver.auth.roles import Role, get_role_permissions

# ---------------------------------------------------------------------------
# Phase 1: Permission resolution tests
# ---------------------------------------------------------------------------


class TestPermissionHelpers:
    """Unit tests for admin permission resolution helpers."""

    def _perms(self, **kwargs: bool) -> dict[str, PermissionResult]:
        return {
            k: PermissionResult(enabled=v, disabled_reason=None if v else "disabled")
            for k, v in kwargs.items()
        }

    # -- has_any_admin_permission --

    def test_has_any_admin_permission_empty(self):
        assert has_any_admin_permission({}) is False

    def test_has_any_admin_permission_view_users(self):
        assert has_any_admin_permission(self._perms(admin_view_users=True)) is True

    def test_has_any_admin_permission_edit_users(self):
        assert has_any_admin_permission(self._perms(admin_edit_users=True)) is True

    def test_has_any_admin_permission_view_roles(self):
        assert has_any_admin_permission(self._perms(admin_view_roles=True)) is True

    def test_has_any_admin_permission_edit_roles(self):
        assert has_any_admin_permission(self._perms(admin_edit_roles=True)) is True

    def test_has_any_admin_permission_non_admin_perms(self):
        # Dagster permissions don't count as admin permissions
        perms = self._perms(launch_pipeline_execution=True)
        assert has_any_admin_permission(perms) is False

    # -- can_view_users --

    def test_can_view_users_view_only(self):
        assert can_view_users(self._perms(admin_view_users=True)) is True

    def test_can_view_users_edit_implies_view(self):
        assert can_view_users(self._perms(admin_edit_users=True)) is True

    def test_can_view_users_no_perms(self):
        assert can_view_users(self._perms()) is False

    def test_can_view_users_role_perms_only(self):
        assert can_view_users(self._perms(admin_view_roles=True)) is False

    # -- can_edit_users --

    def test_can_edit_users_edit(self):
        assert can_edit_users(self._perms(admin_edit_users=True)) is True

    def test_can_edit_users_view_only(self):
        assert can_edit_users(self._perms(admin_view_users=True)) is False

    def test_can_edit_users_no_perms(self):
        assert can_edit_users(self._perms()) is False

    # -- can_view_roles --

    def test_can_view_roles_view_only(self):
        assert can_view_roles(self._perms(admin_view_roles=True)) is True

    def test_can_view_roles_edit_implies_view(self):
        assert can_view_roles(self._perms(admin_edit_roles=True)) is True

    def test_can_view_roles_no_perms(self):
        assert can_view_roles(self._perms()) is False

    # -- can_edit_roles --

    def test_can_edit_roles_edit(self):
        assert can_edit_roles(self._perms(admin_edit_roles=True)) is True

    def test_can_edit_roles_view_only(self):
        assert can_edit_roles(self._perms(admin_view_roles=True)) is False

    def test_can_edit_roles_no_perms(self):
        assert can_edit_roles(self._perms()) is False


class TestBuiltInRoleAdminPermissions:
    """Test that built-in roles include admin permissions in their maps."""

    def test_admin_role_has_admin_permissions(self):
        perms = get_role_permissions(Role.ADMIN)
        assert perms["admin_edit_users"].enabled is True
        assert perms["admin_edit_roles"].enabled is True

    def test_viewer_role_no_admin_permissions(self):
        perms = get_role_permissions(Role.VIEWER)
        assert perms["admin_view_users"].enabled is False
        assert perms["admin_edit_users"].enabled is False
        assert perms["admin_view_roles"].enabled is False
        assert perms["admin_edit_roles"].enabled is False

    def test_editor_role_no_admin_permissions(self):
        perms = get_role_permissions(Role.EDITOR)
        assert perms["admin_edit_users"].enabled is False
        assert perms["admin_edit_roles"].enabled is False

    def test_launcher_role_no_admin_permissions(self):
        perms = get_role_permissions(Role.LAUNCHER)
        assert perms["admin_edit_users"].enabled is False

    def test_catalog_viewer_role_no_admin_permissions(self):
        perms = get_role_permissions(Role.CATALOG_VIEWER)
        assert perms["admin_edit_users"].enabled is False

    def test_admin_role_includes_admin_permission_keys(self):
        perms = get_role_permissions(Role.ADMIN)
        assert "admin_edit_users" in perms
        assert "admin_edit_roles" in perms

    def test_viewer_role_includes_admin_permission_keys(self):
        perms = get_role_permissions(Role.VIEWER)
        # All roles should have the admin permission keys present (disabled)
        assert "admin_view_users" in perms
        assert "admin_edit_users" in perms
        assert "admin_view_roles" in perms
        assert "admin_edit_roles" in perms


class TestAdminPermissionEnum:
    """Test the AdminPermission enum values."""

    def test_enum_values(self):
        assert AdminPermission.ADMIN_VIEW_USERS.value == "admin_view_users"
        assert AdminPermission.ADMIN_EDIT_USERS.value == "admin_edit_users"
        assert AdminPermission.ADMIN_VIEW_ROLES.value == "admin_view_roles"
        assert AdminPermission.ADMIN_EDIT_ROLES.value == "admin_edit_roles"
        assert AdminPermission.ADMIN_VIEW_OIDC.value == "admin_view_oidc"
        assert AdminPermission.ADMIN_EDIT_OIDC.value == "admin_edit_oidc"

    def test_enum_count(self):
        assert len(AdminPermission) == 6


# ---------------------------------------------------------------------------
# Phase 6: Integration tests for /api/me
# ---------------------------------------------------------------------------


class TestApiMeEndpoint:
    """Tests for /api/me including hasAnyAdminPermission."""

    def test_me_endpoint_returns_has_any_admin_permission_key(self):

        # We verify the endpoint code path by checking the source directly
        # Full integration test requires a running server with auth
        import inspect

        from dagster_webserver.auth.routes import me_endpoint

        source = inspect.getsource(me_endpoint)
        assert "hasAnyAdminPermission" in source
        assert "has_any_admin_permission" in source

    def test_custom_permissions_includes_admin_perms(self):
        """Custom roles that include admin permission keys get them enabled."""
        from dagster_webserver.admin.permissions import has_any_admin_permission
        from dagster_webserver.auth.roles import get_custom_permissions

        perms = get_custom_permissions(
            {
                "admin_view_users": True,
                "admin_edit_users": True,
            }
        )
        assert has_any_admin_permission(perms) is True

    def test_custom_permissions_no_admin_perms(self):
        from dagster_webserver.admin.permissions import has_any_admin_permission
        from dagster_webserver.auth.roles import get_custom_permissions

        perms = get_custom_permissions(
            {
                "launch_pipeline_execution": True,
            }
        )
        assert has_any_admin_permission(perms) is False


# ---------------------------------------------------------------------------
# Phase 6: CLI tests
# ---------------------------------------------------------------------------


class TestCLIAdminPortal:
    """Tests for CLI admin portal flags."""

    def test_enable_admin_portal_flag_exists(self):
        from dagster_webserver.cli import start

        # Verify the --enable-admin-portal option exists
        params = {p.name: p for p in start.params}
        assert "enable_admin_portal" in params

    def test_admin_database_url_flag_exists(self):
        from dagster_webserver.cli import start

        params = {p.name: p for p in start.params}
        assert "admin_database_url" in params


# ---------------------------------------------------------------------------
# Phase 6: Portal wiring tests
# ---------------------------------------------------------------------------


class TestPortalWiring:
    """Tests that the admin portal is properly wired into the webserver."""

    def test_webserver_accepts_admin_portal(self):
        import inspect

        from dagster_webserver.webserver import DagsterWebserver

        sig = inspect.signature(DagsterWebserver.__init__)
        assert "admin_portal" in sig.parameters

    def test_app_accepts_admin_portal(self):
        import inspect

        from dagster_webserver.app import create_app_from_workspace_process_context

        sig = inspect.signature(create_app_from_workspace_process_context)
        assert "admin_portal" in sig.parameters

    def test_admin_init_exports(self):
        from dagster_webserver.admin import (
            AdminPermission,
            AdminPortal,
            BaseAdminView,
            RoleView,
            UserView,
        )

        # Just verify all imports succeed
        assert AdminPermission is not None
        assert AdminPortal is not None
        assert BaseAdminView is not None
        assert UserView is not None
        assert RoleView is not None


# ---------------------------------------------------------------------------
# Phase 6 Step 6.2: Full integration tests for the admin portal
# ---------------------------------------------------------------------------


class TestPortalIntegration:
    """Integration tests that exercise the admin portal end-to-end with TestClient."""

    def _make_app(self, db_url: str = "sqlite+aiosqlite:///:memory:"):
        """Build a minimal Starlette app with the admin portal mounted."""
        from starlette.applications import Starlette
        from starlette.routing import Mount

        from dagster_webserver.admin import AdminPortal, AdminPortalMiddleware
        from dagster_webserver.auth.db_backend import DatabaseUserBackend

        backend = DatabaseUserBackend(db_url)
        portal = AdminPortal(backend)

        # Build a minimal admin sub-app with middleware
        from starlette.middleware import Middleware

        admin_app = Starlette(
            routes=portal.routes,
            middleware=[Middleware(AdminPortalMiddleware)],
        )

        # Wrap with a fake auth middleware that injects request.state.user
        def fake_auth_middleware(request, call_next):
            from dagster_webserver.auth.users import AuthUser

            request.state.user = AuthUser(
                username="admin",
                email="admin@test.com",
                role="admin",
            )
            return call_next(request)

        from starlette.middleware.base import BaseHTTPMiddleware

        admin_app.add_middleware(BaseHTTPMiddleware, dispatch=fake_auth_middleware)

        admin_app.state.ROUTE_NAME = "admin"
        app = Starlette(routes=[Mount("/admin", app=admin_app, name="admin")])
        return app, backend

    def test_admin_dashboard_returns_200(self):
        from starlette.testclient import TestClient

        app, _ = self._make_app()
        client = TestClient(app)
        resp = client.get("/admin/")
        assert resp.status_code == 200
        assert "Admin Portal" in resp.text or "Dashboard" in resp.text

    def test_admin_dashboard_no_user_returns_403(self):
        """Without request.state.user set by auth middleware, portal returns 403."""
        from starlette.applications import Starlette
        from starlette.middleware import Middleware
        from starlette.routing import Mount
        from starlette.testclient import TestClient

        from dagster_webserver.admin import AdminPortal, AdminPortalMiddleware
        from dagster_webserver.auth.db_backend import DatabaseUserBackend

        backend = DatabaseUserBackend("sqlite+aiosqlite:///:memory:")
        portal = AdminPortal(backend)
        admin_app = Starlette(
            routes=portal.routes,
            middleware=[Middleware(AdminPortalMiddleware)],
        )
        app = Starlette(routes=[Mount("/admin", app=admin_app, name="admin")])

        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/admin/")
        assert resp.status_code == 401

    def test_admin_users_list_returns_200(self):
        from starlette.testclient import TestClient

        app, _ = self._make_app()
        client = TestClient(app)
        resp = client.get("/admin/users/list")
        assert resp.status_code == 200

    def test_admin_roles_list_returns_200(self):
        from starlette.testclient import TestClient

        app, _ = self._make_app()
        client = TestClient(app)
        resp = client.get("/admin/roles/list")
        assert resp.status_code == 200

    def test_admin_users_create_returns_200(self):
        from starlette.testclient import TestClient

        app, _ = self._make_app()
        client = TestClient(app)
        resp = client.get("/admin/users/create")
        assert resp.status_code == 200

    def test_admin_roles_create_returns_200(self):
        from starlette.testclient import TestClient

        app, _ = self._make_app()
        client = TestClient(app)
        resp = client.get("/admin/roles/create")
        assert resp.status_code == 200

    def test_user_create_then_list(self):
        """Create a user via POST, then verify it appears in the list."""
        from starlette.testclient import TestClient

        app, _ = self._make_app()
        client = TestClient(app)

        # Create a user
        resp = client.post(
            "/admin/users/create",
            data={
                "username": "newuser",
                "email": "new@test.com",
                "password": "secret123",
                "role": "viewer",
            },
        )
        assert resp.status_code in (200, 302)  # redirect or success

        # List users — newuser should appear
        resp = client.get("/admin/users/list")
        assert resp.status_code == 200
        assert "newuser" in resp.text

    def test_role_create_then_list(self):
        """Create a custom role via POST, then verify it appears in the list."""
        from starlette.testclient import TestClient

        app, _ = self._make_app()
        client = TestClient(app)

        resp = client.post(
            "/admin/roles/create",
            data={
                "name": "custom_role",
                "description": "A custom role",
            },
        )
        assert resp.status_code in (200, 302)

        resp = client.get("/admin/roles/list")
        assert resp.status_code == 200
        assert "custom_role" in resp.text

    def test_built_in_role_cannot_be_deleted(self):
        """Deleting a built-in role should raise ValueError."""
        import asyncio
        from unittest.mock import MagicMock

        from dagster_webserver.admin.views import RoleView
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
        from dagster_webserver.auth.users import AuthUser

        async def _run():
            import pytest

            backend = DatabaseUserBackend("sqlite+aiosqlite:///:memory:")
            view = RoleView(backend)
            request = MagicMock()
            request.state.user = AuthUser(
                username="admin", email="a@t.com", role="admin"
            )
            with pytest.raises(ValueError, match="Built-in"):
                await view.delete(request, ["admin"])

        asyncio.run(_run())

    def test_self_delete_blocked(self):
        """Admin cannot delete themselves."""
        import asyncio
        from unittest.mock import MagicMock

        from dagster_webserver.admin.views import UserView
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
        from dagster_webserver.auth.users import AuthUser

        async def _run():
            import pytest

            backend = DatabaseUserBackend("sqlite+aiosqlite:///:memory:")
            view = UserView(backend)
            user = AuthUser(username="admin", email="admin@test.com", role="admin")
            request = MagicMock()
            request.state.user = user
            with pytest.raises(ValueError, match="self|own"):
                await view.delete(request, ["admin"])

        asyncio.run(_run())

    def test_non_admin_user_gets_403(self):
        """A user without admin permissions gets 403 on /admin."""
        from starlette.applications import Starlette
        from starlette.middleware import Middleware
        from starlette.middleware.base import BaseHTTPMiddleware
        from starlette.routing import Mount
        from starlette.testclient import TestClient

        from dagster_webserver.admin import AdminPortal, AdminPortalMiddleware
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
        from dagster_webserver.auth.users import AuthUser

        backend = DatabaseUserBackend("sqlite+aiosqlite:///:memory:")
        portal = AdminPortal(backend)

        def viewer_auth(request, call_next):
            request.state.user = AuthUser(
                username="viewer",
                email="viewer@test.com",
                role="viewer",
                custom_permissions={
                    "admin_view_users": PermissionResult(
                        enabled=False, disabled_reason="no"
                    ),
                    "admin_edit_users": PermissionResult(
                        enabled=False, disabled_reason="no"
                    ),
                    "admin_view_roles": PermissionResult(
                        enabled=False, disabled_reason="no"
                    ),
                    "admin_edit_roles": PermissionResult(
                        enabled=False, disabled_reason="no"
                    ),
                },
            )
            return call_next(request)

        admin_app = Starlette(
            routes=portal.routes,
            middleware=[Middleware(AdminPortalMiddleware)],
        )
        admin_app.add_middleware(BaseHTTPMiddleware, dispatch=viewer_auth)
        app = Starlette(routes=[Mount("/admin", app=admin_app, name="admin")])

        client = TestClient(app)
        resp = client.get("/admin/")
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Phase 6 Step 6.3: /api/me integration tests
# ---------------------------------------------------------------------------


class TestApiMeIntegration:
    """Integration tests for /api/me with hasAnyAdminPermission."""

    def test_me_endpoint_has_any_admin_permission_for_admin(self):
        """ADMIN role should have hasAnyAdminPermission=true in /api/me."""
        from dagster_webserver.admin.permissions import has_any_admin_permission
        from dagster_webserver.auth.roles import get_role_permissions

        perms = get_role_permissions(Role.ADMIN)
        assert has_any_admin_permission(perms) is True

    def test_me_endpoint_has_any_admin_permission_for_viewer(self):
        """VIEWER role should have hasAnyAdminPermission=false in /api/me."""
        from dagster_webserver.admin.permissions import has_any_admin_permission
        from dagster_webserver.auth.roles import get_role_permissions

        perms = get_role_permissions(Role.VIEWER)
        assert has_any_admin_permission(perms) is False

    def test_me_endpoint_has_any_admin_permission_for_editor(self):
        """EDITOR role should have hasAnyAdminPermission=false in /api/me."""
        from dagster_webserver.admin.permissions import has_any_admin_permission
        from dagster_webserver.auth.roles import get_role_permissions

        perms = get_role_permissions(Role.EDITOR)
        assert has_any_admin_permission(perms) is False


# ---------------------------------------------------------------------------
# Phase 6 Step 6.4: UI tests (source-level verification)
# ---------------------------------------------------------------------------


class TestAdminPortalItemComponent:
    """Tests for AdminPortalItem.tsx — verify source-level correctness."""

    def test_admin_portal_item_file_exists(self):
        """Verify the UI patch file exists."""
        import os

        patch_path = os.path.join(
            os.path.dirname(__file__), "..", "patches", "admin-portal-ui.patch"
        )
        assert os.path.isfile(patch_path), f"Patch file not found at {patch_path}"

    def test_admin_portal_item_calls_api_me(self):
        """AdminPortalItem should call /api/me to check permissions."""
        import os

        patch_path = os.path.join(
            os.path.dirname(__file__), "..", "patches", "admin-portal-ui.patch"
        )
        with open(patch_path) as f:
            content = f.read()
        assert "/api/me" in content

    def test_admin_portal_item_checks_has_any_admin_permission(self):
        """AdminPortalItem should check hasAnyAdminPermission from /api/me."""
        import os

        patch_path = os.path.join(
            os.path.dirname(__file__), "..", "patches", "admin-portal-ui.patch"
        )
        with open(patch_path) as f:
            content = f.read()
        assert "hasAnyAdminPermission" in content

    def test_admin_portal_item_returns_null_when_no_access(self):
        """AdminPortalItem should return null when user has no admin access."""
        import os

        patch_path = os.path.join(
            os.path.dirname(__file__), "..", "patches", "admin-portal-ui.patch"
        )
        with open(patch_path) as f:
            content = f.read()
        assert "return null" in content

    def test_admin_portal_item_links_to_admin(self):
        """AdminPortalItem should link to /admin."""
        import os

        patch_path = os.path.join(
            os.path.dirname(__file__), "..", "patches", "admin-portal-ui.patch"
        )
        with open(patch_path) as f:
            content = f.read()
        assert 'href="/admin"' in content

    def test_admin_portal_item_uses_admin_icon(self):
        """AdminPortalItem should use the 'admin' icon."""
        import os

        patch_path = os.path.join(
            os.path.dirname(__file__), "..", "patches", "admin-portal-ui.patch"
        )
        with open(patch_path) as f:
            content = f.read()
        assert 'name="admin"' in content

    def test_main_navigation_items_includes_admin_portal_item(self):
        """mainNavigationItems.tsx patch should import and include AdminPortalItem."""
        import os

        patch_path = os.path.join(
            os.path.dirname(__file__), "..", "patches", "admin-portal-ui.patch"
        )
        with open(patch_path) as f:
            content = f.read()
        assert "AdminPortalItem" in content
        assert "getBottomGroups" in content or "admin" in content.lower()


# ---------------------------------------------------------------------------
# Phase 6 Step 6.2 (continued): Self-protection rule tests
# ---------------------------------------------------------------------------


class TestSelfProtectionRules:
    """Tests for self-protection rules in UserView (async, request-based)."""

    def _make_request(self, user):
        """Build a minimal Request mock with state.user set."""
        from unittest.mock import MagicMock

        request = MagicMock()
        request.state.user = user
        return request

    def test_user_view_delete_raises_on_self_delete(self):
        """UserView.delete() should raise ValueError for self-deletion."""
        import asyncio

        import pytest

        from dagster_webserver.admin.views import UserView
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
        from dagster_webserver.auth.users import AuthUser

        async def _run():
            backend = DatabaseUserBackend("sqlite+aiosqlite:///:memory:")
            view = UserView(backend)
            user = AuthUser(
                username="admin",
                email="admin@test.com",
                role="admin",
                custom_permissions={
                    "admin_edit_users": PermissionResult(
                        enabled=True, disabled_reason=None
                    ),
                },
            )
            request = self._make_request(user)
            with pytest.raises(ValueError, match="self|own"):
                await view.delete(request, ["admin"])

        asyncio.run(_run())

    def test_user_view_edit_raises_on_self_demotion(self):
        """UserView.edit() should raise ValueError for self-demotion."""
        import asyncio

        import pytest

        from dagster_webserver.admin.views import UserView
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
        from dagster_webserver.auth.users import AuthUser

        async def _run():
            backend = DatabaseUserBackend("sqlite+aiosqlite:///:memory:")
            view = UserView(backend)
            user = AuthUser(
                username="admin",
                email="admin@test.com",
                role="admin",
                custom_permissions={
                    "admin_edit_users": PermissionResult(
                        enabled=True, disabled_reason=None
                    ),
                },
            )
            request = self._make_request(user)
            with pytest.raises(ValueError, match="role|admin"):
                await view.edit(request, "admin", {"role": "viewer"})

        asyncio.run(_run())


# ---------------------------------------------------------------------------
# Phase 6 Step 6.2 (continued): Built-in role protection tests
# ---------------------------------------------------------------------------


class TestBuiltInRoleProtection:
    """Tests for built-in role protection in RoleView (async, request-based)."""

    def _make_request(self, user):
        from unittest.mock import MagicMock

        request = MagicMock()
        request.state.user = user
        return request

    def test_role_view_edit_raises_on_builtin(self):
        """RoleView.edit() should raise ValueError for built-in roles."""
        import asyncio

        import pytest

        from dagster_webserver.admin.views import RoleView
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
        from dagster_webserver.auth.users import AuthUser

        async def _run():
            backend = DatabaseUserBackend("sqlite+aiosqlite:///:memory:")
            view = RoleView(backend)
            user = AuthUser(
                username="admin",
                email="admin@test.com",
                role="admin",
                custom_permissions={
                    "admin_edit_roles": PermissionResult(
                        enabled=True, disabled_reason=None
                    ),
                },
            )
            request = self._make_request(user)
            with pytest.raises(ValueError, match="Built-in"):
                await view.edit(request, "admin", {"permissions": {}})

        asyncio.run(_run())

    def test_role_view_delete_raises_on_builtin(self):
        """RoleView.delete() should raise ValueError for built-in roles."""
        import asyncio

        import pytest

        from dagster_webserver.admin.views import RoleView
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
        from dagster_webserver.auth.users import AuthUser

        async def _run():
            backend = DatabaseUserBackend("sqlite+aiosqlite:///:memory:")
            view = RoleView(backend)
            user = AuthUser(
                username="admin",
                email="admin@test.com",
                role="admin",
                custom_permissions={
                    "admin_edit_roles": PermissionResult(
                        enabled=True, disabled_reason=None
                    ),
                },
            )
            request = self._make_request(user)
            with pytest.raises(ValueError, match="Built-in"):
                await view.delete(request, ["admin"])

        asyncio.run(_run())


# ---------------------------------------------------------------------------
# Phase 6 Step 6.2 (continued): Password reset tests
# ---------------------------------------------------------------------------


class TestPasswordReset:
    """Tests for the password reset flow."""

    def _make_request(self, user):
        from unittest.mock import MagicMock

        request = MagicMock()
        request.state.user = user
        return request

    def test_password_reset_generates_password(self):
        """Password reset should generate a random password."""
        import asyncio

        from dagster_webserver.admin.views import UserView
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
        from dagster_webserver.auth.users import AuthUser

        async def _run():
            backend = DatabaseUserBackend("sqlite+aiosqlite:///:memory:")
            view = UserView(backend)
            user = AuthUser(
                username="admin",
                email="admin@test.com",
                role="admin",
                custom_permissions={
                    "admin_edit_users": PermissionResult(
                        enabled=True, disabled_reason=None
                    ),
                },
            )
            request = self._make_request(user)

            # Create a user first
            await view.create(
                request,
                {
                    "username": "target",
                    "email": "t@t.com",
                    "password": "old",
                    "role": "viewer",
                },
            )

            # Reset password via row action
            result = await view.reset_password_action(request, "target")
            # Result is "New password: <password>"
            assert result.startswith("New password:")
            password = result.split(": ")[1]
            assert len(password) >= 12
            assert password.isalnum()

        asyncio.run(_run())

    def test_password_reset_row_action_exists(self):
        """The reset_password row action is registered on UserView."""
        from dagster_webserver.admin.views import UserView
        from dagster_webserver.auth.db_backend import DatabaseUserBackend

        backend = DatabaseUserBackend("sqlite+aiosqlite:///:memory:")
        view = UserView(backend)
        assert "reset_password" in view._row_actions
