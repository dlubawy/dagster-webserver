"""Tests for the auth module and its integration with dagster-webserver."""

from __future__ import annotations

import tempfile
import textwrap
from unittest import mock

import pytest
from dagster._core.test_utils import instance_for_test
from dagster._core.workspace.permissions import Permissions
from dagster._utils import file_relative_path
from starlette.testclient import TestClient

from dagster_webserver.app import create_app_from_workspace_process_context
from dagster_webserver.auth import (
    AuthConfig,
    AuthUser,
    FileUserBackend,
    InMemoryUserBackend,
    Role,
    SessionAuthProvider,
    get_custom_permissions,
    get_role_permissions,
)
from dagster_webserver.auth.context import (
    AuthenticatedWorkspaceProcessContext,
    AuthenticatedWorkspaceRequestContext,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def instance():
    with instance_for_test() as inst:
        yield inst


@pytest.fixture
def workspace_process_context(instance):
    from dagster._core.workspace.load import (
        load_workspace_process_context_from_yaml_paths,
    )

    with load_workspace_process_context_from_yaml_paths(
        instance,
        [file_relative_path(__file__, "./workspace.yaml")],
    ) as ctx:
        yield ctx


@pytest.fixture
def users_dict():
    return {
        "admin": {
            "password": "admin_pass",
            "role": "admin",
            "email": "admin@example.com",
        },
        "editor": {"password": "editor_pass", "role": "editor"},
        "viewer": {"password": "viewer_pass", "role": "viewer"},
        "launcher": {"password": "launch_pass", "role": "launcher"},
    }


@pytest.fixture
def in_memory_backend(users_dict):
    return InMemoryUserBackend(users_dict)


@pytest.fixture
def session_provider(in_memory_backend):
    config = AuthConfig(
        session_max_age=3600,
        allowed_routes=["login", "logout", "/server_info", "/dagit_info"],
    )
    config._session_secret = "test-secret-key"  # type: ignore[attr-defined]
    return SessionAuthProvider(in_memory_backend, config=config)


@pytest.fixture
def app_with_auth(workspace_process_context, session_provider):
    return create_app_from_workspace_process_context(
        workspace_process_context,
        auth_provider=session_provider,
    )


@pytest.fixture
def app_no_auth(workspace_process_context):
    return create_app_from_workspace_process_context(workspace_process_context)


# ---------------------------------------------------------------------------
# Role permission map tests
# ---------------------------------------------------------------------------


class TestRolePermissions:
    def test_viewer_has_no_permissions(self):
        perms = get_role_permissions(Role.VIEWER)
        for perm, result in perms.items():
            assert not result.enabled, f"VIEWER should not have {perm}"

    def test_editor_has_all_permissions(self):
        perms = get_role_permissions(Role.EDITOR)
        for perm, result in perms.items():
            assert result.enabled, f"EDITOR should have {perm}"

    def test_admin_has_all_permissions(self):
        perms = get_role_permissions(Role.ADMIN)
        for perm, result in perms.items():
            assert result.enabled, f"ADMIN should have {perm}"

    def test_launcher_can_launch_runs(self):
        perms = get_role_permissions(Role.LAUNCHER)
        assert perms[Permissions.LAUNCH_PIPELINE_EXECUTION].enabled
        assert perms[Permissions.LAUNCH_PIPELINE_REEXECUTION].enabled
        assert perms[Permissions.TERMINATE_PIPELINE_EXECUTION].enabled
        assert perms[Permissions.LAUNCH_PARTITION_BACKFILL].enabled
        assert perms[Permissions.CANCEL_PARTITION_BACKFILL].enabled

    def test_launcher_cannot_edit_schedules(self):
        perms = get_role_permissions(Role.LAUNCHER)
        assert not perms[Permissions.START_SCHEDULE].enabled
        assert not perms[Permissions.EDIT_SENSOR].enabled
        assert not perms[Permissions.RELOAD_WORKSPACE].enabled

    def test_catalog_viewer_has_no_permissions(self):
        perms = get_role_permissions(Role.CATALOG_VIEWER)
        for perm, result in perms.items():
            assert not result.enabled, f"CATALOG_VIEWER should not have {perm}"

    def test_custom_permissions(self):
        custom = get_custom_permissions(
            {
                Permissions.LAUNCH_PIPELINE_EXECUTION: True,
                Permissions.START_SCHEDULE: True,
            }
        )
        assert custom[Permissions.LAUNCH_PIPELINE_EXECUTION].enabled
        assert custom[Permissions.START_SCHEDULE].enabled
        # Only the keys in the input dict are present
        assert Permissions.EDIT_SENSOR not in custom

    def test_disabled_reason_message(self):
        perms = get_role_permissions(Role.VIEWER)
        for perm, result in perms.items():
            if not result.enabled:
                assert result.disabled_reason is not None
                assert (
                    str(Role.VIEWER.value) in result.disabled_reason
                    and str(perm) in result.disabled_reason
                )


# ---------------------------------------------------------------------------
# User backend tests
# ---------------------------------------------------------------------------


class TestInMemoryUserBackend:
    @pytest.mark.asyncio
    async def test_authenticate_valid_user(self, in_memory_backend):
        user = await in_memory_backend.authenticate("admin", "admin_pass")
        assert user is not None
        assert user.username == "admin"
        assert user.role == "admin"
        assert user.email == "admin@example.com"

    @pytest.mark.asyncio
    async def test_authenticate_invalid_password(self, in_memory_backend):
        user = await in_memory_backend.authenticate("admin", "wrong_password")
        assert user is None

    @pytest.mark.asyncio
    async def test_authenticate_unknown_user(self, in_memory_backend):
        user = await in_memory_backend.authenticate("nobody", "anything")
        assert user is None

    @pytest.mark.asyncio
    async def test_get_user(self, in_memory_backend):
        user = await in_memory_backend.get_user("editor")
        assert user is not None
        assert user.role == "editor"

    @pytest.mark.asyncio
    async def test_get_user_missing(self, in_memory_backend):
        user = await in_memory_backend.get_user("nonexistent")
        assert user is None


class TestFileUserBackend:
    @pytest.mark.asyncio
    async def test_load_yaml_file(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            f.write(
                textwrap.dedent(
                    """\
                    users:
                      alice:
                        password: secret
                        role: editor
                      bob:
                        password: bobpass
                        role: viewer
                    """
                )
            )
            f.flush()
            backend = FileUserBackend(f.name)

        user = await backend.authenticate("alice", "secret")
        assert user is not None
        assert user.role == "editor"

        user2 = await backend.authenticate("bob", "bobpass")
        assert user2 is not None
        assert user2.role == "viewer"

        user3 = await backend.authenticate("alice", "wrong")
        assert user3 is None

    @pytest.mark.asyncio
    async def test_load_json_file(self):
        import json

        with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
            json.dump(
                {"users": {"charlie": {"password": "charlie_pass", "role": "admin"}}},
                f,
            )
            f.flush()
            backend = FileUserBackend(f.name)

        user = await backend.authenticate("charlie", "charlie_pass")
        assert user is not None
        assert user.role == "admin"

    @pytest.mark.asyncio
    async def test_missing_file(self, tmp_path):
        backend = FileUserBackend(str(tmp_path / "nonexistent.yaml"))
        user = await backend.authenticate("nobody", "nada")
        assert user is None


# ---------------------------------------------------------------------------
# Auth provider tests
# ---------------------------------------------------------------------------


class TestSessionAuthProvider:
    @pytest.mark.asyncio
    async def test_login_sets_session(self, session_provider):
        class FakeRequest:
            session = {}
            state = mock.MagicMock()

        request = FakeRequest()
        user = await session_provider.login("admin", "admin_pass", request)
        assert user is not None
        assert request.session["username"] == "admin"

    @pytest.mark.asyncio
    async def test_login_invalid_credentials(self, session_provider):
        class FakeRequest:
            session = {}
            state = mock.MagicMock()

        request = FakeRequest()
        user = await session_provider.login("admin", "wrong", request)
        assert user is None
        assert "username" not in request.session

    @pytest.mark.asyncio
    async def test_logout_clears_session(self, session_provider):
        class FakeRequest:
            session = {"username": "admin"}
            state = mock.MagicMock()

        request = FakeRequest()
        await session_provider.logout(request)
        assert "username" not in request.session

    @pytest.mark.asyncio
    async def test_authenticate_request_with_session(self, session_provider):
        class FakeState:
            user = None

        class FakeRequest:
            session = {"username": "editor"}
            state = FakeState()

        request = FakeRequest()
        user = await session_provider.authenticate_request(request)
        assert user is not None
        assert user.username == "editor"
        assert request.state.user is not None

    @pytest.mark.asyncio
    async def test_authenticate_request_no_session(self, session_provider):
        class FakeRequest:
            session = {}
            state = mock.MagicMock()

        request = FakeRequest()
        user = await session_provider.authenticate_request(request)
        assert user is None

    def test_get_user_permissions_editor(self, session_provider):
        user = AuthUser(username="test", role="editor")
        perms = session_provider.get_user_permissions(user)
        assert perms[Permissions.LAUNCH_PIPELINE_EXECUTION].enabled

    def test_get_user_permissions_viewer(self, session_provider):
        user = AuthUser(username="test", role="viewer")
        perms = session_provider.get_user_permissions(user)
        assert not perms[Permissions.LAUNCH_PIPELINE_EXECUTION].enabled

    def test_get_user_permissions_unknown_role_falls_back(self, session_provider):
        user = AuthUser(username="test", role="unknown_role")
        perms = session_provider.get_user_permissions(user)
        # Should fall back to VIEWER (all disabled)
        assert not perms[Permissions.LAUNCH_PIPELINE_EXECUTION].enabled


# ---------------------------------------------------------------------------
# AuthenticatedWorkspaceRequestContext tests
# ---------------------------------------------------------------------------


class TestAuthenticatedWorkspaceRequestContext:
    def test_permissions_from_role(self, workspace_process_context, session_provider):
        user = AuthUser(username="editor", role="editor")
        ctx = workspace_process_context.create_request_context()
        # Manually build an authenticated context
        auth_ctx = AuthenticatedWorkspaceRequestContext(
            instance=ctx.instance,
            current_workspace=ctx.get_current_workspace(),
            process_context=ctx.process_context,
            version=ctx.version,
            source=ctx.source,
            read_only=ctx.read_only,
            user=user,
            auth_provider=session_provider,
        )
        perms = auth_ctx.permissions
        assert perms[Permissions.LAUNCH_PIPELINE_EXECUTION].enabled

    def test_permissions_fallback_no_user(
        self, workspace_process_context, session_provider
    ):
        """When no user, fall back to standard read_only behavior."""
        ctx = workspace_process_context.create_request_context()
        auth_ctx = AuthenticatedWorkspaceRequestContext(
            instance=ctx.instance,
            current_workspace=ctx.get_current_workspace(),
            process_context=ctx.process_context,
            version=ctx.version,
            source=ctx.source,
            read_only=ctx.read_only,
            user=None,
            auth_provider=session_provider,
        )
        # Should fall back to the standard permissions (read_only=False = editor)
        perms = auth_ctx.permissions
        assert perms[Permissions.LAUNCH_PIPELINE_EXECUTION].enabled

    def test_viewer_tags_includes_user(
        self, workspace_process_context, session_provider
    ):
        user = AuthUser(username="testuser", role="editor")
        ctx = workspace_process_context.create_request_context()
        auth_ctx = AuthenticatedWorkspaceRequestContext(
            instance=ctx.instance,
            current_workspace=ctx.get_current_workspace(),
            process_context=ctx.process_context,
            version=ctx.version,
            source=ctx.source,
            read_only=ctx.read_only,
            user=user,
            auth_provider=session_provider,
        )
        tags = auth_ctx.get_viewer_tags()
        assert tags.get("dagster.io/user") == "testuser"
        assert tags.get("dagster.io/role") == "editor"

    def test_reporting_user_tags_includes_user(
        self, workspace_process_context, session_provider
    ):
        user = AuthUser(username="reporter", role="viewer")
        ctx = workspace_process_context.create_request_context()
        auth_ctx = AuthenticatedWorkspaceRequestContext(
            instance=ctx.instance,
            current_workspace=ctx.get_current_workspace(),
            process_context=ctx.process_context,
            version=ctx.version,
            source=ctx.source,
            read_only=ctx.read_only,
            user=user,
            auth_provider=session_provider,
        )
        tags = auth_ctx.get_reporting_user_tags()
        assert tags.get("dagster.io/user") == "reporter"


# ---------------------------------------------------------------------------
# AuthenticatedWorkspaceProcessContext tests
# ---------------------------------------------------------------------------


class TestAuthenticatedWorkspaceProcessContext:
    def test_creates_authenticated_context(
        self, workspace_process_context, session_provider
    ):
        auth_ctx = AuthenticatedWorkspaceProcessContext(
            inner=workspace_process_context,
            auth_provider=session_provider,
        )
        request_ctx = auth_ctx.create_request_context()
        assert isinstance(request_ctx, AuthenticatedWorkspaceRequestContext)
        assert request_ctx.user is None  # No source = no user

    def test_delegates_to_inner(self, workspace_process_context, session_provider):
        auth_ctx = AuthenticatedWorkspaceProcessContext(
            inner=workspace_process_context,
            auth_provider=session_provider,
        )
        assert auth_ctx.instance is workspace_process_context.instance
        assert auth_ctx.version == workspace_process_context.version


# ---------------------------------------------------------------------------
# End-to-end integration tests with TestClient
# ---------------------------------------------------------------------------


class TestAuthIntegration:
    """Tests that verify the auth middleware, routes, and permission
    enforcement work end-to-end through the Starlette TestClient.
    """

    def test_no_auth_allows_all(self, app_no_auth):
        """Without auth provider, all routes are accessible."""
        client = TestClient(app_no_auth, raise_server_exceptions=False)
        resp = client.post(
            "/graphql", json={"query": "{ permissions { permission value } }"}
        )
        # Should succeed without auth
        assert resp.status_code == 200

    def test_auth_enabled_blocks_graphql(self, app_with_auth):
        """With auth, unauthenticated GraphQL requests get 401."""
        client = TestClient(app_with_auth, raise_server_exceptions=False)
        resp = client.post(
            "/graphql", json={"query": "{ permissions { permission value } }"}
        )
        assert resp.status_code == 401

    def test_login_returns_form_on_get(self, app_with_auth):
        client = TestClient(app_with_auth, raise_server_exceptions=False)
        resp = client.get("/login", headers={"Accept": "text/html"})
        assert resp.status_code == 200
        assert b"Sign in" in resp.content

    def test_login_authenticates_on_post(self, app_with_auth):
        client = TestClient(app_with_auth, raise_server_exceptions=False, cookies={})
        # POST login with valid credentials
        resp = client.post(
            "/login",
            data={"username": "admin", "password": "admin_pass"},
            follow_redirects=False,
        )
        # Should redirect after successful login
        assert resp.status_code in (303, 302, 200)

    def test_login_rejects_invalid_credentials(self, app_with_auth):
        client = TestClient(app_with_auth, raise_server_exceptions=False)
        resp = client.post(
            "/login",
            data={"username": "admin", "password": "wrong"},
            follow_redirects=False,
        )
        assert resp.status_code == 400

    def test_api_me_returns_user_info_after_login(self, app_with_auth):
        client = TestClient(app_with_auth, raise_server_exceptions=False)
        # Login first (POST to /login) — session cookie persists in client
        client.post("/login", data={"username": "editor", "password": "editor_pass"})
        # Check /api/me
        resp = client.get("/api/me")
        assert resp.status_code == 200
        data = resp.json()
        assert data["username"] == "editor"
        assert data["role"] == "editor"

    def test_api_me_requires_auth(self, app_with_auth):
        client = TestClient(app_with_auth, raise_server_exceptions=False)
        resp = client.get("/api/me")
        assert resp.status_code == 401

    def test_logout_clears_session(self, app_with_auth):
        client = TestClient(app_with_auth, raise_server_exceptions=False)
        # Login
        client.post("/login", data={"username": "admin", "password": "admin_pass"})
        # Verify logged in
        resp = client.get("/api/me")
        assert resp.status_code == 200
        # Logout
        client.get("/logout", follow_redirects=False)
        # Verify logged out
        resp = client.get("/api/me")
        assert resp.status_code == 401

    def test_graphql_permissions_reflect_role(self, app_with_auth):
        """After login, GraphQL permissions query should reflect user's role."""
        client = TestClient(app_with_auth, raise_server_exceptions=False)
        # Login as viewer
        client.post("/login", data={"username": "viewer", "password": "viewer_pass"})

        query = "{ permissions { permission value disabledReason } }"
        resp = client.post("/graphql", json={"query": query})
        assert resp.status_code == 200
        data = resp.json()
        assert "data" in data
        perms = data["data"]["permissions"]
        # Viewer should have all permissions disabled
        for perm in perms:
            assert not perm["value"], f"Viewer should not have {perm['permission']}"

    def test_graphql_permissions_editor(self, app_with_auth):
        """After login as editor, all permissions should be enabled."""
        client = TestClient(app_with_auth, raise_server_exceptions=False)
        client.post("/login", data={"username": "editor", "password": "editor_pass"})

        query = "{ permissions { permission value } }"
        resp = client.post("/graphql", json={"query": query})
        assert resp.status_code == 200
        data = resp.json()
        perms = data["data"]["permissions"]
        for perm in perms:
            assert perm["value"], f"Editor should have {perm['permission']}"

    def test_graphql_permissions_launcher(self, app_with_auth):
        """Launcher should have run permissions but not schedule permissions."""
        client = TestClient(app_with_auth, raise_server_exceptions=False)
        client.post("/login", data={"username": "launcher", "password": "launch_pass"})

        query = "{ permissions { permission value } }"
        resp = client.post("/graphql", json={"query": query})
        assert resp.status_code == 200
        data = resp.json()
        perms = {p["permission"]: p["value"] for p in data["data"]["permissions"]}
        assert perms["launch_pipeline_execution"] is True
        assert perms["start_schedule"] is False

    def test_server_info_allowed_without_auth(self, app_with_auth):
        """/server_info should be accessible without auth (in allowed_routes)."""
        client = TestClient(app_with_auth, raise_server_exceptions=False)
        resp = client.get("/server_info")
        assert resp.status_code == 200
