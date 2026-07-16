"""End-to-end integration tests for DatabaseUserBackend through the webserver.

Uses the same fixture pattern as ``test_auth.py`` so the full Starlette app
is wired up with a ``DatabaseUserBackend`` + ``SessionAuthProvider``.
"""

from __future__ import annotations

import asyncio

import pytest
from dagster._core.test_utils import instance_for_test
from dagster._core.workspace.load import (
    load_workspace_process_context_from_yaml_paths,
)
from dagster._utils import file_relative_path
from starlette.testclient import TestClient

from dagster_webserver.app import create_app_from_workspace_process_context
from dagster_webserver.auth import SessionAuthProvider
from dagster_webserver.auth.db_backend import DatabaseUserBackend
from dagster_webserver.auth.provider import AuthConfig

# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture()
def instance():
    with instance_for_test() as inst:
        yield inst


@pytest.fixture()
def workspace_process_context(instance):
    with load_workspace_process_context_from_yaml_paths(
        instance,
        [file_relative_path(__file__, "./workspace.yaml")],
    ) as ctx:
        yield ctx


@pytest.fixture()
def db_backend():
    """DatabaseUserBackend backed by an in-memory SQLite database."""
    return DatabaseUserBackend(
        "sqlite+aiosqlite:///:memory:",
        create_tables=True,
        default_role="viewer",
    )


@pytest.fixture()
def db_session_provider(db_backend: DatabaseUserBackend):
    config = AuthConfig(
        session_max_age=3600,
        allowed_routes=["login", "logout", "/server_info", "/dagit_info"],
    )
    config._session_secret = "test-secret-key"  # type: ignore[attr-defined]
    return SessionAuthProvider(db_backend, config=config)


@pytest.fixture()
def app_with_db_auth(workspace_process_context, db_session_provider):
    return create_app_from_workspace_process_context(
        workspace_process_context,
        auth_provider=db_session_provider,
    )


@pytest.fixture()
def admin_user(db_backend: DatabaseUserBackend):
    """Create an admin user in the test database."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            db_backend.create_user("admin", "admin_pass", role="admin")
        )
    finally:
        loop.close()


@pytest.fixture()
def viewer_user(db_backend: DatabaseUserBackend):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(
            db_backend.create_user("viewer", "viewer_pass", role="viewer")
        )
    finally:
        loop.close()


@pytest.fixture()
def analyst_user(db_backend: DatabaseUserBackend):
    """Create a custom role + user in the test database."""
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(
            db_backend.create_role(
                "analyst",
                permissions={
                    "LAUNCH_PIPELINE_EXECUTION": True,
                    "LAUNCH_PIPELINE_REEXECUTION": True,
                    "TERMINATE_PIPELINE_EXECUTION": True,
                },
            )
        )
        return loop.run_until_complete(
            db_backend.create_user("analyst", "analyst_pass", role="analyst")
        )
    finally:
        loop.close()


# ── Tests ───────────────────────────────────────────────────────────────────


class TestDbAuthIntegration:
    """Full login flow through the webserver with DatabaseUserBackend."""

    def test_login_and_me(self, app_with_db_auth, admin_user):
        client = TestClient(app_with_db_auth, raise_server_exceptions=False)
        resp = client.post(
            "/login",
            data={"username": "admin", "password": "admin_pass"},
            follow_redirects=False,
        )
        assert resp.status_code == 303

        resp = client.get("/api/me")
        assert resp.status_code == 200
        data = resp.json()
        assert data["username"] == "admin"
        assert data["role"] == "admin"

    def test_login_invalid_credentials(self, app_with_db_auth, admin_user):
        client = TestClient(app_with_db_auth, raise_server_exceptions=False)
        resp = client.post(
            "/login",
            data={"username": "admin", "password": "wrong"},
            follow_redirects=False,
        )
        assert resp.status_code == 400

    def test_api_me_requires_auth(self, app_with_db_auth):
        client = TestClient(app_with_db_auth, raise_server_exceptions=False)
        resp = client.get("/api/me")
        assert resp.status_code == 401

    def test_logout_clears_session(self, app_with_db_auth, admin_user):
        client = TestClient(app_with_db_auth, raise_server_exceptions=False)
        client.post(
            "/login",
            data={"username": "admin", "password": "admin_pass"},
            follow_redirects=False,
        )
        client.post("/logout", follow_redirects=False)
        resp = client.get("/api/me")
        assert resp.status_code == 401

    def test_graphql_permissions_viewer(self, app_with_db_auth, viewer_user):
        """Viewer should have no permissions enabled."""
        client = TestClient(app_with_db_auth, raise_server_exceptions=False)
        client.post(
            "/login",
            data={"username": "viewer", "password": "viewer_pass"},
            follow_redirects=False,
        )
        resp = client.post(
            "/graphql",
            json={"query": "{ permissions { permission value } }"},
        )
        assert resp.status_code == 200
        perms = resp.json()["data"]["permissions"]
        assert all(not entry["value"] for entry in perms)

    def test_graphql_permissions_custom_role(self, app_with_db_auth, analyst_user):
        """Custom role should reflect the permissions stored in the DB."""
        client = TestClient(app_with_db_auth, raise_server_exceptions=False)
        client.post(
            "/login",
            data={"username": "analyst", "password": "analyst_pass"},
            follow_redirects=False,
        )
        resp = client.post(
            "/graphql",
            json={"query": "{ permissions { permission value } }"},
        )
        assert resp.status_code == 200
        perms = {
            entry["permission"]: entry["value"]
            for entry in resp.json()["data"]["permissions"]
        }
        assert perms["LAUNCH_PIPELINE_EXECUTION"] is True
        assert perms["LAUNCH_PIPELINE_REEXECUTION"] is True
        assert perms["TERMINATE_PIPELINE_EXECUTION"] is True
        # Permissions not in the custom role map are absent from the response
        assert "EDIT_SENSOR" not in perms

    def test_inactive_user_rejected(self, db_backend: DatabaseUserBackend):
        """Inactive user should be rejected at the backend level."""
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(
                db_backend.create_user("inactive", "pass", role="viewer")
            )
            loop.run_until_complete(db_backend.update_user("inactive", is_active=False))
            result = loop.run_until_complete(
                db_backend.authenticate("inactive", "pass")
            )
        finally:
            loop.close()
        assert result is None

    def test_null_role_uses_default(self, db_backend: DatabaseUserBackend):
        """User with no role_id should fall back to the default role."""
        loop = asyncio.new_event_loop()
        try:
            user = loop.run_until_complete(
                db_backend.create_user("nole", "pass", role=None)
            )
        finally:
            loop.close()
        assert user.role == "viewer"
