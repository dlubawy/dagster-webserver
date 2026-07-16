"""Tests for DatabaseUserBackend."""

from __future__ import annotations

import pytest

from dagster_webserver.auth.db_backend import DatabaseUserBackend

pytestmark = pytest.mark.asyncio


@pytest.fixture()
def backend():
    """Create a DatabaseUserBackend with an in-memory SQLite database."""
    return DatabaseUserBackend(
        "sqlite+aiosqlite:///:memory:",
        create_tables=True,
        default_role="viewer",
    )


# ── Role CRUD ───────────────────────────────────────────────────


class TestRoleCRUD:
    async def test_list_builtin_roles(self, backend: DatabaseUserBackend):
        roles = await backend.list_roles()
        names = [r.name for r in roles]
        # All five built-in roles should be seeded
        for expected in ("catalog_viewer", "viewer", "launcher", "editor", "admin"):
            assert expected in names

    async def test_get_builtin_role(self, backend: DatabaseUserBackend):
        role = await backend.get_role("admin")
        assert role is not None
        assert role.is_builtin is True
        assert role.name == "admin"

    async def test_get_unknown_role(self, backend: DatabaseUserBackend):
        assert await backend.get_role("nonexistent") is None

    async def test_create_custom_role(self, backend: DatabaseUserBackend):
        role = await backend.create_role(
            "analyst",
            permissions={"read_workspace": True, "launch_pipeline_execution": False},
        )
        assert role.name == "analyst"
        assert role.is_builtin is False
        assert role.permissions == {
            "read_workspace": True,
            "launch_pipeline_execution": False,
        }

    async def test_create_duplicate_role_raises(self, backend: DatabaseUserBackend):
        await backend.create_role("dup", permissions={"read_workspace": True})
        with pytest.raises(Exception):  # IntegrityError
            await backend.create_role("dup", permissions={"read_workspace": False})

    async def test_update_custom_role(self, backend: DatabaseUserBackend):
        await backend.create_role("analyst", permissions={"read_workspace": True})
        updated = await backend.update_role(
            "analyst",
            permissions={"read_workspace": True, "launch_pipeline_execution": True},
        )
        assert updated.permissions["launch_pipeline_execution"] is True

    async def test_update_builtin_role_raises(self, backend: DatabaseUserBackend):
        with pytest.raises(ValueError, match="cannot be modified"):
            await backend.update_role("admin", permissions={})

    async def test_update_unknown_role_raises(self, backend: DatabaseUserBackend):
        with pytest.raises(ValueError, match="not found"):
            await backend.update_role("nonexistent", permissions={})

    async def test_delete_custom_role(self, backend: DatabaseUserBackend):
        await backend.create_role("temp", permissions={})
        await backend.delete_role("temp")
        assert await backend.get_role("temp") is None

    async def test_delete_builtin_role_raises(self, backend: DatabaseUserBackend):
        with pytest.raises(ValueError, match="cannot be deleted"):
            await backend.delete_role("viewer")

    async def test_delete_unknown_role_raises(self, backend: DatabaseUserBackend):
        with pytest.raises(ValueError, match="not found"):
            await backend.delete_role("nonexistent")

    async def test_delete_role_with_users_raises(self, backend: DatabaseUserBackend):
        await backend.create_role("temp", permissions={})
        await backend.create_user("u1", "pass", role="temp")
        with pytest.raises(ValueError, match="still assigned"):
            await backend.delete_role("temp")


# ── User CRUD ───────────────────────────────────────────────────


class TestUserCRUD:
    async def test_create_user(self, backend: DatabaseUserBackend):
        user = await backend.create_user("alice", "secret", role="admin")
        assert user.username == "alice"
        assert user.role == "admin"

    async def test_create_user_no_role(self, backend: DatabaseUserBackend):
        """User created without a role should fall back to default at resolution."""
        user = await backend.create_user("bob", "secret", role=None)
        assert user.username == "bob"
        # Falls back to default_role ("viewer")
        assert user.role == "viewer"

    async def test_create_user_unknown_role_raises(self, backend: DatabaseUserBackend):
        with pytest.raises(ValueError, match="not found"):
            await backend.create_user("charlie", "pass", role="nonexistent")

    async def test_create_duplicate_user_raises(self, backend: DatabaseUserBackend):
        await backend.create_user("dup", "pass")
        with pytest.raises(Exception):  # IntegrityError
            await backend.create_user("dup", "pass2")

    async def test_authenticate_valid(self, backend: DatabaseUserBackend):
        await backend.create_user("alice", "secret", role="editor")
        user = await backend.authenticate("alice", "secret")
        assert user is not None
        assert user.username == "alice"
        assert user.role == "editor"

    async def test_authenticate_invalid_password(self, backend: DatabaseUserBackend):
        await backend.create_user("alice", "secret")
        assert await backend.authenticate("alice", "wrong") is None

    async def test_authenticate_unknown_user(self, backend: DatabaseUserBackend):
        assert await backend.authenticate("nobody", "pass") is None

    async def test_authenticate_inactive_user(self, backend: DatabaseUserBackend):
        await backend.create_user("alice", "secret")
        await backend.update_user("alice", is_active=False)
        assert await backend.authenticate("alice", "secret") is None

    async def test_get_user(self, backend: DatabaseUserBackend):
        await backend.create_user("alice", "secret", role="launcher")
        user = await backend.get_user("alice")
        assert user is not None
        assert user.role == "launcher"

    async def test_get_user_inactive(self, backend: DatabaseUserBackend):
        await backend.create_user("alice", "secret")
        await backend.update_user("alice", is_active=False)
        assert await backend.get_user("alice") is None

    async def test_get_user_not_found(self, backend: DatabaseUserBackend):
        assert await backend.get_user("nobody") is None

    async def test_update_user_password(self, backend: DatabaseUserBackend):
        await backend.create_user("alice", "old")
        await backend.update_user("alice", password="new")
        assert await backend.authenticate("alice", "new") is not None
        assert await backend.authenticate("alice", "old") is None

    async def test_update_user_role(self, backend: DatabaseUserBackend):
        await backend.create_user("alice", "pass", role="viewer")
        updated = await backend.update_user("alice", role="admin")
        assert updated.role == "admin"

    async def test_update_user_unknown_raises(self, backend: DatabaseUserBackend):
        with pytest.raises(ValueError, match="not found"):
            await backend.update_user("nobody", email="x@x.com")

    async def test_delete_user(self, backend: DatabaseUserBackend):
        await backend.create_user("alice", "pass")
        await backend.delete_user("alice")
        assert await backend.get_user("alice") is None

    async def test_delete_unknown_user_raises(self, backend: DatabaseUserBackend):
        with pytest.raises(ValueError, match="not found"):
            await backend.delete_user("nobody")

    async def test_list_users(self, backend: DatabaseUserBackend):
        await backend.create_user("alice", "pass", role="editor")
        await backend.create_user("bob", "pass", role="viewer")
        users = await backend.list_users()
        names = [u.username for u in users]
        assert "alice" in names
        assert "bob" in names

    async def test_list_users_excludes_inactive(self, backend: DatabaseUserBackend):
        await backend.create_user("alice", "pass")
        await backend.create_user("bob", "pass")
        await backend.update_user("bob", is_active=False)
        users = await backend.list_users()
        names = [u.username for u in users]
        assert "alice" in names
        assert "bob" not in names


# ── Role resolution (null role_id fallback) ─────────────────────


class TestRoleResolution:
    async def test_to_auth_user_builtin_role(self, backend: DatabaseUserBackend):
        await backend.create_user("alice", "pass", role="editor")
        user = await backend.get_user("alice")
        assert user is not None
        assert user.role == "editor"
        assert user.custom_permissions is None

    async def test_to_auth_user_custom_role(self, backend: DatabaseUserBackend):
        perms = {"read_workspace": True, "launch_pipeline_execution": False}
        await backend.create_role("analyst", permissions=perms)
        await backend.create_user("alice", "pass", role="analyst")
        user = await backend.get_user("alice")
        assert user is not None
        assert user.role == "custom"
        assert user.custom_permissions == perms

    async def test_to_auth_user_null_role_falls_back_to_default(
        self, backend: DatabaseUserBackend
    ):
        await backend.create_user("alice", "pass", role=None)
        user = await backend.get_user("alice")
        assert user is not None
        # Default role is "viewer"
        assert user.role == "viewer"
        assert user.custom_permissions is None

    async def test_authenticate_null_role_falls_back_to_default(
        self, backend: DatabaseUserBackend
    ):
        await backend.create_user("alice", "pass", role=None)
        user = await backend.authenticate("alice", "pass")
        assert user is not None
        assert user.role == "viewer"

    async def test_custom_default_role(self):
        """When default_role is set to 'admin', null role users get admin."""
        db = DatabaseUserBackend(
            "sqlite+aiosqlite:///:memory:",
            create_tables=True,
            default_role="admin",
        )
        await db.create_user("alice", "pass", role=None)
        user = await db.get_user("alice")
        assert user is not None
        assert user.role == "admin"
