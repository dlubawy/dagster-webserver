"""Tests for dagster_webserver.database — models, engine, migrations."""

from __future__ import annotations

import pytest

from dagster_webserver.database import Base, init_engine
from dagster_webserver.database.engine import get_engine, get_session_factory


@pytest.fixture()
def engine_and_session():
    """Create an in-memory SQLite engine and session factory."""
    engine, session_factory = init_engine("sqlite+aiosqlite:///:memory:")
    return engine, session_factory


class TestDatabaseModels:
    def test_base_has_roles_and_users_tables(self):
        assert "roles" in Base.metadata.tables
        assert "users" in Base.metadata.tables

    def test_role_model_columns(self):
        role_table = Base.metadata.tables["roles"]
        assert "id" in role_table.columns
        assert "name" in role_table.columns
        assert "permissions" in role_table.columns
        assert "is_builtin" in role_table.columns
        assert "created_at" in role_table.columns
        assert "updated_at" in role_table.columns

    def test_user_model_columns(self):
        user_table = Base.metadata.tables["users"]
        assert "id" in user_table.columns
        assert "username" in user_table.columns
        assert "password_hash" in user_table.columns
        assert "role_id" in user_table.columns
        assert "email" in user_table.columns
        assert "display_name" in user_table.columns
        assert "is_active" in user_table.columns
        assert "created_at" in user_table.columns
        assert "updated_at" in user_table.columns

    def test_user_role_id_is_nullable(self):
        user_table = Base.metadata.tables["users"]
        role_id_col = user_table.columns["role_id"]
        assert role_id_col.nullable is True

    def test_role_name_is_unique(self):
        role_table = Base.metadata.tables["roles"]
        name_col = role_table.columns["name"]
        assert name_col.unique is True

    def test_user_username_is_unique(self):
        user_table = Base.metadata.tables["users"]
        username_col = user_table.columns["username"]
        assert username_col.unique is True


class TestDatabaseEngine:
    def test_init_engine_creates_engine(self, engine_and_session):
        engine, session_factory = engine_and_session
        assert engine is not None
        assert session_factory is not None

    def test_get_engine_returns_engine(self, engine_and_session):
        engine, _ = engine_and_session
        assert get_engine() is engine

    def test_get_session_factory_returns_factory(self, engine_and_session):
        _, session_factory = engine_and_session
        assert get_session_factory() is session_factory

    def test_get_engine_raises_before_init(self):
        # Reset module state
        import dagster_webserver.database.engine as eng

        original_engine = eng._engine
        original_factory = eng._session_factory
        try:
            eng._engine = None
            eng._session_factory = None
            with pytest.raises(RuntimeError, match="not initialised"):
                get_engine()
            with pytest.raises(RuntimeError, match="not initialised"):
                get_session_factory()
        finally:
            eng._engine = original_engine
            eng._session_factory = original_factory

    def test_mask_url_hides_credentials(self):
        from dagster_webserver.database.engine import _mask_url

        assert "secret" not in _mask_url("postgresql+asyncpg://user:secret@host/db")
        assert "postgresql" in _mask_url("postgresql+asyncpg://user:secret@host/db")
