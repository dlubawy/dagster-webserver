"""Tests for DynamicAgentWorkspaceTarget and AgentWorkspaceRefresher."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import yaml


class TestDynamicAgentWorkspaceTarget:
    def test_create_origins_empty_db(self):
        """With no heartbeats in the DB, returns empty origins."""
        from dagster_webserver.api.agent_workspace import DynamicAgentWorkspaceTarget

        target = DynamicAgentWorkspaceTarget(
            db_url="sqlite+aiosqlite:///:memory:",
        )
        # Will log a warning about missing table but should return []
        origins = target.create_origins()
        assert origins == []

    def test_create_origins_with_data(self, tmp_path):
        """Returns gRPC origins for running locations with fresh heartbeats."""
        from dagster_webserver.api.agent_workspace import DynamicAgentWorkspaceTarget

        db_file = tmp_path / "test.db"
        db_url = f"sqlite+aiosqlite:///{db_file}"

        # Set up DB with a heartbeat
        import asyncio

        async def _setup():
            from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

            from dagster_webserver.database.models import (
                AgentHeartbeat,
                Base,
            )

            engine = create_async_engine(db_url)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            async with AsyncSession(engine) as session:
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                hb = AgentHeartbeat(
                    agent_id="test-agent",
                    timestamp=now,
                    code_server_heartbeats={
                        "my_location": {
                            "status": "running",
                            "endpoint": "127.0.0.1:54321",
                        }
                    },
                )
                session.add(hb)
                await session.commit()
            await engine.dispose()

        asyncio.run(_setup())

        target = DynamicAgentWorkspaceTarget(db_url=db_url)
        origins = target.create_origins()
        assert len(origins) == 1
        assert origins[0].location_name == "my_location"
        assert origins[0].host == "127.0.0.1"
        assert origins[0].port == 54321

    def test_create_origins_filters_stale(self, tmp_path):
        """Heartbeats older than staleness window are ignored."""
        from datetime import timedelta

        from dagster_webserver.api.agent_workspace import DynamicAgentWorkspaceTarget

        db_file = tmp_path / "test.db"
        db_url = f"sqlite+aiosqlite:///{db_file}"

        import asyncio

        async def _setup():
            from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

            from dagster_webserver.database.models import (
                AgentHeartbeat,
                Base,
            )

            engine = create_async_engine(db_url)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            async with AsyncSession(engine) as session:
                # Stale heartbeat (5 minutes old, beyond 120s window)
                stale_time = datetime.now(timezone.utc).replace(
                    tzinfo=None
                ) - timedelta(minutes=5)
                hb = AgentHeartbeat(
                    agent_id="test-agent",
                    timestamp=stale_time,
                    code_server_heartbeats={
                        "stale_location": {
                            "status": "running",
                            "endpoint": "127.0.0.1:9999",
                        }
                    },
                )
                session.add(hb)
                await session.commit()
            await engine.dispose()

        asyncio.run(_setup())

        target = DynamicAgentWorkspaceTarget(db_url=db_url, staleness_seconds=120)
        origins = target.create_origins()
        assert origins == []


class TestAgentWorkspaceRefresher:
    def _make_mock_wpc(self):
        class MockWPC:
            code_location_names: list[str] = []

            def reload_workspace(self):
                pass

        return MockWPC()

    def test_start_seeds_file_with_empty_load_from(self, tmp_path):
        """File is created immediately with load_from: [] on start."""
        from dagster_webserver.api.agent_workspace import AgentWorkspaceRefresher

        ws_file = str(tmp_path / "agent_workspace.yaml")
        refresher = AgentWorkspaceRefresher(
            self._make_mock_wpc(),
            db_url="sqlite+aiosqlite:///:memory:",
            workspace_file_path=ws_file,
        )
        refresher.start()
        try:
            assert os.path.exists(ws_file)
            with open(ws_file) as f:
                content = yaml.safe_load(f)
            assert content == {"load_from": []}
        finally:
            refresher.stop()

    def test_start_creates_parent_dirs(self, tmp_path):
        """Parent directories are created if they don't exist."""
        from dagster_webserver.api.agent_workspace import AgentWorkspaceRefresher

        ws_file = str(tmp_path / "nested" / "dir" / "agent_workspace.yaml")
        refresher = AgentWorkspaceRefresher(
            self._make_mock_wpc(),
            db_url="sqlite+aiosqlite:///:memory:",
            workspace_file_path=ws_file,
        )
        refresher.start()
        try:
            assert os.path.exists(ws_file)
        finally:
            refresher.stop()

    def test_write_workspace_file_with_endpoints(self, tmp_path):
        """File contains gRPC server entries when endpoints exist."""
        from dagster_webserver.api.agent_workspace import AgentWorkspaceRefresher

        ws_file = str(tmp_path / "agent_workspace.yaml")
        db_file = tmp_path / "test.db"
        db_url = f"sqlite+aiosqlite:///{db_file}"

        # Set up DB with a heartbeat
        import asyncio

        async def _setup():
            from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

            from dagster_webserver.database.models import (
                AgentHeartbeat,
                Base,
            )

            engine = create_async_engine(db_url)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            async with AsyncSession(engine) as session:
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                hb = AgentHeartbeat(
                    agent_id="test-agent",
                    timestamp=now,
                    code_server_heartbeats={
                        "loc_a": {"status": "running", "endpoint": "10.0.0.1:5000"},
                        "loc_b": {"status": "running", "endpoint": "10.0.0.2:5001"},
                    },
                )
                session.add(hb)
                await session.commit()
            await engine.dispose()

        asyncio.run(_setup())

        refresher = AgentWorkspaceRefresher(
            self._make_mock_wpc(),
            db_url=db_url,
            workspace_file_path=ws_file,
        )
        # Call _write_workspace_file directly (no thread needed for test)
        refresher._write_workspace_file()

        with open(ws_file) as f:
            content = yaml.safe_load(f)

        assert len(content["load_from"]) == 2
        names = [
            entry["grpc_server"]["location_name"] for entry in content["load_from"]
        ]
        assert names == ["loc_a", "loc_b"]
        # Verify structure matches what dagster-daemon expects
        entry = content["load_from"][0]
        assert "grpc_server" in entry
        assert entry["grpc_server"]["host"] == "10.0.0.1"
        assert entry["grpc_server"]["port"] == 5000

    def test_write_workspace_file_atomic(self, tmp_path):
        """No temp files are left behind after write."""
        from dagster_webserver.api.agent_workspace import AgentWorkspaceRefresher

        ws_file = str(tmp_path / "agent_workspace.yaml")
        refresher = AgentWorkspaceRefresher(
            self._make_mock_wpc(),
            db_url="sqlite+aiosqlite:///:memory:",
            workspace_file_path=ws_file,
        )
        refresher._write_workspace_file()

        # No leftover temp files
        remaining = [
            f for f in os.listdir(tmp_path) if f.startswith(".agent_workspace_")
        ]
        assert remaining == []

    def test_no_file_when_path_is_none(self, tmp_path):
        """Refresher works without a file path (file writing is skipped)."""
        from dagster_webserver.api.agent_workspace import AgentWorkspaceRefresher

        refresher = AgentWorkspaceRefresher(
            self._make_mock_wpc(),
            db_url="sqlite+aiosqlite:///:memory:",
            workspace_file_path=None,
        )
        refresher.start()
        try:
            # Should not raise
            pass
        finally:
            refresher.stop()

    def test_stop_is_idempotent(self):
        """Calling stop multiple times doesn't raise."""
        from dagster_webserver.api.agent_workspace import AgentWorkspaceRefresher

        refresher = AgentWorkspaceRefresher(
            self._make_mock_wpc(),
            db_url="sqlite+aiosqlite:///:memory:",
            workspace_file_path=None,
        )
        refresher.start()
        refresher.stop()
        refresher.stop()  # Second call should be safe
