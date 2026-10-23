"""Dynamic workspace loading from agent-managed code locations.

When the webserver is started with ``--agent-workspaces``, it uses a
``DynamicAgentWorkspaceTarget`` that queries the database for gRPC endpoints
reported by agents via heartbeats. The target is re-queried on each
workspace reload, so new/removed agent locations are picked up automatically.

A background thread periodically triggers ``reload_workspace()`` to keep
the workspace in sync with agent state.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any

from dagster._core.workspace.load_target import (
    WorkspaceLoadTarget,
)

logger = logging.getLogger(__name__)


# Heartbeats older than this are considered stale and ignored.
# Should be at least 2x the agent's heartbeat_interval_seconds.
DEFAULT_HEARTBEAT_STALENESS_SECONDS = 120

# How often to reload the workspace from agent data (seconds).
DEFAULT_AGENT_WORKSPACE_REFRESH_INTERVAL = 30


def _run_async_in_thread(coro):
    """Run an async coroutine in a new thread with its own event loop.

    This avoids the 'cannot nest asyncio.run() inside a running event loop'
    problem that occurs when create_origins() is called from within
    uvicorn's async context at webserver startup.
    """
    import asyncio

    result: dict[str, tuple[str, int]] = {}
    exception: Exception | None = None

    def _runner():
        nonlocal result, exception
        try:
            result = asyncio.run(coro)
        except Exception as e:
            exception = e

    t = threading.Thread(target=_runner, name="agent-workspace-db-query", daemon=True)
    t.start()
    t.join(timeout=10)

    if exception is not None:
        logger.warning("Agent workspace DB query failed: %s", exception)
        return {}

    if t.is_alive():
        logger.warning("Agent workspace DB query timed out")
        return {}

    return result


class DynamicAgentWorkspaceTarget(WorkspaceLoadTarget):
    """A WorkspaceLoadTarget that dynamically queries the DB for agent locations.

    Each call to ``create_origins()`` queries the database for the latest
    non-stale heartbeats and returns GrpcServerTarget entries for all
    running locations. This means workspace reloads automatically pick up
    new/removed agent-managed locations.
    """

    def __init__(
        self,
        db_url: str,
        staleness_seconds: float = DEFAULT_HEARTBEAT_STALENESS_SECONDS,
    ):
        self._db_url = db_url
        self._staleness_seconds = staleness_seconds

    def create_origins(self):
        """Query the DB for running agent locations with fresh heartbeats."""
        from dagster._core.remote_origin import GrpcServerCodeLocationOrigin

        endpoints = _run_async_in_thread(self._query_agent_endpoints())

        origins = []
        for location_name, (host, port) in sorted(endpoints.items()):
            origin = GrpcServerCodeLocationOrigin(
                host=host,
                port=port,
                socket=None,
                location_name=location_name,
            )
            origins.append(origin)
            logger.debug(
                "Agent workspace: location %s -> %s:%d", location_name, host, port
            )

        if origins:
            logger.debug(
                "DynamicAgentWorkspaceTarget: found %d running locations", len(origins)
            )
        return origins

    async def _query_agent_endpoints(self) -> dict[str, tuple[str, int]]:
        """Query DB for non-stale agent heartbeats and extract running endpoints.

        Returns a dict mapping location_name -> (host, port).
        Only considers heartbeats within the staleness window.
        """
        from datetime import timedelta

        from sqlalchemy import select
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

        from dagster_webserver.database.models import AgentHeartbeat

        # Compute the cutoff: only consider heartbeats newer than this
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC for SQLite
        cutoff = cutoff - timedelta(seconds=self._staleness_seconds)

        engine = create_async_engine(self._db_url)
        try:
            async with AsyncSession(engine) as session:
                stmt = (
                    select(AgentHeartbeat)
                    .where(
                        AgentHeartbeat.timestamp >= cutoff,
                        AgentHeartbeat.code_server_heartbeats.isnot(None),
                    )
                    .order_by(AgentHeartbeat.timestamp.desc())
                )
                result = await session.execute(stmt)
                heartbeats = result.scalars().all()

                # Build endpoint map: most recent heartbeat wins for each location
                endpoints: dict[str, tuple[str, int]] = {}

                # Iterate in reverse (oldest first) so newer entries overwrite
                for hb in reversed(heartbeats):
                    code_servers: dict[str, Any] = hb.code_server_heartbeats or {}
                    for location_name, info in code_servers.items():
                        if not isinstance(info, dict):
                            continue
                        if info.get("status") != "running":
                            # Location is no longer running — remove if we had it
                            endpoints.pop(location_name, None)
                            continue
                        endpoint_str = info.get("endpoint", "")
                        if not endpoint_str:
                            continue
                        try:
                            host, port_str = endpoint_str.rsplit(":", 1)
                            port = int(port_str)
                        except ValueError:
                            logger.warning(
                                "Invalid endpoint for location %s: %s",
                                location_name,
                                endpoint_str,
                            )
                            continue
                        endpoints[location_name] = (host, port)

                return endpoints
        finally:
            await engine.dispose()


class AgentWorkspaceRefresher:
    """Background thread that periodically reloads the workspace from agent data.

    Started when ``--agent-workspaces`` is active. Calls
    ``workspace_process_context.reload_workspace()`` at a fixed interval
    so new/removed agent locations are picked up without a webserver restart.

    Optionally writes a workspace YAML file (``workspace_file_path``) that
    external processes like ``dagster-daemon`` can use via ``--workspace``
    to resolve the same code locations.
    """

    def __init__(
        self,
        workspace_process_context: Any,
        db_url: str,
        interval_seconds: float = DEFAULT_AGENT_WORKSPACE_REFRESH_INTERVAL,
        workspace_file_path: str | None = None,
    ):
        self._workspace_process_context = workspace_process_context
        self._db_url = db_url
        self._interval = interval_seconds
        self._workspace_file_path = workspace_file_path
        self._shutdown_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the background refresh thread."""
        self._shutdown_event.clear()
        # Seed the file immediately so daemon can start right away.
        # Even if no agents are reporting yet, the file must exist with
        # valid content (load_from: []) for dagster-daemon to parse it.
        if self._workspace_file_path:
            self._write_workspace_file()
        self._thread = threading.Thread(
            target=self._run,
            name="agent-workspace-refresher",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Agent workspace refresher started (interval=%.0fs, file=%s)",
            self._interval,
            self._workspace_file_path or "none",
        )

    def stop(self) -> None:
        """Stop the background refresh thread."""
        self._shutdown_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("Agent workspace refresher stopped.")

    def _run(self) -> None:
        """Loop: sleep, then reload workspace and update file."""
        while not self._shutdown_event.wait(self._interval):
            try:
                before = set(self._workspace_process_context.code_location_names)
                self._workspace_process_context.reload_workspace()
                after = set(self._workspace_process_context.code_location_names)

                added = after - before
                removed = before - after
                if added or removed:
                    logger.info(
                        "Agent workspace refreshed: +%s -%s (total: %d)",
                        sorted(added) or "nothing",
                        sorted(removed) or "nothing",
                        len(after),
                    )

                # Update the workspace file for external consumers (daemon)
                if self._workspace_file_path:
                    self._write_workspace_file()
            except Exception:
                logger.exception("Error during agent workspace refresh")

    def _write_workspace_file(self) -> None:
        """Write a workspace YAML file with current gRPC server entries.

        The file format is compatible with ``dagster-daemon run --workspace``.
        Uses atomic write (write to temp, then rename) to prevent the daemon
        from reading a partially-written file.
        """
        import os
        import tempfile

        import yaml

        # Query DB directly for current endpoints (same logic as DynamicAgentWorkspaceTarget)
        target = DynamicAgentWorkspaceTarget(
            db_url=self._db_url,
            staleness_seconds=DEFAULT_HEARTBEAT_STALENESS_SECONDS,
        )
        try:
            endpoints = _run_async_in_thread(target._query_agent_endpoints())
        except Exception:
            logger.warning(
                "Failed to query endpoints for workspace file", exc_info=True
            )
            endpoints = {}

        load_from = []
        for location_name, (host, port) in sorted(endpoints.items()):
            load_from.append(
                {
                    "grpc_server": {
                        "location_name": location_name,
                        "host": host,
                        "port": port,
                    }
                }
            )

        # Always write valid YAML — even empty must have load_from: []
        workspace_config = {"load_from": load_from}

        file_path = self._workspace_file_path  # type: ignore[arg-type]
        dir_path = os.path.dirname(file_path) or "."
        os.makedirs(dir_path, exist_ok=True)

        # Atomic write: write to temp file in same directory, then rename
        fd, tmp_path = tempfile.mkstemp(
            dir=dir_path, prefix=".agent_workspace_", suffix=".yaml"
        )
        try:
            with os.fdopen(fd, "w") as f:
                yaml.dump(workspace_config, f, default_flow_style=False)
            os.replace(tmp_path, file_path)
            logger.debug(
                "Wrote agent workspace file: %s (%d locations)",
                file_path,
                len(load_from),
            )
        except Exception:
            # Clean up temp file on failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            logger.exception("Failed to write agent workspace file: %s", file_path)
