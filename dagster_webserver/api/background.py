"""Background tasks for the agent system.

Provides periodic tasks that run as part of the webserver's lifespan:
- Re-queue expired 'processing' requests (crashed agent recovery)
- Periodic cleanup of old heartbeat rows (prevent unbounded DB growth)
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger("dagster-webserver.background.agent")

# How often to check for stale requests (seconds)
STALE_REQUEST_CHECK_INTERVAL = 30.0

# How often to clean up old heartbeats (seconds) — once per hour
HEARTBEAT_CLEANUP_INTERVAL = 3600.0

# How long to keep heartbeat rows before deleting (hours)
HEARTBEAT_RETENTION_HOURS = 24


async def stale_request_requeue_loop(session_factory) -> None:
    """Periodically re-queue expired processing requests.

    Runs as a background task in the Starlette lifespan. Finds requests
    that are stuck in 'processing' status past their TTL and resets them
    to 'pending' so another agent can pick them up.

    Args:
        session_factory: AsyncSession factory from the database engine.
    """
    logger.info(
        "Stale request requeue loop started (interval=%.0fs)",
        STALE_REQUEST_CHECK_INTERVAL,
    )

    while True:
        await asyncio.sleep(STALE_REQUEST_CHECK_INTERVAL)
        try:
            async with session_factory() as session:
                from dagster_webserver.database.agent_queries import (
                    requeue_stale_requests,
                )

                count = await requeue_stale_requests(session)
                if count > 0:
                    logger.warning("Re-queued %d stale agent request(s)", count)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Error in stale request requeue loop")


async def start_stale_request_requeue(session_factory) -> asyncio.Task:
    """Start the stale request requeue background task.

    Returns the asyncio.Task so it can be cancelled during shutdown.
    """
    return asyncio.create_task(
        stale_request_requeue_loop(session_factory),
        name="agent-stale-request-requeue",
    )


async def heartbeat_cleanup_loop(session_factory) -> None:
    """Periodically delete old heartbeat rows to prevent unbounded DB growth.

    Runs as a background task in the Starlette lifespan. Deletes heartbeats
    older than HEARTBEAT_RETENTION_HOURS (default 24h).
    """
    logger.info(
        "Heartbeat cleanup loop started (interval=%.0fs, retention=%dh)",
        HEARTBEAT_CLEANUP_INTERVAL,
        HEARTBEAT_RETENTION_HOURS,
    )

    while True:
        await asyncio.sleep(HEARTBEAT_CLEANUP_INTERVAL)
        try:
            async with session_factory() as session:
                from dagster_webserver.database.agent_queries import (
                    delete_old_heartbeats,
                )

                count = await delete_old_heartbeats(
                    session, retention_hours=HEARTBEAT_RETENTION_HOURS
                )
                if count > 0:
                    logger.info("Cleaned up %d old heartbeat row(s)", count)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Error in heartbeat cleanup loop")


async def start_heartbeat_cleanup(session_factory) -> asyncio.Task:
    """Start the heartbeat cleanup background task."""
    return asyncio.create_task(
        heartbeat_cleanup_loop(session_factory),
        name="agent-heartbeat-cleanup",
    )


@asynccontextmanager
async def build_agent_lifespan(session_factory):
    """Build a Starlette lifespan context manager for agent background tasks.

    On startup:
    - Load workspace.yaml locations into the database (if present)
    - Start the stale request requeue loop
    - Start the heartbeat cleanup loop

    On shutdown:
    - Cancel all background tasks
    """
    # Load workspace.yaml on startup (non-fatal if it fails)
    try:
        from dagster_webserver.api.workspace_loader import (
            upsert_workspace_yaml_locations,
        )

        count = await upsert_workspace_yaml_locations(session_factory)
        if count > 0:
            logger.info("Loaded %d location(s) from workspace.yaml", count)
    except Exception:
        logger.exception("Failed to load workspace.yaml (non-fatal)")

    requeue_task = await start_stale_request_requeue(session_factory)
    cleanup_task = await start_heartbeat_cleanup(session_factory)
    try:
        yield
    finally:
        for task in (requeue_task, cleanup_task):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
