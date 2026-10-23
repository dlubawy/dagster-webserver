"""Async database access helpers for agent tables.

Provides common operations on the agent_heartbeats, agent_requests,
code_location_definitions, and code_location_snapshots tables.
All functions accept an AsyncSession and return ORM objects or primitives.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from dagster_webserver.database.models import (
    CODE_LOCATION_STATUSES,
    AgentHeartbeat,
    AgentRequest,
    CodeLocationDefinition,
    CodeLocationSnapshot,
)

# ── Workspace Entries (Code Location Definitions) ─────────────────────


async def get_workspace_entries(
    session: AsyncSession,
    agent_labels: dict[str, str] | None = None,
    location_names: list[str] | None = None,
) -> list[CodeLocationDefinition]:
    """Return active code locations, optionally filtered by labels and names."""
    stmt = select(CodeLocationDefinition).where(
        CodeLocationDefinition.status == "active"
    )

    if location_names:
        stmt = stmt.where(CodeLocationDefinition.location_name.in_(location_names))

    result = await session.execute(stmt)
    locations = list(result.scalars().all())

    # Label filtering is done in Python since JSON comparison varies by dialect
    if agent_labels:
        locations = [
            loc for loc in locations if _labels_match(loc.labels, agent_labels)
        ]

    return locations


def _labels_match(
    entry_labels: dict[str, str] | None, required_labels: dict[str, str]
) -> bool:
    """Check that all required labels are present in the entry's labels."""
    if not entry_labels:
        # No labels on entry means it matches any agent (global)
        return True
    return all(entry_labels.get(k) == v for k, v in required_labels.items())


async def upsert_code_location(
    session: AsyncSession,
    location_name: str,
    deploy_metadata: dict[str, Any],
    labels: dict[str, str] | None = None,
    status: str = "active",
) -> CodeLocationDefinition:
    """Create or update a code location definition."""
    if status not in CODE_LOCATION_STATUSES:
        raise ValueError(
            f"Invalid status '{status}'. Must be one of: "
            + ", ".join(CODE_LOCATION_STATUSES)
        )
    stmt = select(CodeLocationDefinition).where(
        CodeLocationDefinition.location_name == location_name
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing:
        existing.deploy_metadata = deploy_metadata
        existing.labels = labels
        existing.status = status
        await session.commit()
        await session.refresh(existing)
        return existing
    else:
        new_loc = CodeLocationDefinition(
            location_name=location_name,
            deploy_metadata=deploy_metadata,
            labels=labels,
            status=status,
        )
        session.add(new_loc)
        await session.commit()
        await session.refresh(new_loc)
        return new_loc


async def list_code_locations(
    session: AsyncSession,
    status: str | None = None,
) -> list[CodeLocationDefinition]:
    """List all code location definitions, optionally filtered by status."""
    stmt = select(CodeLocationDefinition).order_by(CodeLocationDefinition.location_name)
    if status:
        stmt = stmt.where(CodeLocationDefinition.status == status)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def delete_code_location(session: AsyncSession, location_name: str) -> bool:
    """Hard-delete a code location definition.

    Agents reconcile against the workspace-entries API (which only returns
    ``status == 'active'`` rows), so removing the row causes them to tear
    down the location's server on their next sync. Lifecycle signalling
    (e.g. pause/resume) is handled via the ``status`` column, not deletion.
    """
    stmt = select(CodeLocationDefinition).where(
        CodeLocationDefinition.location_name == location_name
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing:
        await session.delete(existing)
        await session.commit()
        return True
    return False


# ── Agent Requests (Work Queue) ───────────────────────────────────────


async def enqueue_request(
    session: AsyncSession,
    request_api: str,
    request_body: str,
    location_name: str | None = None,
    labels: dict[str, str] | None = None,
    ttl_seconds: int = 300,
) -> AgentRequest:
    """Add a new request to the work queue."""
    request = AgentRequest(
        id=str(uuid.uuid4()),
        request_api=request_api,
        request_body=request_body,
        location_name=location_name,
        labels=labels,
        status="pending",
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds),
    )
    session.add(request)
    await session.commit()
    await session.refresh(request)
    return request


async def pop_requests(
    session: AsyncSession,
    agent_id: str,
    agent_labels: dict[str, str] | None = None,
    limit: int = 10,
) -> list[AgentRequest]:
    """Pop pending requests from the queue with optimistic locking.

    Marks requests as 'processing' and assigns them to the given agent.
    Only returns requests whose labels match the agent's labels.
    """
    now = datetime.now(timezone.utc)

    # Fetch candidates ordered by creation time (FIFO)
    stmt = (
        select(AgentRequest)
        .where(
            AgentRequest.status == "pending",
            (AgentRequest.expires_at.is_(None)) | (AgentRequest.expires_at > now),
        )
        .order_by(AgentRequest.created_at)
        .limit(limit * 3)  # Over-fetch to account for label filtering
    )
    result = await session.execute(stmt)
    candidates = list(result.scalars().all())

    # Filter by labels in Python (JSON comparison varies by dialect)
    matched: list[AgentRequest] = []
    for req in candidates:
        if _labels_match(req.labels, agent_labels or {}):
            matched.append(req)
        if len(matched) >= limit:
            break

    # Claim the matched requests
    for req in matched:
        req.status = "processing"
        req.assigned_to_agent = agent_id
        req.popped_at = now

    await session.commit()
    for req in matched:
        await session.refresh(req)

    return matched


async def complete_request(
    session: AsyncSession,
    request_id: str,
    agent_id: str | None = None,
    response_body: str | None = None,
) -> bool:
    """Mark a request as completed and store the response."""
    conditions = [AgentRequest.id == request_id]
    if agent_id:
        conditions.append(AgentRequest.assigned_to_agent == agent_id)

    stmt = (
        update(AgentRequest)
        .where(*conditions)
        .values(status="completed", response_body=response_body)
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount > 0


async def fail_request(
    session: AsyncSession,
    request_id: str,
    agent_id: str | None = None,
    error: str | None = None,
) -> bool:
    """Mark a request as failed with an error message."""
    conditions = [AgentRequest.id == request_id]
    if agent_id:
        conditions.append(AgentRequest.assigned_to_agent == agent_id)

    stmt = (
        update(AgentRequest)
        .where(*conditions)
        .values(status="failed", response_body=error)
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount > 0


async def get_request_status(
    session: AsyncSession, request_id: str
) -> AgentRequest | None:
    """Get the current status of a request (for polling)."""
    stmt = select(AgentRequest).where(AgentRequest.id == request_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def requeue_stale_requests(session: AsyncSession) -> int:
    """Re-queue requests that have expired while in 'processing' status.

    Returns the number of requests re-queued.
    """
    # Use naive UTC to match SQLite's storage (no tz info)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    stmt = (
        update(AgentRequest)
        .where(
            AgentRequest.status == "processing",
            AgentRequest.expires_at.isnot(None),
            AgentRequest.expires_at < now,
        )
        .values(status="pending", assigned_to_agent=None, popped_at=None)
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount


# ── Agent Heartbeats ──────────────────────────────────────────────────


async def submit_heartbeat(
    session: AsyncSession,
    agent_id: str,
    agent_labels: dict[str, str] | None = None,
    agent_type: str | None = None,
    metadata: dict[str, Any] | None = None,
    errors: list[str] | None = None,
    code_server_heartbeats: dict[str, Any] | None = None,
) -> AgentHeartbeat:
    """Record an agent heartbeat."""
    heartbeat = AgentHeartbeat(
        agent_id=agent_id,
        agent_labels=agent_labels,
        agent_type=agent_type,
        metadata_=metadata,
        errors=errors,
        code_server_heartbeats=code_server_heartbeats,
    )
    session.add(heartbeat)
    await session.commit()
    await session.refresh(heartbeat)
    return heartbeat


async def get_latest_heartbeat(
    session: AsyncSession, agent_id: str
) -> AgentHeartbeat | None:
    """Get the most recent heartbeat for an agent."""
    stmt = (
        select(AgentHeartbeat)
        .where(AgentHeartbeat.agent_id == agent_id)
        .order_by(AgentHeartbeat.timestamp.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


# ── Code Location Snapshots ───────────────────────────────────────────


async def store_snapshot(
    session: AsyncSession,
    location_name: str,
    snapshot_type: str,
    sha1_hash: str,
    size_bytes: int,
    data: bytes | None = None,
) -> CodeLocationSnapshot:
    """Store a snapshot (upsert by unique constraint)."""
    stmt = select(CodeLocationSnapshot).where(
        CodeLocationSnapshot.location_name == location_name,
        CodeLocationSnapshot.snapshot_type == snapshot_type,
        CodeLocationSnapshot.sha1_hash == sha1_hash,
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing:
        return existing  # Already stored, no update needed

    snapshot = CodeLocationSnapshot(
        location_name=location_name,
        snapshot_type=snapshot_type,
        sha1_hash=sha1_hash,
        size_bytes=size_bytes,
        data=data,
    )
    session.add(snapshot)
    await session.commit()
    await session.refresh(snapshot)
    return snapshot


async def check_snapshot_exists(
    session: AsyncSession,
    location_name: str,
    snapshot_type: str,
    sha1_hash: str,
) -> CodeLocationSnapshot | None:
    """Check if a snapshot already exists (by content hash)."""
    stmt = select(CodeLocationSnapshot).where(
        CodeLocationSnapshot.location_name == location_name,
        CodeLocationSnapshot.snapshot_type == snapshot_type,
        CodeLocationSnapshot.sha1_hash == sha1_hash,
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


# ── Heartbeat Cleanup ────────────────────────────────────────────────


async def delete_old_heartbeats(
    session: AsyncSession,
    retention_hours: int = 24,
) -> int:
    """Delete agent heartbeats older than the retention window.

    Prevents unbounded growth of the agent_heartbeats table. Agents send
    heartbeats every ~30s, so a single agent generates ~2880 rows/day.

    Args:
        session: Active async database session.
        retention_hours: How long to keep heartbeat rows (default 24h).

    Returns:
        Number of rows deleted.
    """
    from sqlalchemy import delete

    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
        hours=retention_hours
    )
    stmt = delete(AgentHeartbeat).where(AgentHeartbeat.timestamp < cutoff)
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount or 0
