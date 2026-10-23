"""End-to-end integration tests for the agent request queue.

Tests the full lifecycle: enqueue → pop → process → complete/fail → poll status,
plus stale request re-queuing. Uses an in-memory SQLite database.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dagster_webserver.database.models import (
    AgentHeartbeat,
    AgentRequest,
    Base,
)


@pytest.fixture
def engine():
    """Create an in-memory SQLite engine with all tables."""
    return create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)


@pytest_asyncio.fixture
async def session_factory(engine):
    """Create a session factory bound to the test engine."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest_asyncio.fixture
async def session(session_factory):
    """Provide a single session for simple tests."""
    async with session_factory() as s:
        yield s


class TestRequestQueueLifecycle:
    """Test the full request queue lifecycle."""

    @pytest.mark.asyncio
    async def test_enqueue_and_pop(self, session_factory):
        from dagster_webserver.database.agent_queries import (
            enqueue_request,
            pop_requests,
        )

        async with session_factory() as session:
            # Enqueue a request
            req = await enqueue_request(
                session,
                request_api="LAUNCH_RUN",
                request_body='{"run_key": "test-run-1"}',
                location_name="my-location",
                labels=None,
                ttl_seconds=300,
            )
            assert req.id is not None
            assert req.status == "pending"

            # Pop it
            popped = await pop_requests(session, agent_id="agent-1", limit=10)
            assert len(popped) == 1
            assert popped[0].id == req.id
            assert popped[0].status == "processing"
            assert popped[0].assigned_to_agent == "agent-1"

    @pytest.mark.asyncio
    async def test_complete_request(self, session_factory):
        from dagster_webserver.database.agent_queries import (
            complete_request,
            enqueue_request,
            get_request_status,
            pop_requests,
        )

        async with session_factory() as session:
            req = await enqueue_request(
                session,
                request_api="PING_LOCATION",
                request_body='{"location_name": "loc-a"}',
                location_name="loc-a",
            )

            popped = await pop_requests(session, agent_id="agent-1")
            assert len(popped) == 1

            # Complete it
            success = await complete_request(
                session,
                request_id=req.id,
                agent_id="agent-1",
                response_body='{"status": "alive"}',
            )
            assert success

            # Verify status
            final = await get_request_status(session, req.id)
            assert final.status == "completed"
            assert final.response_body == '{"status": "alive"}'

    @pytest.mark.asyncio
    async def test_fail_request(self, session_factory):
        from dagster_webserver.database.agent_queries import (
            enqueue_request,
            fail_request,
            get_request_status,
            pop_requests,
        )

        async with session_factory() as session:
            req = await enqueue_request(
                session,
                request_api="TERMINATE_RUN",
                request_body='{"run_id": "r-123"}',
                location_name="loc-b",
            )

            await pop_requests(session, agent_id="agent-1")

            # Fail it
            success = await fail_request(
                session,
                request_id=req.id,
                agent_id="agent-1",
                error="Run not found",
            )
            assert success

            final = await get_request_status(session, req.id)
            assert final.status == "failed"
            assert final.response_body == "Run not found"

    @pytest.mark.asyncio
    async def test_complete_with_wrong_agent_fails(self, session_factory):
        """Only the assigned agent can complete a request."""
        from dagster_webserver.database.agent_queries import (
            complete_request,
            enqueue_request,
            pop_requests,
        )

        async with session_factory() as session:
            req = await enqueue_request(
                session,
                request_api="PING_LOCATION",
                request_body="{}",
            )

            await pop_requests(session, agent_id="agent-1")

            # Try to complete with a different agent — should fail
            success = await complete_request(
                session,
                request_id=req.id,
                agent_id="agent-2",  # Wrong agent!
                response_body="{}",
            )
            assert not success

    @pytest.mark.asyncio
    async def test_multiple_requests_fifo(self, session_factory):
        """Requests are popped in FIFO order."""
        from dagster_webserver.database.agent_queries import (
            enqueue_request,
            pop_requests,
        )

        async with session_factory() as session:
            req1 = await enqueue_request(
                session, request_api="PING_LOCATION", request_body='{"n": 1}'
            )
            req2 = await enqueue_request(
                session, request_api="PING_LOCATION", request_body='{"n": 2}'
            )
            req3 = await enqueue_request(
                session, request_api="PING_LOCATION", request_body='{"n": 3}'
            )

            popped = await pop_requests(session, agent_id="agent-1", limit=10)
            assert [r.id for r in popped] == [req1.id, req2.id, req3.id]

    @pytest.mark.asyncio
    async def test_pop_limit(self, session_factory):
        """Respect the limit parameter on pop."""
        from dagster_webserver.database.agent_queries import (
            enqueue_request,
            pop_requests,
        )

        async with session_factory() as session:
            for i in range(5):
                await enqueue_request(
                    session, request_api="PING_LOCATION", request_body=f'{{"n": {i}}}'
                )

            popped = await pop_requests(session, agent_id="agent-1", limit=3)
            assert len(popped) == 3

            # Remaining 2 can be popped next time
            remaining = await pop_requests(session, agent_id="agent-1", limit=10)
            assert len(remaining) == 2

    @pytest.mark.asyncio
    async def test_stale_request_requeue(self, session_factory):
        """Expired processing requests are re-queued to pending."""
        from dagster_webserver.database.agent_queries import (
            enqueue_request,
            pop_requests,
            requeue_stale_requests,
        )

        async with session_factory() as session:
            req = await enqueue_request(
                session,
                request_api="LAUNCH_RUN",
                request_body="{}",
                ttl_seconds=1,  # Very short TTL
            )

            await pop_requests(session, agent_id="agent-1")

            # Manually expire it (simulate time passing)
            from sqlalchemy import update

            past = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
                seconds=2
            )
            await session.execute(
                update(AgentRequest)
                .where(AgentRequest.id == req.id)
                .values(expires_at=past)
            )
            await session.commit()

            # Requeue stale requests
            count = await requeue_stale_requests(session)
            assert count == 1

            # Verify it's back to pending
            from dagster_webserver.database.agent_queries import get_request_status

            status = await get_request_status(session, req.id)
            assert status.status == "pending"
            assert status.assigned_to_agent is None

    @pytest.mark.asyncio
    async def test_non_expired_not_requeued(self, session_factory):
        """Non-expired processing requests are NOT re-queued."""
        from dagster_webserver.database.agent_queries import (
            enqueue_request,
            pop_requests,
            requeue_stale_requests,
        )

        async with session_factory() as session:
            await enqueue_request(
                session,
                request_api="LAUNCH_RUN",
                request_body="{}",
                ttl_seconds=3600,  # Long TTL — won't expire
            )

            await pop_requests(session, agent_id="agent-1")

            count = await requeue_stale_requests(session)
            assert count == 0

    @pytest.mark.asyncio
    async def test_label_routing(self, session_factory):
        """Requests with labels can be filtered by agents with matching labels."""
        from dagster_webserver.database.agent_queries import (
            enqueue_request,
            pop_requests,
        )

        async with session_factory() as session:
            # Request for a specific label
            await enqueue_request(
                session,
                request_api="PING_LOCATION",
                request_body="{}",
                labels={"region": "us-east"},
            )
            # Request with no label (global)
            await enqueue_request(
                session,
                request_api="PING_LOCATION",
                request_body="{}",
                labels=None,
            )

            # Agent with matching label gets both
            popped = await pop_requests(
                session, agent_id="agent-east", agent_labels={"region": "us-east"}
            )
            assert len(popped) == 2


class TestHeartbeatCleanup:
    @pytest.mark.asyncio
    async def test_delete_old_heartbeats(self, session_factory):
        """Old heartbeats are deleted, recent ones are kept."""
        from dagster_webserver.database.agent_queries import delete_old_heartbeats

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        old_time = now - timedelta(hours=48)  # 2 days ago (beyond 24h retention)
        recent_time = now - timedelta(hours=1)  # 1 hour ago (within retention)

        async with session_factory() as session:
            # Insert old and recent heartbeats
            session.add(
                AgentHeartbeat(
                    agent_id="agent-old",
                    timestamp=old_time,
                    code_server_heartbeats=None,
                )
            )
            session.add(
                AgentHeartbeat(
                    agent_id="agent-recent",
                    timestamp=recent_time,
                    code_server_heartbeats={
                        "loc": {"status": "running", "endpoint": "127.0.0.1:5000"}
                    },
                )
            )
            await session.commit()

        # Delete with 24h retention
        async with session_factory() as session:
            deleted = await delete_old_heartbeats(session, retention_hours=24)
            assert deleted == 1

        # Verify only recent heartbeat remains
        from sqlalchemy import select

        async with session_factory() as session:
            result = await session.execute(select(AgentHeartbeat))
            remaining = result.scalars().all()
            assert len(remaining) == 1
            assert remaining[0].agent_id == "agent-recent"

    @pytest.mark.asyncio
    async def test_delete_old_heartbeats_empty_table(self, session_factory):
        """Cleanup on empty table returns 0."""
        from dagster_webserver.database.agent_queries import delete_old_heartbeats

        async with session_factory() as session:
            deleted = await delete_old_heartbeats(session, retention_hours=24)
            assert deleted == 0
