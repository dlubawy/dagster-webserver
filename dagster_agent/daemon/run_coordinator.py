"""Run enqueuer — demonstrates queue-based run launching through the agent.

This module shows how a daemon (or any component) can enqueue run launches
through the agent queue instead of launching them directly. In the full
daemon fork, this replaces the direct gRPC call in `QueuedRunCoordinatorDaemon`.

The pattern:
1. Daemon calls `AgentRunEnqueuer.enqueue_run()`
2. A LAUNCH_RUN request is placed in the agent_requests queue
3. An agent pops the request and launches the run via its code location server
4. The agent uploads a response with the run ID
5. The daemon (optionally) polls for the response to confirm launch
"""

from __future__ import annotations

import logging
from typing import Any

from dagster_agent.daemon.client import DaemonAgentClient

logger = logging.getLogger("dagster-agent.daemon.run_coordinator")


class AgentRunEnqueuer:
    """Enqueues run launches and terminations through the agent queue.

    This is a proof-of-concept module that demonstrates the queue-based
    pattern. A full daemon fork would integrate this into its coordination
    loop, replacing direct gRPC calls with queue submissions.
    """

    def __init__(self, client: DaemonAgentClient):
        self._client = client

    async def enqueue_run(
        self,
        run_key: str,
        location_name: str,
        repository_origin: dict[str, Any],
        pipeline_name: str | None = None,
        job_name: str | None = None,
        partition_name: str | None = None,
        tags: dict[str, str] | None = None,
        run_config: dict[str, Any] | None = None,
        solid_selection: list[str] | None = None,
        wait_for_response: bool = True,
        timeout: float = 60.0,
    ) -> dict[str, Any]:
        """Enqueue a run launch request.

        Args:
            run_key: Unique key for this run (idempotency).
            location_name: Target code location.
            repository_origin: Origin info for the repository (selector fields).
            pipeline_name: Pipeline name (for asset-based definitions).
            job_name: Job name (if different from pipeline).
            partition_name: Partition to launch (optional).
            tags: Run tags.
            run_config: Run configuration overrides.
            solid_selection: Subset of ops to execute.
            wait_for_response: If True, poll until the agent confirms launch.
            timeout: Max seconds to wait for response.

        Returns:
            Dict with at least {"request_id": ...}. If wait_for_response is
            True and succeeds, also includes {"run_id": ...} from the agent.
        """
        request_body: dict[str, Any] = {
            "run_key": run_key,
            "location_name": location_name,
            "repository_origin": repository_origin,
        }
        if pipeline_name:
            request_body["pipeline_name"] = pipeline_name
        if job_name:
            request_body["job_name"] = job_name
        if partition_name:
            request_body["partition_name"] = partition_name
        if tags:
            request_body["tags"] = tags
        if run_config:
            request_body["run_config"] = run_config
        if solid_selection:
            request_body["solid_selection"] = solid_selection

        request_id = await self._client.enqueue_request(
            request_api="LAUNCH_RUN",
            request_body=request_body,
            location_name=location_name,
        )

        logger.info(
            "Enqueued LAUNCH_RUN for run_key=%s location=%s (request_id=%s)",
            run_key,
            location_name,
            request_id,
        )

        if wait_for_response:
            response = await self._client.wait_for_response(request_id, timeout=timeout)
            return {"request_id": request_id, **response}

        return {"request_id": request_id}

    async def enqueue_terminate_run(
        self,
        run_id: str,
        location_name: str,
        user_id: str | None = None,
        wait_for_response: bool = True,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Enqueue a run termination request.

        Args:
            run_id: The ID of the run to terminate.
            location_name: Code location where the run is executing.
            user_id: User requesting termination (for audit).
            wait_for_response: If True, poll until confirmed.
            timeout: Max seconds to wait.

        Returns:
            Dict with request_id and (optionally) termination confirmation.
        """
        request_body: dict[str, Any] = {
            "run_id": run_id,
            "location_name": location_name,
        }
        if user_id:
            request_body["user_id"] = user_id

        request_id = await self._client.enqueue_request(
            request_api="TERMINATE_RUN",
            request_body=request_body,
            location_name=location_name,
        )

        logger.info(
            "Enqueued TERMINATE_RUN for run_id=%s (request_id=%s)",
            run_id,
            request_id,
        )

        if wait_for_response:
            response = await self._client.wait_for_response(request_id, timeout=timeout)
            return {"request_id": request_id, **response}

        return {"request_id": request_id}

    async def ping_location(
        self, location_name: str, timeout: float = 10.0
    ) -> dict[str, Any]:
        """Check if a code location's server is alive via the agent.

        Returns:
            Dict with {"status": "alive"|"unavailable", ...}
        """
        request_id = await self._client.enqueue_request(
            request_api="PING_LOCATION",
            request_body={"location_name": location_name},
            location_name=location_name,
        )

        return await self._client.wait_for_response(request_id, timeout=timeout)
