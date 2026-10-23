"""Client for the daemon to enqueue work through the agent queue.

The daemon uses this client to submit requests (e.g., launch runs) that
will be picked up and processed by an agent. Supports the request-reply
pattern where the daemon can poll for a response.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger("dagster-agent.daemon.client")


class DaemonAgentClient:
    """Client for the daemon to enqueue work through the agent queue.

    Usage::

        async with DaemonAgentClient("http://localhost:3005", "api-key") as client:
            request_id = await client.enqueue_request(
                request_api="LAUNCH_RUN",
                request_body={"run_key": "my-run"},
                location_name="my-location",
            )
            response = await client.wait_for_response(request_id, timeout=60)
    """

    def __init__(self, webserver_url: str, api_key: str, timeout: float = 30.0):
        self._base_url = webserver_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        )

    async def __aenter__(self) -> DaemonAgentClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def enqueue_request(
        self,
        request_api: str,
        request_body: dict[str, Any] | str,
        location_name: str | None = None,
        labels: dict[str, str] | None = None,
        ttl_seconds: int = 300,
    ) -> str:
        """Enqueue a work request and return its ID.

        Args:
            request_api: The type of work (e.g., "LAUNCH_RUN", "TERMINATE_RUN").
            request_body: The request payload (dict or pre-serialized JSON string).
            location_name: Target code location (optional for global requests).
            labels: Agent labels to route the request to specific agents.
            ttl_seconds: How long the request stays pending before expiring.

        Returns:
            The request ID (UUID string) for tracking/polling.
        """
        body: dict[str, Any] = {
            "request_api": request_api,
            "request_body": request_body,
            "ttl_seconds": ttl_seconds,
        }
        if location_name:
            body["location_name"] = location_name
        if labels:
            body["labels"] = labels

        resp = await self._client.post("/api/agent/enqueue-request", json=body)
        resp.raise_for_status()
        return resp.json()["request_id"]

    async def get_request_status(self, request_id: str) -> dict[str, Any]:
        """Poll for a request's current status.

        Returns:
            Dict with keys: request_id, status (pending/processing/completed/failed),
            response_body (when completed).
        """
        resp = await self._client.get(f"/api/agent/request-status/{request_id}")
        resp.raise_for_status()
        return resp.json()

    async def wait_for_response(
        self,
        request_id: str,
        timeout: float = 60.0,
        poll_interval: float = 1.0,
    ) -> dict[str, Any]:
        """Poll until the request is completed or failed.

        Args:
            request_id: The request ID to wait for.
            timeout: Maximum seconds to wait before raising TimeoutError.
            poll_interval: Seconds between status polls.

        Returns:
            The response body dict when the request completes.

        Raises:
            TimeoutError: If the request doesn't complete within timeout.
            RuntimeError: If the request fails.
        """
        elapsed = 0.0
        while elapsed < timeout:
            status = await self.get_request_status(request_id)
            state = status.get("status", "pending")

            if state == "completed":
                import json

                response_body = status.get("response_body")
                if isinstance(response_body, str):
                    try:
                        return json.loads(response_body)
                    except (json.JSONDecodeError, TypeError):
                        return {"raw": response_body}
                return response_body or {}

            if state == "failed":
                raise RuntimeError(
                    f"Request {request_id} failed: {status.get('response_body', 'unknown error')}"
                )

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(
            f"Request {request_id} did not complete within {timeout}s "
            f"(last status: {state})"
        )
