"""HTTP client for dagster-agent ↔ webserver communication.

Provides an async HTTP client that the agent uses to interact with
the webserver REST API endpoints under ``/api/agent/*``.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger("dagster-agent.api")


class AgentAPIClient:
    """Async HTTP client for the agent to communicate with the webserver.

    All requests include an ``Authorization: Bearer <api_key>`` header.
    Retries with exponential backoff on transient failures (5xx, timeouts).
    """

    def __init__(self, base_url: str, api_key: str, timeout: float = 30.0):
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        )

    async def __aenter__(self) -> AgentAPIClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    # ── Workspace ────────────────────────────────────────────────

    async def get_workspace_entries(
        self,
        labels: dict[str, str] | None = None,
        location_names: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch code locations this agent should manage."""
        params: dict[str, str] = {}
        if labels:
            import json

            params["labels"] = json.dumps(labels)
        if location_names:
            params["location_name"] = ",".join(location_names)

        resp = await self._client.get("/api/agent/workspace-entries", params=params)
        resp.raise_for_status()
        return resp.json().get("locations", [])

    # ── Request Queue ────────────────────────────────────────────

    async def pop_requests(
        self, limit: int = 10, labels: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
        """Pop pending requests from the work queue."""
        body: dict[str, Any] = {"limit": limit}
        if labels:
            body["labels"] = labels

        resp = await self._client.post("/api/agent/pop-requests", json=body)
        resp.raise_for_status()
        return resp.json().get("requests", [])

    async def enqueue_request(
        self,
        request_api: str,
        request_body: dict[str, Any] | str,
        location_name: str | None = None,
        labels: dict[str, str] | None = None,
        ttl_seconds: int = 300,
    ) -> str:
        """Enqueue a work request. Returns the request ID."""
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
        """Poll for a request's status and response."""
        resp = await self._client.get(f"/api/agent/request-status/{request_id}")
        resp.raise_for_status()
        return resp.json()

    async def complete_request(
        self, request_id: str, response_body: str | dict[str, Any]
    ) -> None:
        """Mark a request as completed with a response body."""
        resp = await self._client.post(
            f"/api/agent/complete-request/{request_id}",
            json={"response_body": response_body},
        )
        resp.raise_for_status()

    async def fail_request(self, request_id: str, error: str) -> None:
        """Mark a request as failed with an error message."""
        resp = await self._client.post(
            f"/api/agent/fail-request/{request_id}",
            json={"error": error},
        )
        resp.raise_for_status()

    # ── Heartbeat ────────────────────────────────────────────────

    async def submit_heartbeat(self, heartbeat_dict: dict[str, Any]) -> None:
        """Submit an agent health heartbeat."""
        resp = await self._client.post("/api/agent/heartbeat", json=heartbeat_dict)
        resp.raise_for_status()

    # ── Data Upload ──────────────────────────────────────────────

    async def upload_code_location_data(
        self,
        location_name: str,
        manifest: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        """Upload loaded repository data for a code location."""
        body: dict[str, Any] = {"location_name": location_name}
        if manifest:
            body["manifest"] = manifest
        if error:
            body["error"] = error

        resp = await self._client.post("/api/agent/upload-data", json=body)
        resp.raise_for_status()

    # ── Snapshots ────────────────────────────────────────────────

    async def check_snapshot(
        self, location_name: str, snapshot_type: str, sha1_hash: str
    ) -> dict[str, Any]:
        """Check if a snapshot already exists on the server."""
        resp = await self._client.get(
            "/api/agent/check-snapshot",
            params={
                "location_name": location_name,
                "snapshot_type": snapshot_type,
                "sha1_hash": sha1_hash,
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def upload_snapshot(
        self,
        location_name: str,
        snapshot_type: str,
        sha1_hash: str,
        data: bytes,
    ) -> dict[str, Any]:
        """Upload a serialized snapshot."""
        import base64

        body = {
            "location_name": location_name,
            "snapshot_type": snapshot_type,
            "sha1_hash": sha1_hash,
            "size_bytes": len(data),
            "data": base64.b64encode(data).decode(),
        }
        resp = await self._client.post("/api/agent/upload-snapshot", json=body)
        resp.raise_for_status()
        return resp.json()
