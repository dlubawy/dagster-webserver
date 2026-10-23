"""DagsterAgent — the main agent process loop.

The agent runs in a single-threaded async event loop with periodic tasks:
1. **Workspace sync** — fetch code locations from webserver, update launcher state
2. **Request polling** — pop work requests from the queue, dispatch to handlers
3. **Heartbeat** — report health and code server status to the webserver

The agent is started via ``dagster-agent run`` and runs until interrupted.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from typing import Any

from dagster_agent.api.client import AgentAPIClient
from dagster_agent.api.types import (
    AgentHeartbeat,
    AgentRequest,
    DagsterAgentApi,
)
from dagster_agent.instance import DagsterAgentInstance
from dagster_agent.observability import (
    AgentMetrics,
    SentinelWriter,
    create_sentinel_writer,
)
from dagster_agent.user_code_launcher.base import (
    DagsterUserCodeLauncher,
    UserCodeLauncherEntry,
)

logger = logging.getLogger("dagster-agent")


class DagsterAgent:
    """Main agent process that coordinates launcher and request processing.

    Usage::

        with DagsterAgent(instance) as agent:
            agent.run_loop(launcher)
    """

    def __init__(self, instance: DagsterAgentInstance):
        self._instance = instance
        self._client: AgentAPIClient | None = None
        self._shutdown_event: asyncio.Event | None = None
        self._running = False
        self._request_handlers: dict[DagsterAgentApi, Any] = {}
        self._sentinel: SentinelWriter | None = create_sentinel_writer(
            instance.sentinel_dir
        )
        self._metrics = AgentMetrics(
            log_interval_seconds=instance.metrics_log_interval_seconds
        )

    # ── Context manager ────────────────────────────────────────────

    def __enter__(self) -> DagsterAgent:
        """Sync context manager (used by CLI)."""
        self._client = AgentAPIClient(
            base_url=self._instance.webserver_url,
            api_key=self._instance.api_key,
        )
        self._shutdown_event = asyncio.Event()
        return self

    def __exit__(self, *args: Any) -> None:
        """Sync context manager exit — runs shutdown in a new event loop."""
        try:
            asyncio.run(self.shutdown())
        except RuntimeError:
            # Already inside an event loop (shouldn't happen from CLI,
            # but guard against it)
            self._client = None

    async def __aenter__(self) -> DagsterAgent:
        self._client = AgentAPIClient(
            base_url=self._instance.webserver_url,
            api_key=self._instance.api_key,
        )
        self._shutdown_event = asyncio.Event()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()

    # ── Main loop ──────────────────────────────────────────────────

    def run_loop(self, launcher: DagsterUserCodeLauncher[Any]) -> None:
        """Run the agent main loop (blocking)."""
        asyncio.run(self._async_main(launcher))

    async def _async_main(self, launcher: DagsterUserCodeLauncher[Any]) -> None:
        """Async main loop running all periodic tasks concurrently."""
        assert self._client is not None
        assert self._shutdown_event is not None

        self._running = True
        logger.info(
            "Agent starting (webserver=%s, labels=%s, sentinel_dir=%s)",
            self._instance.webserver_url,
            self._instance.labels or "{}",
            self._instance.sentinel_dir or "(none)",
        )

        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._shutdown_event.set)
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                pass

        # Run all periodic tasks concurrently
        await asyncio.gather(
            self._workspace_sync_loop(launcher),
            self._request_polling_loop(launcher),
            self._heartbeat_loop(launcher),
            self._sentinel_loop(launcher),
            self._shutdown_waiter(),
        )

    async def _sentinel_loop(self, launcher: DagsterUserCodeLauncher[Any]) -> None:
        """Periodically write sentinel files and log metrics."""
        assert self._shutdown_event is not None

        while not self._shutdown_event.is_set():
            if self._sentinel:
                self._sentinel.touch_liveness()
                ready = launcher.ready_to_serve_requests and bool(
                    launcher.running_locations
                )
                self._sentinel.touch_readiness(ready)

            # Periodic metrics logging
            self._metrics.maybe_log_metrics(
                running_locations=launcher.running_locations,
                ready=launcher.ready_to_serve_requests,
            )

            await asyncio.sleep(10)  # Check every 10s

    async def _shutdown_waiter(self) -> None:
        """Wait for shutdown signal."""
        assert self._shutdown_event is not None
        await self._shutdown_event.wait()
        logger.info("Shutdown signal received.")

    # ── Workspace sync ─────────────────────────────────────────────

    async def _workspace_sync_loop(
        self, launcher: DagsterUserCodeLauncher[Any]
    ) -> None:
        """Periodically fetch workspace entries and update the launcher."""
        assert self._client is not None
        assert self._shutdown_event is not None

        while not self._shutdown_event.is_set():
            try:
                await self._sync_workspace(launcher)
            except Exception as exc:
                logger.warning("Workspace sync failed: %s", exc)

            await asyncio.sleep(self._instance.poll_interval_seconds)

    async def _sync_workspace(self, launcher: DagsterUserCodeLauncher[Any]) -> None:
        """Fetch code locations from webserver and update desired state."""
        assert self._client is not None

        locations = await self._client.get_workspace_entries(
            labels=self._instance.labels or None,
            location_names=self._instance.allowed_locations,
        )

        # Build desired entries map. The webserver only returns locations
        # with status 'active', so paused or deleted locations drop out of
        # the desired state and the launcher tears their servers down.
        desired: dict[str, UserCodeLauncherEntry] = {}
        for loc in locations:
            name = loc["location_name"]
            deploy_metadata = loc.get("deploy_metadata") or {}
            entry = UserCodeLauncherEntry(
                location_name=name,
                deploy_metadata=deploy_metadata,
                labels=loc.get("labels"),
            )
            desired[name] = entry

        logger.debug("Workspace sync: %d locations desired", len(desired))
        launcher.update_grpc_metadata(desired)
        self._metrics.record_workspace_sync()

    # ── Request polling ────────────────────────────────────────────

    async def _request_polling_loop(
        self, launcher: DagsterUserCodeLauncher[Any]
    ) -> None:
        """Periodically poll for work requests and process them."""
        assert self._client is not None
        assert self._shutdown_event is not None

        while not self._shutdown_event.is_set():
            try:
                await self._poll_and_process_requests(launcher)
            except Exception as exc:
                logger.warning("Request polling failed: %s", exc)

            # Short sleep for request polling (more frequent than workspace sync)
            poll_interval = min(self._instance.poll_interval_seconds, 2.0)
            await asyncio.sleep(poll_interval)

    async def _poll_and_process_requests(
        self, launcher: DagsterUserCodeLauncher[Any]
    ) -> None:
        """Pop requests from queue and dispatch to handlers."""
        assert self._client is not None

        raw_requests = await self._client.pop_requests(
            limit=10,
            labels=self._instance.labels or None,
        )

        for raw in raw_requests:
            request = AgentRequest(
                request_id=raw["request_id"],
                request_api=raw["request_api"],
                request_body=raw["request_body"],
                location_name=raw.get("location_name"),
                labels=raw.get("labels"),
            )

            logger.debug(
                "Processing request %s (%s)",
                request.request_id,
                request.request_api.value,
            )

            try:
                response = await self._dispatch_request(request, launcher)
                # Mark request as completed with the response
                if response is not None:
                    await self._client.complete_request(request.request_id, response)
                self._metrics.record_request_processed()
            except Exception as exc:
                logger.exception(
                    "Error processing request %s: %s",
                    request.request_id,
                    exc,
                )
                self._metrics.record_request_failed(str(exc))
                # Mark request as failed
                try:
                    await self._client.fail_request(request.request_id, str(exc))
                except Exception:
                    logger.exception(
                        "Failed to mark request %s as failed",
                        request.request_id,
                    )

    async def _dispatch_request(
        self,
        request: AgentRequest,
        launcher: DagsterUserCodeLauncher[Any],
    ) -> str | dict[str, Any] | None:
        """Route a request to the appropriate handler."""
        api = request.request_api

        # Check for registered custom handlers first
        if api in self._request_handlers:
            return await self._request_handlers[api](request, launcher)

        # Built-in handlers
        if api == DagsterAgentApi.PING_LOCATION:
            return await self._handle_ping(request, launcher)
        elif api == DagsterAgentApi.LAUNCH_RUN:
            return await self._handle_launch_run(request, launcher)
        elif api == DagsterAgentApi.TERMINATE_RUN:
            return await self._handle_terminate_run(request, launcher)
        elif api == DagsterAgentApi.CHECK_FOR_WORKSPACE_UPDATES:
            return await self._handle_workspace_update_check(launcher)
        else:
            logger.warning(
                "No handler for request API %s (request %s)",
                api.value,
                request.request_id,
            )
            return {"error": f"No handler for {api.value}"}

    async def _handle_ping(
        self,
        request: AgentRequest,
        launcher: DagsterUserCodeLauncher[Any],
    ) -> dict[str, Any]:
        """Handle PING_LOCATION — verify the location's server is alive."""
        location_name = request.location_name or ""
        if launcher.has_grpc_endpoint(location_name):
            return {"status": "alive", "location": location_name}
        else:
            error = launcher.get_error(location_name)
            return {
                "status": "unavailable",
                "location": location_name,
                "error": error,
            }

    async def _handle_launch_run(
        self,
        request: AgentRequest,
        launcher: DagsterUserCodeLauncher[Any],
    ) -> dict[str, Any]:
        """Handle LAUNCH_RUN — launch a run through the location's gRPC server."""
        # Full implementation in Phase 6 (queue-based run launching)
        location_name = request.location_name or ""
        if not launcher.has_grpc_endpoint(location_name):
            return {
                "error": f"Location '{location_name}' is not available",
                "status": "failed",
            }

        # For MVP, we acknowledge the request but actual run launching
        # will be wired up in Phase 6
        logger.info(
            "LAUNCH_RUN received for location %s (Phase 6 implementation pending)",
            location_name,
        )
        return {"status": "accepted", "location": location_name}

    async def _handle_terminate_run(
        self,
        request: AgentRequest,
        launcher: DagsterUserCodeLauncher[Any],
    ) -> dict[str, Any]:
        """Handle TERMINATE_RUN — terminate a running execution."""
        # Full implementation in Phase 6
        body = (
            json.loads(request.request_body)
            if isinstance(request.request_body, str)
            else request.request_body
        )
        run_id = body.get("run_id", "unknown")
        logger.info("TERMINATE_RUN received for run %s (Phase 6)", run_id)
        return {"status": "accepted", "run_id": run_id}

    async def _handle_workspace_update_check(
        self, launcher: DagsterUserCodeLauncher[Any]
    ) -> dict[str, Any]:
        """Handle CHECK_FOR_WORKSPACE_UPDATES — trigger an immediate sync."""
        # The workspace sync loop handles this periodically;
        # for now just report current state
        return {
            "running_locations": launcher.running_locations,
            "ready": launcher.ready_to_serve_requests,
        }

    # ── Heartbeat ──────────────────────────────────────────────────

    async def _heartbeat_loop(self, launcher: DagsterUserCodeLauncher[Any]) -> None:
        """Periodically send heartbeats to the webserver."""
        assert self._client is not None
        assert self._shutdown_event is not None

        while not self._shutdown_event.is_set():
            try:
                await self._send_heartbeat(launcher)
            except Exception as exc:
                logger.warning("Heartbeat failed: %s", exc)

            await asyncio.sleep(self._instance.heartbeat_interval_seconds)

    async def _send_heartbeat(self, launcher: DagsterUserCodeLauncher[Any]) -> None:
        """Build and send a heartbeat with code server status."""
        assert self._client is not None

        # Collect per-location server status
        code_server_heartbeats = {}
        for location_name in launcher.running_locations:
            endpoint = launcher.get_grpc_endpoint(location_name)
            if endpoint:
                code_server_heartbeats[location_name] = {
                    "status": "running",
                    "endpoint": endpoint.address,
                }

        # Collect errors
        errors = []
        for location in self._instance.allowed_locations or []:
            error = launcher.get_error(location)
            if error:
                errors.append(f"{location}: {error}")

        heartbeat = AgentHeartbeat(
            agent_id=self._instance.labels.get("agent_id", "unknown"),
            agent_type="dagster-agent-oss",
            labels=self._instance.labels,
            metadata={
                "running_locations": launcher.running_locations,
                "ready": launcher.ready_to_serve_requests,
            },
            errors=errors or None,
            code_server_heartbeats=code_server_heartbeats or None,
        )

        await self._client.submit_heartbeat(heartbeat.to_dict())
        self._metrics.record_heartbeat()
        logger.debug("Heartbeat sent")

    # ── Shutdown ───────────────────────────────────────────────────

    async def shutdown(self) -> None:
        """Gracefully shut down the agent."""
        if not self._running:
            return
        self._running = False
        logger.info(
            "Agent shutting down (uptime=%.0fs, requests_ok=%d, requests_failed=%d)",
            self._metrics.uptime_seconds,
            self._metrics.requests_processed,
            self._metrics.requests_failed,
        )

        # Clean up sentinel files
        if self._sentinel:
            self._sentinel.cleanup()

        if self._client is not None:
            await self._client.close()
            self._client = None


# ── Registration helper ─────────────────────────────────────────────────


def register_handler(agent: DagsterAgent, api: DagsterAgentApi) -> Any:
    """Decorator to register a custom request handler.

    Usage::

        @register_handler(agent, DagsterAgentApi.LAUNCH_RUN)
        async def my_launcher(request, launcher):
            ...
    """

    def decorator(func: Any) -> Any:
        agent._request_handlers[api] = func
        return func

    return decorator
