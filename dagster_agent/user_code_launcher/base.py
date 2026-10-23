"""Abstract base class for user code launchers.

The launcher manages gRPC server processes/containers for code locations.
It operates in a reconcile loop:

1. The agent fetches desired workspace entries from the webserver.
2. The launcher diffs desired vs actual state.
3. New servers are started for missing locations.
4. Stale servers (no longer in desired) are stopped.
5. Health checks verify running servers are responsive.

Concrete implementations provide:
- ``_start_new_server_spinup()`` — create the server resource
- ``_wait_for_new_server_ready()`` — block until gRPC is reachable
- ``_remove_server_handle()`` — tear down the server resource
- ``_list_server_handles()`` — enumerate currently managed handles
- ``run_launcher()`` — return a RunLauncher for submitting runs
"""

from __future__ import annotations

import logging
import threading
from abc import abstractmethod, abstractproperty
from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

logger = logging.getLogger("dagster-agent.launcher")


# ── Types ───────────────────────────────────────────────────────────────

ServerHandle = TypeVar("ServerHandle")

#: Key type for location state: (location_name,) — simplified from Cloud's
#: (deployment_name, location_name) tuple since OSS doesn't have deployments.
LocationKey = tuple[str]


@dataclass
class UserCodeLauncherEntry:
    """Desired state for a code location."""

    location_name: str
    deploy_metadata: dict[str, Any] = field(default_factory=dict)
    labels: dict[str, str] | None = None
    loaded_manifest: dict[str, Any] | None = None


@dataclass
class ServerEndpoint:
    """A running gRPC server endpoint."""

    host: str
    port: int
    metadata: list[tuple[str, str]] | None = None

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


# ── Abstract Base Class ─────────────────────────────────────────────────


class DagsterUserCodeLauncher(AbstractContextManager, Generic[ServerHandle]):
    """Abstract base class for managing code location gRPC servers.

    Subclasses implement the platform-specific server lifecycle (process,
    k8s pod, docker container) while this base provides the reconcile loop,
    health checking, and state management.
    """

    def __init__(self, **kwargs: Any):
        self._desired_entries: dict[LocationKey, UserCodeLauncherEntry] = {}
        self._actual_entries: dict[LocationKey, UserCodeLauncherEntry] = {}
        self._grpc_servers: dict[
            LocationKey, "ServerEndpoint | str"  # str = error message
        ] = {}
        self._server_handles: dict[LocationKey, ServerHandle] = {}

        self._metadata_lock = threading.Lock()
        self._started = False
        self._reconcile_thread: threading.Thread | None = None
        self._shutdown_event = threading.Event()
        self._reconcile_interval_seconds = 5.0
        self._reconcile_count = 0

    # ── Abstract properties ────────────────────────────────────────

    @abstractproperty
    def user_code_deployment_type(self) -> str:
        """Identifier for this launcher type (e.g., 'process', 'k8s')."""
        ...

    @abstractmethod
    def requires_images(self) -> bool:
        """Whether this launcher requires container images."""
        ...

    # ── Abstract methods (server lifecycle) ────────────────────────

    @abstractmethod
    def _start_new_server_spinup(
        self,
        entry: UserCodeLauncherEntry,
    ) -> ServerHandle:
        """Create and start a new server for the given location.

        Returns an opaque handle that identifies the running resource.
        Does NOT block until the server is ready — call
        _wait_for_new_server_ready() after this returns.
        """
        ...

    @abstractmethod
    def _wait_for_new_server_ready(
        self,
        entry: UserCodeLauncherEntry,
        handle: ServerHandle,
        timeout_seconds: float = 60.0,
    ) -> ServerEndpoint:
        """Block until the gRPC server is ready to serve requests.

        Returns the endpoint (host, port) once ready.
        Raises if the server fails to start within timeout.
        """
        ...

    @abstractmethod
    def _remove_server_handle(self, handle: ServerHandle) -> None:
        """Tear down the server resource identified by the handle."""
        ...

    @abstractmethod
    def _list_server_handles(self) -> list[ServerHandle]:
        """List all currently managed server handles.

        Used to detect orphaned/stale servers during reconciliation.
        """
        ...

    # ── Concrete: lifecycle management ─────────────────────────────

    def start(self, run_reconcile_thread: bool = True) -> None:
        """Start the launcher (and optionally the reconcile thread)."""
        if self._started:
            return
        self._started = True
        logger.info("Launcher started (type=%s)", self.user_code_deployment_type)

        if run_reconcile_thread:
            self._shutdown_event.clear()
            self._reconcile_thread = threading.Thread(
                target=self._reconcile_loop,
                name="dagster-agent-reconcile",
                daemon=True,
            )
            self._reconcile_thread.start()

    def stop(self) -> None:
        """Gracefully stop the launcher and all managed servers."""
        if not self._started and not self._server_handles:
            return
        logger.info("Stopping launcher...")
        self._shutdown_event.set()

        if self._reconcile_thread is not None:
            self._reconcile_thread.join(timeout=10)
            self._reconcile_thread = None

        # Stop all running servers
        with self._metadata_lock:
            handles = dict(self._server_handles)

        for location_key, handle in handles.items():
            try:
                self._remove_server_handle(handle)
                logger.info("Stopped server for %s", location_key[0])
            except Exception:
                logger.exception("Error stopping server for %s", location_key[0])

        self._server_handles.clear()
        self._grpc_servers.clear()
        self._started = False
        logger.info("Launcher stopped.")

    def __enter__(self) -> DagsterUserCodeLauncher[ServerHandle]:
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()

    # ── Concrete: reconcile loop ───────────────────────────────────

    def _reconcile_loop(self) -> None:
        """Background thread that periodically reconciles state."""
        while not self._shutdown_event.is_set():
            try:
                self.reconcile()
            except Exception:
                logger.exception("Error during reconcile")
            self._shutdown_event.wait(self._reconcile_interval_seconds)

    def reconcile(self) -> None:
        """Diff desired vs actual state and take corrective action.

        1. Start servers for locations in desired but not running.
        2. Stop servers for locations that are running but not desired.
        3. Health-check running servers; restart if unhealthy.
        """
        with self._metadata_lock:
            desired = dict(self._desired_entries)

        if not desired and not self._server_handles:
            return  # Nothing to do yet

        self._reconcile_count += 1

        # ── Add: start servers for new/changed locations ───────────
        for location_key, entry in desired.items():
            if location_key not in self._server_handles:
                # New server needed
                try:
                    logger.info("Starting server for location %s", location_key[0])
                    handle = self._start_new_server_spinup(entry)
                    endpoint = self._wait_for_new_server_ready(entry, handle)
                    self._server_handles[location_key] = handle
                    self._grpc_servers[location_key] = endpoint
                    logger.info(
                        "Server ready for location %s at %s",
                        location_key[0],
                        endpoint.address,
                    )
                except Exception as exc:
                    logger.exception(
                        "Failed to start server for location %s: %s",
                        location_key[0],
                        exc,
                    )
                    self._grpc_servers[location_key] = str(exc)

        # ── Remove: stop servers no longer in desired ──────────────
        with self._metadata_lock:
            current_handles = dict(self._server_handles)

        for location_key, handle in current_handles.items():
            if location_key not in desired:
                logger.info("Stopping server for removed location %s", location_key[0])
                try:
                    self._remove_server_handle(handle)
                except Exception:
                    logger.exception("Error stopping server for %s", location_key[0])
                del self._server_handles[location_key]
                self._grpc_servers.pop(location_key, None)

        # ── Health check: verify running servers are responsive ────
        self._check_server_health()

    def _check_server_health(self) -> None:
        """Check that all running gRPC servers respond to pings.

        Servers that fail health checks are marked as errored and will
        be restarted on the next reconcile cycle.
        """
        with self._metadata_lock:
            current = {
                k: v
                for k, v in self._grpc_servers.items()
                if isinstance(v, ServerEndpoint)
            }

        for location_key, endpoint in current.items():
            try:
                self._ping_server(endpoint)
            except Exception as exc:
                logger.warning(
                    "Health check failed for %s at %s: %s",
                    location_key[0],
                    endpoint.address,
                    exc,
                )
                # Mark as error — will be restarted next reconcile
                self._grpc_servers[location_key] = str(exc)
                # Remove the handle so it gets restarted
                handle = self._server_handles.pop(location_key, None)
                if handle is not None:
                    try:
                        self._remove_server_handle(handle)
                    except Exception:
                        pass

    def _ping_server(self, endpoint: ServerEndpoint) -> None:
        """Ping a gRPC server to verify it's alive. Override for custom checks."""
        # Default implementation: attempt a simple connection check
        import grpc

        channel = grpc.insecure_channel(endpoint.address)
        try:
            grpc.channel_ready_future(channel).result(timeout=5.0)
        finally:
            channel.close()

    # ── Concrete: state management ─────────────────────────────────

    def update_grpc_metadata(
        self, desired_entries: Mapping[str, UserCodeLauncherEntry]
    ) -> None:
        """Set the desired state from webserver workspace entries.

        Called by the agent main loop after fetching workspace entries.
        """
        with self._metadata_lock:
            self._desired_entries = {
                (name,): entry for name, entry in desired_entries.items()
            }

    def has_grpc_endpoint(self, location_name: str) -> bool:
        """Check if a gRPC server is ready for the given location."""
        key = (location_name,)
        return isinstance(self._grpc_servers.get(key), ServerEndpoint)

    def get_grpc_endpoint(self, location_name: str) -> ServerEndpoint | None:
        """Get the endpoint for a running server, or None if not ready."""
        key = (location_name,)
        endpoint = self._grpc_servers.get(key)
        return endpoint if isinstance(endpoint, ServerEndpoint) else None

    def get_error(self, location_name: str) -> str | None:
        """Get the error message for a failed server, or None."""
        key = (location_name,)
        error = self._grpc_servers.get(key)
        return error if isinstance(error, str) else None

    @property
    def ready_to_serve_requests(self) -> bool:
        """True after at least one successful reconcile cycle."""
        return self._reconcile_count > 0

    @property
    def running_locations(self) -> list[str]:
        """Names of locations with active gRPC servers."""
        return [
            key[0]
            for key, val in self._grpc_servers.items()
            if isinstance(val, ServerEndpoint)
        ]


# ── RunLauncher interface ───────────────────────────────────────────────


class LauncherRunLauncher:
    """Base RunLauncher that delegates to the agent's gRPC servers.

    Concrete launchers may override ``launch_run`` for platform-specific
    behavior (e.g., submitting a k8s Job). The default implementation
    submits runs through the code location's gRPC server.
    """

    def __init__(self, launcher: DagsterUserCodeLauncher[Any]):
        self._launcher = launcher

    def launch_run(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[no-untyped-def]
        """Launch a run through the location's gRPC server."""
        raise NotImplementedError(
            "Run launching will be implemented in Phase 6 (queue-based)."
        )
