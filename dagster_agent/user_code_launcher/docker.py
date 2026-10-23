"""Docker-based user code launcher.

Runs code locations as Docker containers. Each location gets its own
container with a mapped port for gRPC communication.

Server handle type: ``DockerContainerHandle`` wrapping a docker Container.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from dagster_agent.user_code_launcher.base import (
    DagsterUserCodeLauncher,
    LauncherRunLauncher,
    ServerEndpoint,
    UserCodeLauncherEntry,
)

logger = logging.getLogger("dagster-agent.launcher.docker")

#: Default gRPC port inside the container
DEFAULT_GRPC_PORT = 3000


@dataclass
class DockerContainerHandle:
    """Opaque handle for a Docker container running a gRPC server."""

    container_id: str
    container_name: str
    image: str
    grpc_port_in_container: int = DEFAULT_GRPC_PORT
    created_at: float = field(default_factory=time.time)


class DockerUserCodeLauncher(DagsterUserCodeLauncher[DockerContainerHandle]):
    """Run code locations as Docker containers.

    Config options:
        networks: List of Docker network names to attach containers to
        env_vars: Base environment variables for all containers
        container_kwargs: Additional kwargs passed to client.containers.run()
            (e.g., mem_limit, cpus, restart_policy)
        image_registry: Optional registry prefix (e.g., "ghcr.io/myorg/")
        server_process_startup_timeout: Seconds to wait for gRPC readiness (default 60)
        container_name_prefix: Prefix for generated container names (default "dagster-loc-")
    """

    def __init__(
        self,
        networks: list[str] | None = None,
        env_vars: list[str] | dict[str, str] | None = None,
        container_kwargs: dict[str, Any] | None = None,
        image_registry: str | None = None,
        server_process_startup_timeout: int = 60,
        container_name_prefix: str = "dagster-loc-",
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._networks = networks or []
        self._env_vars = self._normalize_env(env_vars)
        self._container_kwargs = container_kwargs or {}
        self._image_registry = image_registry or ""
        self._startup_timeout = server_process_startup_timeout
        self._name_prefix = container_name_prefix
        self._docker_client: Any = None  # Lazy-initialized
        self._containers: dict[str, DockerContainerHandle] = {}  # location → handle

    @property
    def user_code_deployment_type(self) -> str:
        return "docker"

    def requires_images(self) -> bool:
        return True

    # ── Abstract implementations ───────────────────────────────────

    def _start_new_server_spinup(
        self, entry: UserCodeLauncherEntry
    ) -> DockerContainerHandle:
        """Create and start a Docker container for the code location."""
        client = self._get_docker_client()
        deploy = entry.deploy_metadata or {}
        image = self._resolve_image(deploy)

        if not image:
            raise ValueError(
                f"Docker launcher requires an 'image' in deploy metadata "
                f"for location '{entry.location_name}'"
            )

        container_name = self._name_prefix + entry.location_name.replace("/", "_")

        # Build environment
        env = dict(self._env_vars)
        env.update(deploy.get("env_vars", {}))

        # Build command: run the dagster gRPC server inside the container
        command = self._build_container_command(entry, deploy)

        logger.info(
            "Starting Docker container %s (image=%s) for location %s",
            container_name,
            image,
            entry.location_name,
        )

        container = client.containers.run(
            image=image,
            name=container_name,
            detach=True,
            ports={DEFAULT_GRPC_PORT: None},  # Auto-assign host port
            environment=env,
            command=command,
            labels={
                "dagster.managed_by": "dagster-agent",
                "dagster.location": entry.location_name,
            },
            networks=self._networks or None,
            **self._container_kwargs,
        )

        handle = DockerContainerHandle(
            container_id=container.id,
            container_name=container_name,
            image=image,
        )
        self._containers[entry.location_name] = handle

        return handle

    def _wait_for_new_server_ready(
        self,
        entry: UserCodeLauncherEntry,
        handle: DockerContainerHandle,
        timeout_seconds: float | None = None,
    ) -> ServerEndpoint:
        """Wait until the gRPC server in the container is accepting connections."""
        timeout = timeout_seconds or self._startup_timeout
        start_time = time.time()

        client = self._get_docker_client()
        container = client.containers.get(handle.container_id)

        while time.time() - start_time < timeout:
            # Check if container is still running
            container.reload()
            if container.status != "running":
                logs = container.logs(tail=50).decode(errors="replace")
                raise RuntimeError(
                    f"Docker container {handle.container_name} for location "
                    f"'{entry.location_name}' stopped during startup. "
                    f"Status: {container.status}. Logs: {logs[-2000:]}"
                )

            # Get the mapped host port
            host_port = self._get_mapped_port(container)
            if host_port is not None:
                # Verify gRPC is actually listening
                if self._check_grpc_ready("127.0.0.1", host_port):
                    logger.info(
                        "Docker container %s ready on port %d for location %s",
                        handle.container_name,
                        host_port,
                        entry.location_name,
                    )
                    return ServerEndpoint(host="127.0.0.1", port=host_port)

            time.sleep(1.0)

        raise TimeoutError(
            f"Docker container {handle.container_name} for location "
            f"'{entry.location_name}' did not become ready within {timeout}s"
        )

    def _remove_server_handle(self, handle: DockerContainerHandle) -> None:
        """Stop and remove the Docker container."""
        client = self._get_docker_client()
        try:
            container = client.containers.get(handle.container_id)
            container.stop(timeout=30)
            container.remove(force=True)
            logger.info("Removed Docker container %s", handle.container_name)
        except Exception as exc:
            logger.warning(
                "Error removing container %s: %s", handle.container_name, exc
            )

        # Clean up tracking
        for name, h in list(self._containers.items()):
            if h.container_id == handle.container_id:
                del self._containers[name]
                break

    def _list_server_handles(self) -> list[DockerContainerHandle]:
        """List all currently managed container handles."""
        return list(self._containers.values())

    # ── Run launcher ───────────────────────────────────────────────

    def run_launcher(self) -> LauncherRunLauncher:
        """Return a RunLauncher that runs runs as Docker containers."""
        return DockerRunLauncher(self)

    # ── Helpers ────────────────────────────────────────────────────

    def _get_docker_client(self) -> Any:
        """Lazily initialize the Docker client."""
        if self._docker_client is None:
            try:
                import docker
            except ImportError as exc:
                raise ImportError(
                    "The 'docker' package is required for DockerUserCodeLauncher. "
                    "Install it with: pip install docker"
                ) from exc
            self._docker_client = docker.from_env()
        return self._docker_client

    def _resolve_image(self, deploy: dict[str, Any]) -> str | None:
        """Resolve the Docker image name from deploy metadata."""
        image = deploy.get("image")
        if not image:
            return None
        # Prepend registry if configured and image doesn't already have one
        if self._image_registry and "/" not in image.split(":")[0]:
            image = f"{self._image_registry}{image}"
        return image

    def _build_container_command(
        self, entry: UserCodeLauncherEntry, deploy: dict[str, Any]
    ) -> list[str]:
        """Build the command to run inside the container."""
        # The container should already have dagster installed.
        # We run the gRPC server with the appropriate code pointer.
        cmd = [
            "python",
            "-m",
            "dagster",
            "api",
            "grpc",
            "--port",
            str(DEFAULT_GRPC_PORT),
        ]

        python_file = deploy.get("python_file")
        module_name = deploy.get("module_name")

        if python_file:
            cmd.extend(["-f", python_file])
        elif module_name:
            cmd.extend(["-m", module_name])

        return cmd

    @staticmethod
    def _normalize_env(
        env_vars: list[str] | dict[str, str] | None,
    ) -> dict[str, str]:
        """Normalize env vars from list or dict format to dict."""
        if not env_vars:
            return {}
        if isinstance(env_vars, dict):
            return env_vars
        # List of "KEY=VALUE" strings
        result = {}
        for item in env_vars:
            if "=" in item:
                key, _, value = item.partition("=")
                result[key] = value
        return result

    @staticmethod
    def _get_mapped_port(container: Any) -> int | None:
        """Get the host port mapped to the container's gRPC port."""
        try:
            ports = container.attrs.get("NetworkSettings", {}).get("Ports", {})
            mapping = ports.get(f"{DEFAULT_GRPC_PORT}/tcp")
            if mapping:
                return int(mapping[0]["HostPort"])
        except (KeyError, IndexError, TypeError):
            pass
        return None

    @staticmethod
    def _check_grpc_ready(host: str, port: int) -> bool:
        """Check if the gRPC port is accepting connections."""
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2.0)
            return s.connect_ex((host, port)) == 0


class DockerRunLauncher(LauncherRunLauncher):
    """Run launcher that executes runs as separate Docker containers."""

    def __init__(self, launcher: DockerUserCodeLauncher):
        super().__init__(launcher)
        self._launcher = launcher  # type: ignore[assignment]

    def launch_run(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[no-untyped-def]
        """Launch a run as a Docker container.

        Full implementation will come with the queue-based run launching flow.
        For now, this delegates to the gRPC server of the code location.
        """
        raise NotImplementedError(
            "Docker run launching will be implemented with the queue-based flow."
        )
