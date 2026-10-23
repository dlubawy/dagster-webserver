"""Kubernetes-based user code launcher.

Deploys code locations as Kubernetes Deployments with associated Services.
Each code location gets its own Deployment + Service pair in the configured
namespace.

Server handle type: ``K8sHandle`` identifying the namespace/name pair.
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

logger = logging.getLogger("dagster-agent.launcher.k8s")

#: Default gRPC port for code location servers in K8s
DEFAULT_GRPC_PORT = 3000


@dataclass
class K8sHandle:
    """Opaque handle for a Kubernetes Deployment running a gRPC server."""

    namespace: str
    name: str
    image: str
    labels: dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


class K8sUserCodeLauncher(DagsterUserCodeLauncher[K8sHandle]):
    """Deploy code locations as Kubernetes Deployments + Services.

    Config options:
        namespace: K8s namespace for code location resources (default "dagster")
        image_pull_policy: Image pull policy for pods (default "IfNotPresent")
        deployment_startup_timeout: Seconds to wait for pod readiness (default 300)
        labels: Base labels applied to all resources
        annotations: Base annotations applied to all resources
        resources: K8s resource requirements (requests/limits)
        node_selector: Node selector for pod scheduling
        service_account_name: Service account for the pods
        env_vars: Base environment variables for all containers
        image_registry: Optional registry prefix
    """

    def __init__(
        self,
        namespace: str = "dagster",
        image_pull_policy: str = "IfNotPresent",
        deployment_startup_timeout: int = 300,
        labels: dict[str, str] | None = None,
        annotations: dict[str, str] | None = None,
        resources: dict[str, Any] | None = None,
        node_selector: dict[str, str] | None = None,
        service_account_name: str | None = None,
        env_vars: list[str] | dict[str, str] | None = None,
        image_registry: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._namespace = namespace
        self._image_pull_policy = image_pull_policy
        self._startup_timeout = deployment_startup_timeout
        self._labels = labels or {"app": "dagster-agent"}
        self._annotations = annotations or {}
        self._resources = resources
        self._node_selector = node_selector
        self._service_account_name = service_account_name
        self._env_vars = self._normalize_env(env_vars)
        self._image_registry = image_registry or ""
        self._apps_api: Any = None  # Lazy-initialized
        self._core_api: Any = None
        self._handles: dict[str, K8sHandle] = {}  # location → handle

    @property
    def user_code_deployment_type(self) -> str:
        return "k8s"

    def requires_images(self) -> bool:
        return True

    # ── Abstract implementations ───────────────────────────────────

    def _start_new_server_spinup(self, entry: UserCodeLauncherEntry) -> K8sHandle:
        """Create a K8s Deployment + Service for the code location."""
        apps_api = self._get_apps_api()
        core_api = self._get_core_api()
        deploy = entry.deploy_metadata or {}
        image = self._resolve_image(deploy)

        if not image:
            raise ValueError(
                f"K8s launcher requires an 'image' in deploy metadata "
                f"for location '{entry.location_name}'"
            )

        name = self._resource_name(entry.location_name)
        service_name = f"{name}-svc"

        # Build labels
        resource_labels = {
            **self._labels,
            "dagster/location": entry.location_name,
        }

        # Build environment
        env = dict(self._env_vars)
        env.update(deploy.get("env_vars", {}))

        # Build container command
        command = self._build_container_command(entry, deploy)

        logger.info(
            "Creating K8s Deployment %s/%s (image=%s) for location %s",
            self._namespace,
            name,
            image,
            entry.location_name,
        )

        # Create the Deployment
        deployment_body = self._build_deployment_spec(
            name=name,
            image=image,
            command=command,
            env=env,
            labels=resource_labels,
        )
        apps_api.create_namespaced_deployment(
            namespace=self._namespace, body=deployment_body
        )

        # Create the Service (ClusterIP)
        service_body = self._build_service_spec(
            name=service_name,
            selector={"app": name},
            port=DEFAULT_GRPC_PORT,
        )
        core_api.create_namespaced_service(namespace=self._namespace, body=service_body)

        handle = K8sHandle(
            namespace=self._namespace,
            name=name,
            image=image,
            labels=resource_labels,
        )
        self._handles[entry.location_name] = handle

        return handle

    def _wait_for_new_server_ready(
        self,
        entry: UserCodeLauncherEntry,
        handle: K8sHandle,
        timeout_seconds: float | None = None,
    ) -> ServerEndpoint:
        """Wait until the K8s pod is ready and gRPC is reachable."""
        timeout = timeout_seconds or self._startup_timeout
        start_time = time.time()
        core_api = self._get_core_api()

        while time.time() - start_time < timeout:
            # Check deployment status
            try:
                deployment = core_api.read_namespaced_deployment_status(
                    name=handle.name, namespace=handle.namespace
                )
                conditions = deployment.status.conditions or []
                available = any(
                    c.type == "Available" and c.status == "True" for c in conditions
                )
                if not available:
                    time.sleep(5)
                    continue
            except Exception as exc:
                logger.debug("Error checking deployment status: %s", exc)
                time.sleep(5)
                continue

            # Pod is available — construct the cluster-internal endpoint
            host = f"{handle.name}-svc.{self._namespace}.svc.cluster.local"
            return ServerEndpoint(host=host, port=DEFAULT_GRPC_PORT)

        raise TimeoutError(
            f"K8s Deployment {handle.namespace}/{handle.name} for location "
            f"'{entry.location_name}' did not become ready within {timeout}s"
        )

    def _remove_server_handle(self, handle: K8sHandle) -> None:
        """Delete the K8s Deployment and Service."""
        apps_api = self._get_apps_api()
        core_api = self._get_core_api()

        try:
            # Delete Service first (so no traffic routes to dying pods)
            service_name = f"{handle.name}-svc"
            core_api.delete_namespaced_service(
                name=service_name, namespace=handle.namespace
            )
        except Exception as exc:
            logger.warning("Error deleting service %s: %s", handle.name, exc)

        try:
            apps_api.delete_namespaced_deployment(
                name=handle.name, namespace=handle.namespace
            )
            logger.info("Deleted K8s Deployment %s/%s", handle.namespace, handle.name)
        except Exception as exc:
            logger.warning("Error deleting deployment %s: %s", handle.name, exc)

        # Clean up tracking
        for name, h in list(self._handles.items()):
            if h.name == handle.name and h.namespace == handle.namespace:
                del self._handles[name]
                break

    def _list_server_handles(self) -> list[K8sHandle]:
        """List all currently managed K8s handles."""
        return list(self._handles.values())

    # ── Run launcher ───────────────────────────────────────────────

    def run_launcher(self) -> LauncherRunLauncher:
        """Return a RunLauncher that runs runs as K8s Jobs."""
        return K8sRunLauncher(self)

    # ── Helpers ────────────────────────────────────────────────────

    def _get_apps_api(self) -> Any:
        """Lazily initialize the K8s Apps API client."""
        if self._apps_api is None:
            try:
                from kubernetes import client, config
            except ImportError as exc:
                raise ImportError(
                    "The 'kubernetes' package is required for K8sUserCodeLauncher. "
                    "Install it with: pip install kubernetes"
                ) from exc

            # Load kubeconfig or in-cluster config
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()

            self._apps_api = client.AppsV1Api()
        return self._apps_api

    def _get_core_api(self) -> Any:
        """Lazily initialize the K8s Core API client."""
        if self._core_api is None:
            try:
                from kubernetes import client
            except ImportError as exc:
                raise ImportError(
                    "The 'kubernetes' package is required for K8sUserCodeLauncher. "
                    "Install it with: pip install kubernetes"
                ) from exc
            self._core_api = client.CoreV1Api()
        return self._core_api

    def _resource_name(self, location_name: str) -> str:
        """Generate a valid K8s resource name from a location name."""
        # K8s names must be lowercase alphanum + '-', max 63 chars
        name = location_name.lower().replace("/", "-").replace("_", "-")
        # Remove invalid characters
        name = "".join(c for c in name if c.isalnum() or c == "-")
        # Truncate to 63 chars (leave room for -svc suffix)
        name = name[:50]
        return f"dl-{name}"

    def _resolve_image(self, deploy: dict[str, Any]) -> str | None:
        """Resolve the Docker image name from deploy metadata."""
        image = deploy.get("image")
        if not image:
            return None
        if self._image_registry and "/" not in image.split(":")[0]:
            image = f"{self._image_registry}{image}"
        return image

    def _build_container_command(
        self, entry: UserCodeLauncherEntry, deploy: dict[str, Any]
    ) -> list[str]:
        """Build the command to run inside the container."""
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

    def _build_deployment_spec(
        self,
        name: str,
        image: str,
        command: list[str],
        env: dict[str, str],
        labels: dict[str, str],
    ) -> Any:
        """Build a K8s Deployment spec dict."""
        from kubernetes import client

        container = client.V1Container(
            name="dagster-server",
            image=image,
            command=command,
            ports=[client.V1ContainerPort(container_port=DEFAULT_GRPC_PORT)],
            env=[client.V1EnvVar(name=k, value=v) for k, v in env.items()],
            resources=(
                client.V1ResourceRequirements(**self._resources)
                if self._resources
                else None
            ),
        )

        pod_spec = client.V1PodSpec(
            containers=[container],
            service_account_name=self._service_account_name,
            node_selector=self._node_selector,
        )

        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels=labels),
            spec=pod_spec,
        )

        spec = client.V1DeploymentSpec(
            replicas=1,
            selector=client.V1LabelSelector(match_labels={"app": name}),
            template=template,
        )

        return client.V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=self._namespace,
                labels=labels,
                annotations=self._annotations,
            ),
            spec=spec,
        )

    def _build_service_spec(
        self, name: str, selector: dict[str, str], port: int
    ) -> Any:
        """Build a K8s Service spec dict."""
        from kubernetes import client

        return client.V1Service(
            api_version="v1",
            kind="Service",
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=self._namespace,
                labels={"app": name},
            ),
            spec=client.V1ServiceSpec(
                type="ClusterIP",
                selector=selector,
                ports=[client.V1ServicePort(port=port, target_port=port)],
            ),
        )

    @staticmethod
    def _normalize_env(
        env_vars: list[str] | dict[str, str] | None,
    ) -> dict[str, str]:
        """Normalize env vars from list or dict format to dict."""
        if not env_vars:
            return {}
        if isinstance(env_vars, dict):
            return env_vars
        result = {}
        for item in env_vars:
            if "=" in item:
                key, _, value = item.partition("=")
                result[key] = value
        return result


class K8sRunLauncher(LauncherRunLauncher):
    """Run launcher that executes runs as Kubernetes Jobs."""

    def __init__(self, launcher: K8sUserCodeLauncher):
        super().__init__(launcher)
        self._launcher = launcher  # type: ignore[assignment]

    def launch_run(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[no-untyped-def]
        """Launch a run as a K8s Job.

        Full implementation will come with the queue-based run launching flow.
        For now, this delegates to the gRPC server of the code location.
        """
        raise NotImplementedError(
            "K8s run launching will be implemented with the queue-based flow."
        )
