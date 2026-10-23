"""Tests for K8sUserCodeLauncher."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from dagster_agent.user_code_launcher.base import ServerEndpoint, UserCodeLauncherEntry
from dagster_agent.user_code_launcher.k8s import (
    DEFAULT_GRPC_PORT,
    K8sHandle,
    K8sUserCodeLauncher,
)


def _make_mock_kubernetes():
    """Create a mock kubernetes module structure."""
    k8s_module = ModuleType("kubernetes")
    k8s_client_module = ModuleType("kubernetes.client")
    k8s_config_module = ModuleType("kubernetes.config")

    # Mock ConfigException
    class ConfigException(Exception):
        pass

    k8s_config_module.ConfigException = ConfigException
    k8s_config_module.load_incluster_config = MagicMock(
        side_effect=ConfigException("not in cluster")
    )
    k8s_config_module.load_kube_config = MagicMock(return_value=None)

    # Mock API clients
    apps_api = MagicMock()
    core_api = MagicMock()
    k8s_client_module.AppsV1Api = MagicMock(return_value=apps_api)
    k8s_client_module.CoreV1Api = MagicMock(return_value=core_api)

    # Mock V1* model classes
    for cls_name in [
        "V1Container",
        "V1ContainerPort",
        "V1EnvVar",
        "V1ResourceRequirements",
        "V1PodSpec",
        "V1PodTemplateSpec",
        "V1LabelSelector",
        "V1DeploymentSpec",
        "V1Deployment",
        "V1ObjectMeta",
        "V1Service",
        "V1ServiceSpec",
        "V1ServicePort",
    ]:
        setattr(k8s_client_module, cls_name, MagicMock())

    k8s_module.client = k8s_client_module
    k8s_module.config = k8s_config_module

    return k8s_module, apps_api, core_api


@pytest.fixture(autouse=True)
def mock_kubernetes_module():
    """Ensure a mock 'kubernetes' module is available for all tests."""
    k8s_module, apps_api, core_api = _make_mock_kubernetes()
    with patch.dict(
        sys.modules,
        {
            "kubernetes": k8s_module,
            "kubernetes.client": k8s_module.client,
            "kubernetes.config": k8s_module.config,
        },
    ):
        yield {"apps": apps_api, "core": core_api, "module": k8s_module}


@pytest.fixture
def launcher() -> K8sUserCodeLauncher:
    return K8sUserCodeLauncher(
        namespace="test-ns",
        image_pull_policy="Always",
        labels={"app": "dagster-agent", "team": "data"},
        resources={"requests": {"cpu": "500m"}},
    )


class TestK8sUserCodeLauncher:
    def test_deployment_type(self, launcher: K8sUserCodeLauncher):
        assert launcher.user_code_deployment_type == "k8s"

    def test_requires_images(self, launcher: K8sUserCodeLauncher):
        assert launcher.requires_images() is True

    def test_default_config(self):
        launcher = K8sUserCodeLauncher()
        assert launcher._namespace == "dagster"
        assert launcher._image_pull_policy == "IfNotPresent"
        assert launcher._startup_timeout == 300
        assert "app" in launcher._labels

    def test_env_normalization_list(self):
        launcher = K8sUserCodeLauncher(env_vars=["FOO=bar", "BAZ=qux"])
        assert launcher._env_vars == {"FOO": "bar", "BAZ": "qux"}

    def test_env_normalization_dict(self):
        launcher = K8sUserCodeLauncher(env_vars={"FOO": "bar"})
        assert launcher._env_vars == {"FOO": "bar"}

    def test_resource_name_simple(self, launcher: K8sUserCodeLauncher):
        assert launcher._resource_name("my-location") == "dl-my-location"

    def test_resource_name_with_slashes(self, launcher: K8sUserCodeLauncher):
        assert launcher._resource_name("org/my-location") == "dl-org-my-location"

    def test_resource_name_truncation(self, launcher: K8sUserCodeLauncher):
        long_name = "a" * 100
        result = launcher._resource_name(long_name)
        # Should be truncated to fit within limits
        assert len(result) <= 53  # "dl-" + 50 chars

    def test_resolve_image_no_registry(self, launcher: K8sUserCodeLauncher):
        assert (
            launcher._resolve_image({"image": "myorg/myimage:1.0"})
            == "myorg/myimage:1.0"
        )

    def test_resolve_image_with_registry(self):
        launcher = K8sUserCodeLauncher(image_registry="ghcr.io/org/")
        assert (
            launcher._resolve_image({"image": "myimage:1.0"})
            == "ghcr.io/org/myimage:1.0"
        )

    def test_resolve_image_missing(self, launcher: K8sUserCodeLauncher):
        assert launcher._resolve_image({}) is None

    def test_build_container_command_python_file(self, launcher: K8sUserCodeLauncher):
        entry = UserCodeLauncherEntry(
            location_name="test", deploy_metadata={"python_file": "/app/defs.py"}
        )
        cmd = launcher._build_container_command(entry, {"python_file": "/app/defs.py"})
        assert "api" in cmd and "grpc" in cmd
        assert "-f" in cmd
        assert "/app/defs.py" in cmd

    def test_build_container_command_module(self, launcher: K8sUserCodeLauncher):
        entry = UserCodeLauncherEntry(
            location_name="test", deploy_metadata={"module_name": "my_defs"}
        )
        cmd = launcher._build_container_command(entry, {"module_name": "my_defs"})
        assert "-m" in cmd
        assert "my_defs" in cmd

    def test_start_new_server_spinup_requires_image(
        self, launcher: K8sUserCodeLauncher, mock_kubernetes_module
    ):
        entry = UserCodeLauncherEntry(location_name="test", deploy_metadata={})
        with pytest.raises(ValueError, match="requires an 'image'"):
            launcher._start_new_server_spinup(entry)

    def test_start_new_server_spinup(
        self, launcher: K8sUserCodeLauncher, mock_kubernetes_module
    ):
        entry = UserCodeLauncherEntry(
            location_name="my-location",
            deploy_metadata={"image": "myorg/myimage:1.0"},
        )

        handle = launcher._start_new_server_spinup(entry)

        assert isinstance(handle, K8sHandle)
        assert handle.namespace == "test-ns"
        assert handle.name == "dl-my-location"
        assert handle.image == "myorg/myimage:1.0"
        assert handle.labels["dagster/location"] == "my-location"

        # Verify APIs were called
        mock_kubernetes_module["apps"].create_namespaced_deployment.assert_called_once()
        mock_kubernetes_module["core"].create_namespaced_service.assert_called_once()

    def test_wait_for_new_server_ready(
        self, launcher: K8sUserCodeLauncher, mock_kubernetes_module
    ):
        # Mock deployment status as available
        deployment_status = MagicMock()
        condition = MagicMock()
        condition.type = "Available"
        condition.status = "True"
        deployment_status.status.conditions = [condition]
        mock_kubernetes_module[
            "core"
        ].read_namespaced_deployment_status.return_value = deployment_status

        handle = K8sHandle(
            namespace="test-ns",
            name="dl-my-location",
            image="test:latest",
        )
        entry = UserCodeLauncherEntry(location_name="my-location")

        endpoint = launcher._wait_for_new_server_ready(entry, handle)

        assert isinstance(endpoint, ServerEndpoint)
        assert endpoint.host == "dl-my-location-svc.test-ns.svc.cluster.local"
        assert endpoint.port == DEFAULT_GRPC_PORT

    def test_wait_for_new_server_not_ready(
        self, launcher: K8sUserCodeLauncher, mock_kubernetes_module
    ):
        # Mock deployment status as not available
        deployment_status = MagicMock()
        condition = MagicMock()
        condition.type = "Available"
        condition.status = "False"
        deployment_status.status.conditions = [condition]
        mock_kubernetes_module[
            "core"
        ].read_namespaced_deployment_status.return_value = deployment_status

        handle = K8sHandle(
            namespace="test-ns",
            name="dl-my-location",
            image="test:latest",
        )
        entry = UserCodeLauncherEntry(location_name="my-location")

        with pytest.raises(TimeoutError, match="did not become ready"):
            launcher._wait_for_new_server_ready(entry, handle, timeout_seconds=0.1)

    def test_remove_server_handle(
        self, launcher: K8sUserCodeLauncher, mock_kubernetes_module
    ):
        handle = K8sHandle(
            namespace="test-ns",
            name="dl-my-location",
            image="test:latest",
        )
        launcher._handles["my-location"] = handle

        launcher._remove_server_handle(handle)

        mock_kubernetes_module["core"].delete_namespaced_service.assert_called_once()
        mock_kubernetes_module["apps"].delete_namespaced_deployment.assert_called_once()
        assert "my-location" not in launcher._handles

    def test_list_server_handles(self, launcher: K8sUserCodeLauncher):
        h1 = K8sHandle("ns", "name1", "img1")
        h2 = K8sHandle("ns", "name2", "img2")
        launcher._handles["loc1"] = h1
        launcher._handles["loc2"] = h2

        handles = launcher._list_server_handles()
        assert len(handles) == 2


class TestK8sHandle:
    def test_defaults(self):
        h = K8sHandle(namespace="ns", name="name", image="img")
        assert h.labels == {}
        assert h.created_at > 0
