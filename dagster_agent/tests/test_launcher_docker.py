"""Tests for DockerUserCodeLauncher."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dagster_agent.user_code_launcher.base import ServerEndpoint, UserCodeLauncherEntry
from dagster_agent.user_code_launcher.docker import (
    DEFAULT_GRPC_PORT,
    DockerContainerHandle,
    DockerUserCodeLauncher,
)


@pytest.fixture
def launcher() -> DockerUserCodeLauncher:
    return DockerUserCodeLauncher(
        networks=["test-net"],
        env_vars={"DAGSTER_HOME": "/opt/dagster"},
        container_kwargs={"mem_limit": "512m"},
    )


@pytest.fixture
def mock_docker_client():
    with patch("docker.from_env") as mock_from_env:
        client = MagicMock()
        mock_from_env.return_value = client
        yield client


class TestDockerUserCodeLauncher:
    def test_deployment_type(self, launcher: DockerUserCodeLauncher):
        assert launcher.user_code_deployment_type == "docker"

    def test_requires_images(self, launcher: DockerUserCodeLauncher):
        assert launcher.requires_images() is True

    def test_default_config(self):
        launcher = DockerUserCodeLauncher()
        assert launcher._networks == []
        assert launcher._env_vars == {}
        assert launcher._container_kwargs == {}
        assert launcher._startup_timeout == 60
        assert launcher._name_prefix == "dagster-loc-"

    def test_env_normalization_list(self):
        launcher = DockerUserCodeLauncher(env_vars=["FOO=bar", "BAZ=qux"])
        assert launcher._env_vars == {"FOO": "bar", "BAZ": "qux"}

    def test_env_normalization_dict(self):
        launcher = DockerUserCodeLauncher(env_vars={"FOO": "bar"})
        assert launcher._env_vars == {"FOO": "bar"}

    def test_resolve_image_no_registry(self, launcher: DockerUserCodeLauncher):
        assert (
            launcher._resolve_image({"image": "myorg/myimage:1.0"})
            == "myorg/myimage:1.0"
        )

    def test_resolve_image_with_registry(self):
        launcher = DockerUserCodeLauncher(image_registry="ghcr.io/org/")
        assert (
            launcher._resolve_image({"image": "myimage:1.0"})
            == "ghcr.io/org/myimage:1.0"
        )

    def test_resolve_image_already_has_registry(self):
        launcher = DockerUserCodeLauncher(image_registry="ghcr.io/org/")
        # Image already has a registry prefix
        assert (
            launcher._resolve_image({"image": "other.org/image:1.0"})
            == "other.org/image:1.0"
        )

    def test_resolve_image_missing(self, launcher: DockerUserCodeLauncher):
        assert launcher._resolve_image({}) is None

    def test_build_container_command_python_file(
        self, launcher: DockerUserCodeLauncher
    ):
        entry = UserCodeLauncherEntry(
            location_name="test", deploy_metadata={"python_file": "/app/defs.py"}
        )
        cmd = launcher._build_container_command(entry, {"python_file": "/app/defs.py"})
        assert "api" in cmd and "grpc" in cmd
        assert "-f" in cmd
        assert "/app/defs.py" in cmd

    def test_build_container_command_module(self, launcher: DockerUserCodeLauncher):
        entry = UserCodeLauncherEntry(
            location_name="test", deploy_metadata={"module_name": "my_defs"}
        )
        cmd = launcher._build_container_command(entry, {"module_name": "my_defs"})
        assert "-m" in cmd
        assert "my_defs" in cmd

    def test_start_new_server_spinup_requires_image(
        self, launcher: DockerUserCodeLauncher, mock_docker_client
    ):
        entry = UserCodeLauncherEntry(location_name="test", deploy_metadata={})
        with pytest.raises(ValueError, match="requires an 'image'"):
            launcher._start_new_server_spinup(entry)

    def test_start_new_server_spinup(
        self, launcher: DockerUserCodeLauncher, mock_docker_client
    ):
        container = MagicMock()
        container.id = "abc123"
        mock_docker_client.containers.run.return_value = container

        entry = UserCodeLauncherEntry(
            location_name="my-location",
            deploy_metadata={"image": "myorg/myimage:1.0"},
        )

        handle = launcher._start_new_server_spinup(entry)

        assert isinstance(handle, DockerContainerHandle)
        assert handle.container_id == "abc123"
        assert handle.container_name == "dagster-loc-my-location"
        assert handle.image == "myorg/myimage:1.0"

        # Verify the docker client was called correctly
        mock_docker_client.containers.run.assert_called_once()
        call_kwargs = mock_docker_client.containers.run.call_args.kwargs
        assert call_kwargs["image"] == "myorg/myimage:1.0"
        assert call_kwargs["name"] == "dagster-loc-my-location"
        assert call_kwargs["detach"] is True
        assert DEFAULT_GRPC_PORT in call_kwargs["ports"]

    def test_wait_for_new_server_ready(
        self, launcher: DockerUserCodeLauncher, mock_docker_client
    ):
        container = MagicMock()
        container.id = "abc123"
        container.status = "running"
        container.attrs = {
            "NetworkSettings": {
                "Ports": {f"{DEFAULT_GRPC_PORT}/tcp": [{"HostPort": "4000"}]}
            }
        }
        mock_docker_client.containers.get.return_value = container

        handle = DockerContainerHandle(
            container_id="abc123",
            container_name="dagster-loc-test",
            image="test:latest",
        )
        entry = UserCodeLauncherEntry(location_name="test")

        with patch.object(launcher, "_check_grpc_ready", return_value=True):
            endpoint = launcher._wait_for_new_server_ready(entry, handle)

        assert isinstance(endpoint, ServerEndpoint)
        assert endpoint.host == "127.0.0.1"
        assert endpoint.port == 4000

    def test_wait_for_new_server_container_died(
        self, launcher: DockerUserCodeLauncher, mock_docker_client
    ):
        container = MagicMock()
        container.id = "abc123"
        container.status = "exited"
        container.logs.return_value = b"error occurred"
        mock_docker_client.containers.get.return_value = container

        handle = DockerContainerHandle(
            container_id="abc123",
            container_name="dagster-loc-test",
            image="test:latest",
        )
        entry = UserCodeLauncherEntry(location_name="test")

        with pytest.raises(RuntimeError, match="stopped during startup"):
            launcher._wait_for_new_server_ready(entry, handle)

    def test_remove_server_handle(
        self, launcher: DockerUserCodeLauncher, mock_docker_client
    ):
        container = MagicMock()
        mock_docker_client.containers.get.return_value = container

        handle = DockerContainerHandle(
            container_id="abc123",
            container_name="dagster-loc-my-location",
            image="test:latest",
        )
        launcher._containers["my-location"] = handle

        launcher._remove_server_handle(handle)

        container.stop.assert_called_once_with(timeout=30)
        container.remove.assert_called_once_with(force=True)
        assert "my-location" not in launcher._containers

    def test_list_server_handles(self, launcher: DockerUserCodeLauncher):
        h1 = DockerContainerHandle("id1", "name1", "img1")
        h2 = DockerContainerHandle("id2", "name2", "img2")
        launcher._containers["loc1"] = h1
        launcher._containers["loc2"] = h2

        handles = launcher._list_server_handles()
        assert len(handles) == 2
        assert h1 in handles
        assert h2 in handles

    def test_get_mapped_port(self, launcher: DockerUserCodeLauncher):
        container = MagicMock()
        container.attrs = {
            "NetworkSettings": {
                "Ports": {f"{DEFAULT_GRPC_PORT}/tcp": [{"HostPort": "54321"}]}
            }
        }
        assert launcher._get_mapped_port(container) == 54321

    def test_get_mapped_port_no_mapping(self, launcher: DockerUserCodeLauncher):
        container = MagicMock()
        container.attrs = {"NetworkSettings": {"Ports": {}}}
        assert launcher._get_mapped_port(container) is None


class TestDockerContainerHandle:
    def test_defaults(self):
        h = DockerContainerHandle(container_id="abc", container_name="test", image="x")
        assert h.grpc_port_in_container == DEFAULT_GRPC_PORT
        assert h.created_at > 0
