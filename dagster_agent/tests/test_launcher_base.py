"""Tests for dagster_agent.user_code_launcher.base."""

from __future__ import annotations

from dagster_agent.user_code_launcher.base import (
    DagsterUserCodeLauncher,
    ServerEndpoint,
    UserCodeLauncherEntry,
)


class MockLauncher(DagsterUserCodeLauncher[int]):
    """Minimal concrete launcher for testing the base class logic."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._started_servers: list[str] = []
        self._stopped_handles: list[int] = []
        self._next_handle = 100
        self._fail_next_start = False

    @property
    def user_code_deployment_type(self) -> str:
        return "mock"

    def requires_images(self) -> bool:
        return False

    def _start_new_server_spinup(self, entry: UserCodeLauncherEntry) -> int:
        if self._fail_next_start:
            self._fail_next_start = False
            raise RuntimeError("Simulated startup failure")
        handle = self._next_handle
        self._next_handle += 1
        self._started_servers.append(entry.location_name)
        return handle

    def _wait_for_new_server_ready(
        self, entry: UserCodeLauncherEntry, handle: int, timeout_seconds: float = 60.0
    ) -> ServerEndpoint:
        # Simulate immediate readiness
        return ServerEndpoint(host="127.0.0.1", port=5000 + handle)

    def _remove_server_handle(self, handle: int) -> None:
        self._stopped_handles.append(handle)

    def _list_server_handles(self) -> list[int]:
        return list(self._server_handles.values())

    def _ping_server(self, endpoint: ServerEndpoint) -> None:
        """Mock: always pass health check."""
        pass


class TestLauncherBase:
    def test_initial_state(self):
        launcher = MockLauncher()
        assert not launcher.ready_to_serve_requests
        assert launcher.running_locations == []

    def test_update_grpc_metadata_sets_desired(self):
        launcher = MockLauncher()
        entries = {
            "loc-a": UserCodeLauncherEntry(location_name="loc-a"),
            "loc-b": UserCodeLauncherEntry(location_name="loc-b"),
        }
        launcher.update_grpc_metadata(entries)

        # Reconcile should start servers
        launcher.reconcile()

        assert "loc-a" in launcher.running_locations
        assert "loc-b" in launcher.running_locations
        assert launcher.ready_to_serve_requests

    def test_reconcile_starts_servers(self):
        launcher = MockLauncher()
        launcher.update_grpc_metadata(
            {"loc-a": UserCodeLauncherEntry(location_name="loc-a")}
        )
        launcher.reconcile()

        assert "loc-a" in launcher._started_servers
        assert launcher.has_grpc_endpoint("loc-a")

    def test_reconcile_stops_removed_locations(self):
        launcher = MockLauncher()
        launcher.update_grpc_metadata(
            {
                "loc-a": UserCodeLauncherEntry(location_name="loc-a"),
                "loc-b": UserCodeLauncherEntry(location_name="loc-b"),
            }
        )
        launcher.reconcile()

        # Now remove loc-b from desired
        launcher.update_grpc_metadata(
            {"loc-a": UserCodeLauncherEntry(location_name="loc-a")}
        )
        launcher.reconcile()

        assert "loc-a" in launcher.running_locations
        assert "loc-b" not in launcher.running_locations
        # loc-b's handle should have been stopped
        assert len(launcher._stopped_handles) == 1

    def test_reconcile_handles_startup_failure(self):
        launcher = MockLauncher()
        launcher._fail_next_start = True
        launcher.update_grpc_metadata(
            {"loc-a": UserCodeLauncherEntry(location_name="loc-a")}
        )
        launcher.reconcile()

        # Server failed to start — should not be in running locations
        assert "loc-a" not in launcher.running_locations
        error = launcher.get_error("loc-a")
        assert error is not None
        assert "Simulated startup failure" in error

    def test_get_grpc_endpoint(self):
        launcher = MockLauncher()
        launcher.update_grpc_metadata(
            {"loc-a": UserCodeLauncherEntry(location_name="loc-a")}
        )
        launcher.reconcile()

        endpoint = launcher.get_grpc_endpoint("loc-a")
        assert endpoint is not None
        assert endpoint.host == "127.0.0.1"
        assert endpoint.port > 5000

    def test_get_grpc_endpoint_not_running(self):
        launcher = MockLauncher()
        assert launcher.get_grpc_endpoint("nonexistent") is None

    def test_has_grpc_endpoint(self):
        launcher = MockLauncher()
        assert not launcher.has_grpc_endpoint("loc-a")

        launcher.update_grpc_metadata(
            {"loc-a": UserCodeLauncherEntry(location_name="loc-a")}
        )
        launcher.reconcile()
        assert launcher.has_grpc_endpoint("loc-a")

    def test_stop_cleans_up_all_servers(self):
        launcher = MockLauncher()
        launcher.update_grpc_metadata(
            {
                "loc-a": UserCodeLauncherEntry(location_name="loc-a"),
                "loc-b": UserCodeLauncherEntry(location_name="loc-b"),
            }
        )
        launcher.reconcile()

        launcher.stop()

        assert len(launcher._stopped_handles) == 2
        assert launcher.running_locations == []

    def test_empty_reconcile_is_noop(self):
        launcher = MockLauncher()
        # No desired entries, no actual — should not crash
        launcher.reconcile()
        assert launcher.running_locations == []


class TestServerEndpoint:
    def test_address(self):
        ep = ServerEndpoint(host="0.0.0.0", port=8080)
        assert ep.address == "0.0.0.0:8080"
