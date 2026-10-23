"""Process-based user code launcher.

Spawns local gRPC server subprocesses for each code location. This is
the simplest launcher — ideal for development and single-machine deployments.

Server handle type: ``int`` (process PID).
"""

from __future__ import annotations

import logging
import os
import socket
import subprocess
import sys
import time
from typing import Any

from dagster_agent.user_code_launcher.base import (
    DagsterUserCodeLauncher,
    LauncherRunLauncher,
    ServerEndpoint,
    UserCodeLauncherEntry,
)

logger = logging.getLogger("dagster-agent.launcher.process")


class ProcessUserCodeLauncher(DagsterUserCodeLauncher[int]):
    """Spawn local gRPC server subprocesses. Handle type = PID.

    Each code location gets a dedicated Python process running
    ``python -m dagster.api.server`` (or equivalent) with the
    appropriate repository definition.

    Config options:
        server_process_startup_timeout: Seconds to wait for gRPC readiness (default 60)
        server_port_range: Tuple of (start, end) ports for gRPC servers (default None = auto)
        extra_env: Additional environment variables for child processes
    """

    def __init__(
        self,
        server_process_startup_timeout: int = 60,
        server_port_range: tuple[int, int] | None = None,
        extra_env: dict[str, str] | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._startup_timeout = server_process_startup_timeout
        self._port_range = server_port_range
        self._extra_env = extra_env or {}
        self._processes: dict[int, subprocess.Popen] = {}  # PID → process
        self._pid_to_port: dict[int, int] = {}  # PID → allocated port

    # ── Abstract implementations ───────────────────────────────────

    @property
    def user_code_deployment_type(self) -> str:
        return "process"

    def requires_images(self) -> bool:
        return False

    def _start_new_server_spinup(self, entry: UserCodeLauncherEntry) -> int:
        """Spawn a gRPC server subprocess for the code location.

        Returns the PID of the spawned process.
        """
        # Allocate a port
        port = self._allocate_port()

        # Build the command to start the dagster API server
        cmd = self._build_server_command(entry, port)

        # Prepare environment
        env = os.environ.copy()
        env.update(self._extra_env)
        env["DAGSTER_WEBSERVER_GRPC_PORT"] = str(port)

        logger.info(
            "Starting gRPC server for %s: %s", entry.location_name, " ".join(cmd)
        )

        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self._processes[process.pid] = process
        self._pid_to_port[process.pid] = port

        return process.pid

    def _wait_for_new_server_ready(
        self,
        entry: UserCodeLauncherEntry,
        handle: int,
        timeout_seconds: float | None = None,
    ) -> ServerEndpoint:
        """Wait until the gRPC server subprocess is accepting connections."""
        timeout = timeout_seconds or self._startup_timeout
        start_time = time.time()

        # Find the port we allocated for this process
        port = self._get_port_for_pid(handle)

        while time.time() - start_time < timeout:
            # Check if process is still alive
            proc = self._processes.get(handle)
            if proc is None or proc.poll() is not None:
                stderr_output = ""
                if proc and proc.stderr:
                    stderr_output = proc.stderr.read().decode(errors="replace")[:2000]
                raise RuntimeError(
                    f"gRPC server process {handle} for location "
                    f"'{entry.location_name}' died during startup. "
                    f"Exit code: {proc.returncode if proc else 'unknown'}. "
                    f"Stderr: {stderr_output}"
                )

            # Check if port is accepting connections
            if self._check_port(port):
                logger.info(
                    "gRPC server for %s ready on port %d (PID %d)",
                    entry.location_name,
                    port,
                    handle,
                )
                return ServerEndpoint(host="127.0.0.1", port=port)

            time.sleep(0.5)

        raise TimeoutError(
            f"gRPC server for location '{entry.location_name}' "
            f"(PID {handle}) did not become ready within {timeout}s"
        )

    def _remove_server_handle(self, handle: int) -> None:
        """Terminate the gRPC server subprocess."""
        proc = self._processes.pop(handle, None)
        if proc is None:
            return

        # Remove port mapping
        self._pid_to_port.pop(handle, None)

        try:
            proc.terminate()
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logger.warning(
                "Process %d did not exit after SIGTERM, sending SIGKILL", handle
            )
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            logger.exception("Error terminating process %d", handle)

    def _list_server_handles(self) -> list[int]:
        """List PIDs of all currently managed server processes."""
        return list(self._processes.keys())

    # ── Run launcher ───────────────────────────────────────────────

    def run_launcher(self) -> LauncherRunLauncher:
        """Return a RunLauncher that submits runs via gRPC."""
        return ProcessRunLauncher(self)

    # ── Helpers ────────────────────────────────────────────────────

    def _build_server_command(
        self, entry: UserCodeLauncherEntry, port: int
    ) -> list[str]:
        """Build the command line to start a gRPC server for this location.

        Uses ``dagster api grpc`` which is the standard Dagster CLI command
        for serving the inter-process API over gRPC.
        """
        deploy = entry.deploy_metadata or {}

        # Determine what to serve
        python_file = deploy.get("python_file")
        module_name = deploy.get("module_name")
        working_dir = deploy.get("working_directory", ".")
        executable = deploy.get("executable_path", sys.executable)

        cmd = [executable, "-m", "dagster", "api", "grpc"]
        cmd.extend(["--port", str(port)])
        cmd.extend(["--location-name", entry.location_name])

        if python_file:
            cmd.extend(["--python-file", python_file])
        elif module_name:
            cmd.extend(["--module-name", module_name])
        else:
            # Default: look for definitions in working directory via --defs-state-info
            # or autoload. For now, use the working directory flag.
            pass

        if working_dir and working_dir != ".":
            cmd.extend(["--working-directory", working_dir])

        return cmd

    def _allocate_port(self) -> int:
        """Allocate a free port for the gRPC server."""
        if self._port_range:
            start, end = self._port_range
            # Simple: find first available in range
            for port in range(start, end + 1):
                if self._check_port(port, expect_free=True):
                    return port
            raise RuntimeError(f"No free ports in range {start}-{end}")

        # Auto-allocate: bind to port 0 to get an OS-assigned port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def _get_port_for_pid(self, pid: int) -> int:
        """Get the port assigned to a given PID."""
        port = self._pid_to_port.get(pid)
        if port is None:
            raise RuntimeError(f"No port tracked for PID {pid}")
        return port

    @staticmethod
    def _check_port(port: int, expect_free: bool = False) -> bool:
        """Check if a port is accepting connections (or is free)."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1.0)
            result = s.connect_ex(("127.0.0.1", port))
            if expect_free:
                return result != 0  # Non-zero means connection refused = free
            return result == 0  # Zero means connection succeeded


class ProcessRunLauncher(LauncherRunLauncher):
    """Run launcher that submits runs via the location's gRPC server."""

    def launch_run(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[no-untyped-def]
        """Launch a run through the code location's gRPC server.

        The actual implementation will be completed in Phase 6 when we
        wire up the queue-based run launching flow.
        """
        raise NotImplementedError(
            "Process run launching will be implemented in Phase 6."
        )
