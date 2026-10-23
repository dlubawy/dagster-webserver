"""DagsterAgentInstance — agent-side configuration and connection to webserver.

The agent instance is created from ``dagster.yaml`` config under the
``agent:`` key. It provides:
- Webserver URL and API credentials for REST communication
- Agent identity (labels, allowed locations)
- Timing parameters (poll interval, heartbeat interval)
- The user code launcher (rehydrated from config via ConfigurableClassData)

The instance does NOT create the HTTP client — that's done by the agent
loop. It only provides the configuration values needed to construct one.
"""

from __future__ import annotations

import logging
import os
from typing import Any, ClassVar, cast

import yaml

logger = logging.getLogger("dagster-agent.instance")


class DagsterAgentInstance:
    """Configuration and state for a single agent process.

    Created via ``DagsterAgentInstance.get()`` which reads from the
    ``agent:`` section of ``dagster.yaml`` in DAGSTER_HOME.

    Example dagster.yaml::

        agent:
          webserver_url: http://localhost:3005
          api_key: <service-account-api-key>
          labels:
            region: us-east-1
          allowed_locations: []  # empty = all locations
          poll_interval_seconds: 5
          heartbeat_interval_seconds: 30
          user_code_launcher:
            type: process
            config: {}
    """

    # ── Config keys (from dagster.yaml "agent" section) ───────────
    _CONFIG_KEY: ClassVar[str] = "agent"

    def __init__(
        self,
        webserver_url: str,
        api_key: str,
        labels: dict[str, str] | None = None,
        allowed_locations: list[str] | None = None,
        poll_interval_seconds: float = 5.0,
        heartbeat_interval_seconds: float = 30.0,
        request_timeout_seconds: float = 300.0,
        user_code_launcher_config: dict[str, Any] | None = None,
        sentinel_dir: str | None = None,
        metrics_log_interval_seconds: float = 60.0,
    ):
        self._webserver_url = webserver_url.rstrip("/")
        self._api_key = api_key
        self._labels = labels or {}
        self._allowed_locations = allowed_locations  # None/empty = all
        self._poll_interval_seconds = poll_interval_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._request_timeout_seconds = request_timeout_seconds
        self._user_code_launcher_config = user_code_launcher_config or {
            "type": "process",
            "config": {},
        }
        self._sentinel_dir = sentinel_dir
        self._metrics_log_interval_seconds = metrics_log_interval_seconds

        # Lazy-loaded launcher (created on first access)
        self._launcher: DagsterUserCodeLauncher | None = None

    # ── Factory ────────────────────────────────────────────────────

    @classmethod
    def get(cls) -> DagsterAgentInstance:
        """Create an instance from dagster.yaml config.

        Reads the ``agent:`` section from the DAGSTER_HOME/dagster.yaml file.
        Falls back to environment variables for key settings.
        """
        dagster_home = os.environ.get("DAGSTER_HOME", ".")
        yaml_path = os.path.join(dagster_home, "dagster.yaml")

        config: dict[str, Any] = {}
        if os.path.exists(yaml_path):
            with open(yaml_path) as f:
                full_config = yaml.safe_load(f) or {}
            config = full_config.get(cls._CONFIG_KEY, {})

        # Allow env var overrides
        webserver_url = config.get(
            "webserver_url", os.environ.get("DAGSTER_AGENT_WEBSERVER_URL")
        )
        api_key = config.get("api_key", os.environ.get("DAGSTER_AGENT_API_KEY"))

        if not webserver_url:
            raise ValueError(
                "Agent configuration missing 'webserver_url'. Set it in "
                "dagster.yaml under the 'agent:' key or via "
                "DAGSTER_AGENT_WEBSERVER_URL env var."
            )
        if not api_key:
            raise ValueError(
                "Agent configuration missing 'api_key'. Set it in "
                "dagster.yaml under the 'agent:' key or via "
                "DAGSTER_AGENT_API_KEY env var."
            )

        return cls(
            webserver_url=webserver_url,
            api_key=api_key,
            labels=config.get("labels"),
            allowed_locations=config.get("allowed_locations"),
            poll_interval_seconds=float(config.get("poll_interval_seconds", 5.0)),
            heartbeat_interval_seconds=float(
                config.get("heartbeat_interval_seconds", 30.0)
            ),
            request_timeout_seconds=float(config.get("request_timeout_seconds", 300.0)),
            user_code_launcher_config=config.get("user_code_launcher"),
            sentinel_dir=config.get("sentinel_dir"),
            metrics_log_interval_seconds=float(
                config.get("metrics_log_interval_seconds", 60.0)
            ),
        )

    # ── Properties ─────────────────────────────────────────────────

    @property
    def webserver_url(self) -> str:
        """Base URL of the dagster-webserver."""
        return self._webserver_url

    @property
    def agent_api_url(self) -> str:
        """Full URL to the agent REST API."""
        return f"{self._webserver_url}/api/agent"

    @property
    def api_key(self) -> str:
        """API key for authenticating with the webserver."""
        return self._api_key

    @property
    def labels(self) -> dict[str, str]:
        """Labels identifying this agent (for routing)."""
        return self._labels

    @property
    def allowed_locations(self) -> list[str] | None:
        """Code locations this agent is allowed to manage. None = all."""
        return self._allowed_locations

    @property
    def poll_interval_seconds(self) -> float:
        """How often to poll for new requests (seconds)."""
        return self._poll_interval_seconds

    @property
    def heartbeat_interval_seconds(self) -> float:
        """How often to send heartbeats (seconds)."""
        return self._heartbeat_interval_seconds

    @property
    def request_timeout_seconds(self) -> float:
        """Timeout for individual request processing (seconds)."""
        return self._request_timeout_seconds

    @property
    def sentinel_dir(self) -> str | None:
        """Directory for liveness/readiness sentinel files (for k8s probes)."""
        return self._sentinel_dir

    @property
    def metrics_log_interval_seconds(self) -> float:
        """How often to log agent metrics (seconds)."""
        return self._metrics_log_interval_seconds

    @property
    def user_code_launcher(self) -> DagsterUserCodeLauncher:
        """The user code launcher instance (lazily created)."""
        if self._launcher is None:
            self._launcher = _create_launcher(self._user_code_launcher_config)
        return cast("DagsterUserCodeLauncher", self._launcher)

    # ── Lifecycle ──────────────────────────────────────────────────

    def __enter__(self) -> DagsterAgentInstance:
        return self

    def __exit__(self, *args: Any) -> None:
        self.dispose()

    def dispose(self) -> None:
        """Clean up resources (stop launcher, etc.)."""
        if self._launcher is not None:
            self._launcher.stop()
            self._launcher = None


# ── Launcher rehydration ────────────────────────────────────────────────


def _create_launcher(config: dict[str, Any]) -> DagsterUserCodeLauncher:
    """Create a user code launcher from its config dict.

    The config has the form: {"type": "process", "config": {...}}
    This maps to the appropriate ConfigurableClass subclass.
    """

    launcher_type = config.get("type", "process")
    launcher_config = config.get("config", {})

    if launcher_type == "process":
        from dagster_agent.user_code_launcher.process import ProcessUserCodeLauncher

        return ProcessUserCodeLauncher(**launcher_config)
    elif launcher_type == "k8s":
        from dagster_agent.user_code_launcher.k8s import K8sUserCodeLauncher

        return K8sUserCodeLauncher(**launcher_config)
    elif launcher_type == "docker":
        from dagster_agent.user_code_launcher.docker import DockerUserCodeLauncher

        return DockerUserCodeLauncher(**launcher_config)
    else:
        raise ValueError(f"Unknown user_code_launcher type: {launcher_type!r}")


# Forward reference to avoid circular import at module load time
from dagster_agent.user_code_launcher.base import DagsterUserCodeLauncher  # noqa: E402
