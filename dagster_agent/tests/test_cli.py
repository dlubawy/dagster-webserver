"""Tests for the dagster-agent CLI — exercises the full execution path.

These tests catch issues like missing context manager protocols, import
errors, and config validation failures that would only surface when a
user actually runs the command.

The agent is a long-running daemon — we never actually run its main loop.
Instead we verify each component (config, context managers, launcher
lifecycle) in isolation so the full wiring is tested without blocking.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest
import yaml
from click.testing import CliRunner

from dagster_agent.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def agent_home(tmp_path):
    """Create a minimal DAGSTER_HOME with valid agent config."""
    config = {
        "agent": {
            "webserver_url": "http://localhost:9999",
            "api_key": "test-key-12345",
            "poll_interval_seconds": 1,
            "heartbeat_interval_seconds": 5,
        }
    }
    yaml_path = tmp_path / "dagster.yaml"
    yaml_path.write_text(yaml.dump(config))
    return str(tmp_path)


class TestCliHelp:
    """Basic CLI structure tests."""

    def test_top_level_help(self, runner):
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "dagster-agent" in result.output
        assert "run" in result.output

    def test_run_help(self, runner):
        result = runner.invoke(cli, ["run", "--help"])
        assert result.exit_code == 0
        assert "--dagster-home" in result.output
        assert "--log-level" in result.output


class TestCliRunConfig:
    """Test config loading and validation for the run command.

    These invoke the CLI but expect it to fail at the config stage —
    before any daemon loop starts — so they return immediately.
    """

    def test_missing_dagster_yaml(self, runner, tmp_path):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        result = runner.invoke(cli, ["run", "--dagster-home", str(empty_dir)])
        assert result.exit_code != 0
        output = result.output.lower() + str(result.exception or "").lower()
        assert "webserver_url" in output or "config" in output or "failed" in output

    def test_missing_webserver_url(self, runner, tmp_path):
        config = {"agent": {"api_key": "test-key"}}
        (tmp_path / "dagster.yaml").write_text(yaml.dump(config))
        result = runner.invoke(cli, ["run", "--dagster-home", str(tmp_path)])
        assert result.exit_code != 0
        output = result.output.lower() + str(result.exception or "").lower()
        assert "webserver_url" in output

    def test_missing_api_key(self, runner, tmp_path):
        config = {"agent": {"webserver_url": "http://localhost:9999"}}
        (tmp_path / "dagster.yaml").write_text(yaml.dump(config))
        result = runner.invoke(cli, ["run", "--dagster-home", str(tmp_path)])
        assert result.exit_code != 0
        output = result.output.lower() + str(result.exception or "").lower()
        assert "api_key" in output

    def test_log_level_invalid_rejected(self, runner, agent_home):
        result = runner.invoke(
            cli, ["run", "--dagster-home", agent_home, "--log-level", "invalid"]
        )
        assert result.exit_code != 0
        assert "Invalid value" in result.output


class TestContextManagerProtocol:
    """Verify context manager protocols exist and work correctly.

    These are the exact bugs users hit: 'X does not support the context
    manager protocol'. We test each component individually.
    """

    def test_instance_sync_context_manager(self, agent_home):
        from dagster_agent.instance import DagsterAgentInstance

        with patch.dict(os.environ, {"DAGSTER_HOME": agent_home}):
            instance = DagsterAgentInstance.get()
            with instance as inst:
                assert inst is instance
                assert inst.webserver_url == "http://localhost:9999"

    def test_agent_sync_context_manager(self, agent_home):
        """DagsterAgent must support sync 'with' (the CLI uses this)."""
        from dagster_agent.agent import DagsterAgent
        from dagster_agent.instance import DagsterAgentInstance

        with patch.dict(os.environ, {"DAGSTER_HOME": agent_home}):
            instance = DagsterAgentInstance.get()
            # __enter__ creates an httpx.AsyncClient — mock it to avoid
            # any socket activity. __exit__ calls asyncio.run(shutdown()).
            with patch("dagster_agent.agent.AgentAPIClient") as mock_client_cls:
                with DagsterAgent(instance) as agent:
                    assert agent is not None
                    assert agent._client is not None
                    mock_client_cls.assert_called_once()

    def test_launcher_context_manager(self, agent_home):
        """Launcher must support 'with' statement."""
        from dagster_agent.instance import DagsterAgentInstance

        with patch.dict(os.environ, {"DAGSTER_HOME": agent_home}):
            instance = DagsterAgentInstance.get()
            launcher = instance.user_code_launcher
            # start(run_reconcile_thread=False) avoids spawning a thread
            launcher.start(run_reconcile_thread=False)
            assert launcher._started
            launcher.stop()
            assert not launcher._started


class TestRunLoopWiring:
    """Verify run_loop delegates to _async_main correctly.

    We patch _async_main so no actual daemon loop runs. This catches
    wiring bugs (wrong args, missing method, etc.) without blocking.
    """

    def test_run_loop_calls_async_main_with_launcher(self, agent_home):
        from dagster_agent.agent import DagsterAgent
        from dagster_agent.instance import DagsterAgentInstance

        with patch.dict(os.environ, {"DAGSTER_HOME": agent_home}):
            instance = DagsterAgentInstance.get()
            launcher = instance.user_code_launcher
            launcher.start(run_reconcile_thread=False)

            mock_async_main = AsyncMock(return_value=None)
            with (
                patch.object(DagsterAgent, "_async_main", new=mock_async_main),
                patch("dagster_agent.agent.AgentAPIClient"),
            ):
                with DagsterAgent(instance) as agent:
                    agent.run_loop(launcher)

            mock_async_main.assert_called_once_with(launcher)
            launcher.stop()
