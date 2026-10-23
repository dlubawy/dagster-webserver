"""Tests for dagster_agent.instance."""

from __future__ import annotations

import os

import pytest
import yaml

from dagster_agent.instance import DagsterAgentInstance


class TestDagsterAgentInstance:
    def test_direct_construction(self):
        inst = DagsterAgentInstance(
            webserver_url="http://localhost:3005",
            api_key="test-key-123",
            labels={"region": "us-east"},
            allowed_locations=["loc-a"],
            poll_interval_seconds=3.0,
            heartbeat_interval_seconds=15.0,
        )
        assert inst.webserver_url == "http://localhost:3005"
        assert inst.agent_api_url == "http://localhost:3005/api/agent"
        assert inst.api_key == "test-key-123"
        assert inst.labels == {"region": "us-east"}
        assert inst.allowed_locations == ["loc-a"]
        assert inst.poll_interval_seconds == 3.0
        assert inst.heartbeat_interval_seconds == 15.0

    def test_trailing_slash_stripped(self):
        inst = DagsterAgentInstance(
            webserver_url="http://localhost:3005/",
            api_key="key",
        )
        assert inst.webserver_url == "http://localhost:3005"

    def test_defaults(self):
        inst = DagsterAgentInstance(
            webserver_url="http://x",
            api_key="key",
        )
        assert inst.labels == {}
        assert inst.allowed_locations is None
        assert inst.poll_interval_seconds == 5.0
        assert inst.heartbeat_interval_seconds == 30.0
        assert inst.request_timeout_seconds == 300.0

    def test_get_from_yaml(self, tmp_path):
        dagster_home = tmp_path / "dagster_home"
        dagster_home.mkdir()

        config = {
            "agent": {
                "webserver_url": "http://my-server:8080",
                "api_key": "secret-key",
                "labels": {"env": "prod"},
                "allowed_locations": ["loc-a", "loc-b"],
                "poll_interval_seconds": 10,
                "heartbeat_interval_seconds": 60,
            }
        }
        (dagster_home / "dagster.yaml").write_text(yaml.dump(config))

        old_home = os.environ.get("DAGSTER_HOME")
        os.environ["DAGSTER_HOME"] = str(dagster_home)
        try:
            inst = DagsterAgentInstance.get()
            assert inst.webserver_url == "http://my-server:8080"
            assert inst.api_key == "secret-key"
            assert inst.labels == {"env": "prod"}
            assert inst.allowed_locations == ["loc-a", "loc-b"]
            assert inst.poll_interval_seconds == 10.0
            assert inst.heartbeat_interval_seconds == 60.0
        finally:
            if old_home is not None:
                os.environ["DAGSTER_HOME"] = old_home
            else:
                del os.environ["DAGSTER_HOME"]

    def test_get_missing_webserver_url(self, tmp_path):
        dagster_home = tmp_path / "dagster_home"
        dagster_home.mkdir()
        (dagster_home / "dagster.yaml").write_text(yaml.dump({"agent": {}}))

        old_home = os.environ.get("DAGSTER_HOME")
        old_url = os.environ.get("DAGSTER_AGENT_WEBSERVER_URL")
        os.environ["DAGSTER_HOME"] = str(dagster_home)
        os.environ.pop("DAGSTER_AGENT_WEBSERVER_URL", None)
        try:
            with pytest.raises(ValueError, match="webserver_url"):
                DagsterAgentInstance.get()
        finally:
            if old_home is not None:
                os.environ["DAGSTER_HOME"] = old_home
            else:
                del os.environ["DAGSTER_HOME"]
            if old_url is not None:
                os.environ["DAGSTER_AGENT_WEBSERVER_URL"] = old_url

    def test_get_from_env_vars(self, tmp_path):
        dagster_home = tmp_path / "dagster_home"
        dagster_home.mkdir()
        # No dagster.yaml — rely on env vars only

        old_home = os.environ.get("DAGSTER_HOME")
        old_url = os.environ.get("DAGSTER_AGENT_WEBSERVER_URL")
        old_key = os.environ.get("DAGSTER_AGENT_API_KEY")
        os.environ["DAGSTER_HOME"] = str(dagster_home)
        os.environ["DAGSTER_AGENT_WEBSERVER_URL"] = "http://env-server:9090"
        os.environ["DAGSTER_AGENT_API_KEY"] = "env-key"
        try:
            inst = DagsterAgentInstance.get()
            assert inst.webserver_url == "http://env-server:9090"
            assert inst.api_key == "env-key"
        finally:
            if old_home is not None:
                os.environ["DAGSTER_HOME"] = old_home
            else:
                del os.environ["DAGSTER_HOME"]
            if old_url is not None:
                os.environ["DAGSTER_AGENT_WEBSERVER_URL"] = old_url
            else:
                os.environ.pop("DAGSTER_AGENT_WEBSERVER_URL", None)
            if old_key is not None:
                os.environ["DAGSTER_AGENT_API_KEY"] = old_key
            else:
                os.environ.pop("DAGSTER_AGENT_API_KEY", None)

    def test_user_code_launcher_process(self):
        inst = DagsterAgentInstance(
            webserver_url="http://x",
            api_key="key",
            user_code_launcher_config={"type": "process", "config": {}},
        )
        launcher = inst.user_code_launcher
        assert launcher.user_code_deployment_type == "process"
        assert not launcher.requires_images()

    def test_user_code_launcher_unknown_type(self):
        inst = DagsterAgentInstance(
            webserver_url="http://x",
            api_key="key",
            user_code_launcher_config={"type": "unknown-type", "config": {}},
        )
        with pytest.raises(ValueError, match="Unknown user_code_launcher"):
            _ = inst.user_code_launcher

    def test_dispose(self):
        inst = DagsterAgentInstance(
            webserver_url="http://x",
            api_key="key",
            user_code_launcher_config={"type": "process", "config": {}},
        )
        # Access launcher to create it
        _ = inst.user_code_launcher
        inst.dispose()
        # After dispose, accessing launcher should recreate it
        launcher = inst.user_code_launcher
        assert launcher is not None
