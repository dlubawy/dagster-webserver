"""Tests for dagster_agent.api.types."""

from __future__ import annotations

import pytest

from dagster_agent.api.types import (
    AgentHeartbeat,
    AgentRequest,
    CodeLocationDeployData,
    DagsterAgentApi,
)


class TestDagsterAgentApi:
    def test_all_values_are_strings(self):
        for api in DagsterAgentApi:
            assert isinstance(api.value, str)

    def test_from_string(self):
        assert DagsterAgentApi("LAUNCH_RUN") == DagsterAgentApi.LAUNCH_RUN

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError):
            DagsterAgentApi("NOT_A_REAL_API")


class TestAgentRequest:
    def test_basic_construction(self):
        req = AgentRequest(
            request_id="abc-123",
            request_api=DagsterAgentApi.LAUNCH_RUN,
            request_body='{"run_key": "test"}',
        )
        assert req.request_id == "abc-123"
        assert req.request_api == DagsterAgentApi.LAUNCH_RUN
        assert req.location_name is None

    def test_from_string_api(self):
        req = AgentRequest(
            request_id="1",
            request_api="PING_LOCATION",
            request_body="{}",
            location_name="my-loc",
        )
        assert req.request_api == DagsterAgentApi.PING_LOCATION
        assert req.location_name == "my-loc"

    def test_repr(self):
        req = AgentRequest(
            request_id="x",
            request_api=DagsterAgentApi.TERMINATE_RUN,
            request_body="{}",
            location_name="loc-a",
        )
        r = repr(req)
        assert "x" in r
        assert "TERMINATE_RUN" in r
        assert "loc-a" in r


class TestAgentHeartbeat:
    def test_to_dict(self):
        hb = AgentHeartbeat(
            agent_id="agent-1",
            agent_type="dagster-agent-oss",
            labels={"region": "us-east"},
            metadata={"running_locations": ["loc-a"]},
            errors=None,
            code_server_heartbeats={"loc-a": {"status": "running"}},
        )
        d = hb.to_dict()
        assert d["agent_id"] == "agent-1"
        assert d["agent_type"] == "dagster-agent-oss"
        assert d["labels"] == {"region": "us-east"}
        assert d["code_server_heartbeats"]["loc-a"]["status"] == "running"

    def test_to_dict_none_fields(self):
        hb = AgentHeartbeat(agent_id="agent-2")
        d = hb.to_dict()
        assert d["errors"] is None
        assert d["metadata"] is None


class TestCodeLocationDeployData:
    def test_from_dict_and_roundtrip(self):
        original = {
            "python_file": "/path/to/defs.py",
            "working_directory": "/app",
            "executable_path": "/usr/bin/python3",
            "attribute": "repo_def",
        }
        data = CodeLocationDeployData.from_dict(original)
        assert data.python_file == "/path/to/defs.py"
        assert data.working_directory == "/app"
        assert data.executable_path == "/usr/bin/python3"
        assert data.attribute == "repo_def"

        # Roundtrip
        restored = data.to_dict()
        assert restored == original

    def test_from_dict_empty(self):
        data = CodeLocationDeployData.from_dict({})
        assert data.python_file is None
        assert data.module_name is None
        assert data.to_dict() == {}

    def test_module_name_variant(self):
        data = CodeLocationDeployData(
            module_name="my_package",
            working_directory="/opt",
        )
        d = data.to_dict()
        assert d["module_name"] == "my_package"
        assert d["working_directory"] == "/opt"
        assert "python_file" not in d

    def test_to_code_pointer_none(self):
        data = CodeLocationDeployData()
        assert data.to_code_pointer() is None
