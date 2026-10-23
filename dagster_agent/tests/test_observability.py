"""Tests for agent observability (sentinel files + metrics)."""

from __future__ import annotations

import os
import time

from dagster_agent.observability import (
    AgentMetrics,
    SentinelWriter,
    create_sentinel_writer,
)


class TestSentinelWriter:
    def test_init_creates_directory(self, tmp_path):
        sentinel_dir = str(tmp_path / "sentinels")
        SentinelWriter(sentinel_dir)
        assert os.path.isdir(sentinel_dir)

    def test_touch_liveness(self, tmp_path):
        writer = SentinelWriter(str(tmp_path))
        liveness_file = tmp_path / "liveness"

        assert not liveness_file.exists()
        writer.touch_liveness()
        assert liveness_file.exists()

        # Touch again updates mtime
        content1 = liveness_file.read_text()
        time.sleep(0.01)
        writer.touch_liveness()
        content2 = liveness_file.read_text()
        assert content1 != content2  # Timestamp changed

    def test_touch_readiness_true(self, tmp_path):
        writer = SentinelWriter(str(tmp_path))
        readiness_file = tmp_path / "readiness"

        assert not readiness_file.exists()
        writer.touch_readiness(ready=True)
        assert readiness_file.exists()

    def test_touch_readiness_false_removes_file(self, tmp_path):
        writer = SentinelWriter(str(tmp_path))
        readiness_file = tmp_path / "readiness"

        # Create it first
        writer.touch_readiness(ready=True)
        assert readiness_file.exists()

        # Remove it
        writer.touch_readiness(ready=False)
        assert not readiness_file.exists()

    def test_touch_readiness_false_no_file(self, tmp_path):
        """Should not error if file doesn't exist."""
        writer = SentinelWriter(str(tmp_path))
        writer.touch_readiness(ready=False)  # No error

    def test_cleanup(self, tmp_path):
        writer = SentinelWriter(str(tmp_path))
        writer.touch_liveness()
        writer.touch_readiness(ready=True)

        assert (tmp_path / "liveness").exists()
        assert (tmp_path / "readiness").exists()

        writer.cleanup()
        assert not (tmp_path / "liveness").exists()
        assert not (tmp_path / "readiness").exists()

    def test_cleanup_no_files(self, tmp_path):
        """Should not error if files don't exist."""
        writer = SentinelWriter(str(tmp_path))
        writer.cleanup()  # No error


class TestAgentMetrics:
    def test_initial_state(self):
        m = AgentMetrics()
        assert m.requests_processed == 0
        assert m.requests_failed == 0
        assert m.heartbeats_sent == 0
        assert m.workspace_syncs == 0
        assert m.errors == []

    def test_uptime(self):
        m = AgentMetrics()
        assert m.uptime_seconds >= 0
        time.sleep(0.01)
        assert m.uptime_seconds > 0

    def test_record_request_processed(self):
        m = AgentMetrics()
        m.record_request_processed()
        m.record_request_processed()
        assert m.requests_processed == 2

    def test_record_request_failed(self):
        m = AgentMetrics()
        m.record_request_failed("error 1")
        m.record_request_failed("error 2")
        assert m.requests_failed == 2
        assert "error 1" in m.errors
        assert "error 2" in m.errors

    def test_errors_ring_buffer(self):
        m = AgentMetrics()
        for i in range(15):
            m.record_request_failed(f"error {i}")
        # Only last 10 should be kept
        assert len(m.errors) == 10
        assert m.errors[0] == "error 5"
        assert m.errors[-1] == "error 14"

    def test_record_heartbeat(self):
        m = AgentMetrics()
        m.record_heartbeat()
        assert m.heartbeats_sent == 1

    def test_record_workspace_sync(self):
        m = AgentMetrics()
        m.record_workspace_sync()
        assert m.workspace_syncs == 1

    def test_maybe_log_metrics_respects_interval(self, caplog):
        m = AgentMetrics(log_interval_seconds=3600)  # 1 hour
        with caplog.at_level("INFO", logger="dagster-agent.observability"):
            m.maybe_log_metrics(running_locations=["loc1"], ready=True)
        # Should not have logged (interval not elapsed)
        assert "Agent metrics" not in caplog.text

    def test_maybe_log_metrics_logs_after_interval(self, caplog):
        m = AgentMetrics(log_interval_seconds=0.01)  # Very short interval
        time.sleep(0.02)
        with caplog.at_level("INFO", logger="dagster-agent.observability"):
            m.maybe_log_metrics(running_locations=["loc1", "loc2"], ready=True)
        assert "Agent metrics" in caplog.text
        assert "requests_ok=0" in caplog.text
        assert "locations=2" in caplog.text

    def test_maybe_log_metrics_includes_errors(self, caplog):
        m = AgentMetrics(log_interval_seconds=0.01)
        m.record_request_failed("something broke")
        time.sleep(0.02)
        with caplog.at_level("INFO", logger="dagster-agent.observability"):
            m.maybe_log_metrics(running_locations=[], ready=False)
        assert "Recent errors" in caplog.text
        assert "something broke" in caplog.text


class TestCreateSentinelWriter:
    def test_returns_writer_when_dir_set(self, tmp_path):
        writer = create_sentinel_writer(str(tmp_path))
        assert isinstance(writer, SentinelWriter)

    def test_returns_none_when_dir_not_set(self):
        writer = create_sentinel_writer(None)
        assert writer is None
