"""Agent observability: sentinel files and periodic metrics logging.

Sentinel files are used by k8s liveness/readiness probes:
- ``{sentinel_dir}/liveness`` — written on every main loop iteration
- ``{sentinel_dir}/readiness`` — written when at least one location is ready

Metrics are logged periodically to provide operational visibility into
agent health (requests processed, errors, uptime, etc.).
"""

from __future__ import annotations

import logging
import os
import time

logger = logging.getLogger("dagster-agent.observability")


class SentinelWriter:
    """Writes liveness/readiness sentinel files for k8s probes.

    The files are touched (mtime updated) on each call rather than
    rewritten, so they can be monitored with ``stat -c %Y`` or similar.
    """

    def __init__(self, sentinel_dir: str):
        self._dir = sentinel_dir
        self._liveness_path = os.path.join(sentinel_dir, "liveness")
        self._readiness_path = os.path.join(sentinel_dir, "readiness")
        os.makedirs(sentinel_dir, exist_ok=True)

    def touch_liveness(self) -> None:
        """Update the liveness sentinel file (called every loop iteration)."""
        try:
            _touch_file(self._liveness_path)
        except OSError as exc:
            logger.warning("Failed to write liveness sentinel: %s", exc)

    def touch_readiness(self, ready: bool) -> None:
        """Update the readiness sentinel file.

        If ``ready`` is True, touch the file (probe passes).
        If ``ready`` is False, remove it (probe fails).
        """
        try:
            if ready:
                _touch_file(self._readiness_path)
            elif os.path.exists(self._readiness_path):
                os.unlink(self._readiness_path)
        except OSError as exc:
            logger.warning("Failed to update readiness sentinel: %s", exc)

    def cleanup(self) -> None:
        """Remove sentinel files (called on shutdown)."""
        for path in (self._liveness_path, self._readiness_path):
            try:
                if os.path.exists(path):
                    os.unlink(path)
            except OSError:
                pass


def _touch_file(path: str) -> None:
    """Create or update a file's mtime."""
    with open(path, "w") as f:
        f.write(f"{time.time():.6f}")


class AgentMetrics:
    """Tracks agent operational metrics for periodic logging.

    Metrics tracked:
    - Uptime (seconds since start)
    - Total requests processed / failed
    - Heartbeats sent
    - Workspace syncs completed
    - Current running locations count
    """

    def __init__(self, log_interval_seconds: float = 60.0):
        self._start_time = time.time()
        self._log_interval = log_interval_seconds
        self._last_log_time = time.time()

        # Counters
        self.requests_processed = 0
        self.requests_failed = 0
        self.heartbeats_sent = 0
        self.workspace_syncs = 0
        self.errors: list[str] = []  # Ring buffer of recent errors (max 10)

    @property
    def uptime_seconds(self) -> float:
        return time.time() - self._start_time

    def record_request_processed(self) -> None:
        self.requests_processed += 1

    def record_request_failed(self, error: str) -> None:
        self.requests_failed += 1
        self.errors.append(error)
        if len(self.errors) > 10:
            self.errors.pop(0)

    def record_heartbeat(self) -> None:
        self.heartbeats_sent += 1

    def record_workspace_sync(self) -> None:
        self.workspace_syncs += 1

    def maybe_log_metrics(self, running_locations: list[str], ready: bool) -> None:
        """Log metrics if the log interval has elapsed."""
        now = time.time()
        if now - self._last_log_time < self._log_interval:
            return
        self._last_log_time = now

        logger.info(
            "Agent metrics | uptime=%.0fs requests_ok=%d requests_failed=%d "
            "heartbeats=%d syncs=%d locations=%d ready=%s",
            self.uptime_seconds,
            self.requests_processed,
            self.requests_failed,
            self.heartbeats_sent,
            self.workspace_syncs,
            len(running_locations),
            ready,
        )

        if self.errors:
            logger.info(
                "Recent errors (%d): %s",
                len(self.errors),
                "; ".join(self.errors[-3:]),
            )


def create_sentinel_writer(sentinel_dir: str | None) -> SentinelWriter | None:
    """Create a SentinelWriter if sentinel_dir is configured, else None."""
    if sentinel_dir:
        return SentinelWriter(sentinel_dir)
    return None
