"""Core data types for the Dagster agent.

Defines the API request types, heartbeat structures, and deployment
data records used in communication between the agent and webserver.
"""

from __future__ import annotations

from enum import Enum, unique
from typing import Any


@unique
class DagsterAgentApi(str, Enum):
    """Types of work requests that agents can process."""

    LAUNCH_RUN = "LAUNCH_RUN"
    TERMINATE_RUN = "TERMINATE_RUN"
    GET_EXTERNAL_EXECUTION_PLAN = "GET_EXTERNAL_EXECUTION_PLAN"
    GET_EXTERNAL_SCHEDULE_EXECUTION_DATA = "GET_EXTERNAL_SCHEDULE_EXECUTION_DATA"
    GET_EXTERNAL_SENSOR_EXECUTION_DATA = "GET_EXTERNAL_SENSOR_EXECUTION_DATA"
    GET_EXTERNAL_PARTITION_CONFIG = "GET_EXTERNAL_PARTITION_CONFIG"
    GET_EXTERNAL_PARTITION_NAMES = "GET_EXTERNAL_PARTITION_NAMES"
    GET_EXTERNAL_PARTITION_TAGS = "GET_EXTERNAL_PARTITION_TAGS"
    PING_LOCATION = "PING_LOCATION"
    CHECK_FOR_WORKSPACE_UPDATES = "CHECK_FOR_WORKSPACE_UPDATES"


class AgentRequest:
    """A work request popped from the queue by an agent."""

    def __init__(
        self,
        request_id: str,
        request_api: DagsterAgentApi | str,
        request_body: str,
        location_name: str | None = None,
        labels: dict[str, str] | None = None,
    ):
        self.request_id = request_id
        self.request_api = (
            DagsterAgentApi(request_api)
            if isinstance(request_api, str)
            else request_api
        )
        self.request_body = request_body
        self.location_name = location_name
        self.labels = labels

    def __repr__(self) -> str:
        return (
            f"AgentRequest(id={self.request_id!r}, api={self.request_api.value!r}, "
            f"location={self.location_name!r})"
        )


class AgentHeartbeat:
    """Health report submitted by an agent to the webserver."""

    def __init__(
        self,
        agent_id: str,
        agent_type: str | None = None,
        labels: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None,
        errors: list[str] | None = None,
        code_server_heartbeats: dict[str, Any] | None = None,
    ):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.labels = labels
        self.metadata = metadata
        self.errors = errors
        self.code_server_heartbeats = code_server_heartbeats

    def to_dict(self) -> dict[str, Any]:
        """Serialize for REST API submission."""
        return {
            "agent_id": self.agent_id,
            "agent_type": self.agent_type,
            "labels": self.labels,
            "metadata": self.metadata,
            "errors": self.errors,
            "code_server_heartbeats": self.code_server_heartbeats,
        }


class CodeLocationDeployData:
    """Deployment configuration for a code location.

    Describes how the agent should start the gRPC server process
    for a given code location.
    """

    def __init__(
        self,
        python_file: str | None = None,
        module_name: str | None = None,
        working_directory: str | None = None,
        executable_path: str | None = None,
        attribute: str | None = None,
        image: str | None = None,  # For k8s/docker launchers
    ):
        self.python_file = python_file
        self.module_name = module_name
        self.working_directory = working_directory
        self.executable_path = executable_path
        self.attribute = attribute
        self.image = image

    def to_code_pointer(self):
        """Convert to a Dagster CodePointer if enough info is present."""
        if not (self.python_file or self.module_name):
            return None

        from dagster._core.code_pointer import FileCodePointer, ModuleCodePointer

        if self.python_file:
            return FileCodePointer(
                file_path=self.python_file,
                working_directory=self.working_directory,
                python_executable=self.executable_path,
                module_name=None,
                attribute=self.attribute,
            )
        elif self.module_name:
            return ModuleCodePointer(
                module_name=self.module_name,
                working_directory=self.working_directory,
                python_executable=self.executable_path,
                attribute=self.attribute,
            )
        return None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CodeLocationDeployData:
        """Deserialize from a deploy_metadata JSON object."""
        return cls(
            python_file=data.get("python_file"),
            module_name=data.get("module_name"),
            working_directory=data.get("working_directory"),
            executable_path=data.get("executable_path"),
            attribute=data.get("attribute"),
            image=data.get("image"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a deploy_metadata JSON object."""
        result: dict[str, Any] = {}
        if self.python_file:
            result["python_file"] = self.python_file
        if self.module_name:
            result["module_name"] = self.module_name
        if self.working_directory:
            result["working_directory"] = self.working_directory
        if self.executable_path:
            result["executable_path"] = self.executable_path
        if self.attribute:
            result["attribute"] = self.attribute
        if self.image:
            result["image"] = self.image
        return result
