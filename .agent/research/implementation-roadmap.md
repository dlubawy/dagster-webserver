# Implementation Roadmap — Phased Plan for DagsterAgent

## Overview

This document outlines a phased implementation plan for creating the `dagster-agent` system. Each phase delivers a working increment that can be tested and deployed independently.

______________________________________________________________________

## Phase 0: Foundations (Weeks 1-2)

### Objective

Set up project structure, define core data types, and establish the webserver database schema.

### Tasks

#### 0.1 Project Structure

```
dagster-agent/                  # New package (can be in this repo or separate)
├── dagster_agent/
│   ├── __init__.py
│   ├── cli.py                  # dagster-agent CLI
│   ├── agent.py                # DagsterAgent main class
│   ├── instance.py             # DagsterAgentInstance
│   ├── user_code_launcher/
│   │   ├── __init__.py
│   │   ├── base.py             # DagsterUserCodeLauncher (abstract)
│   │   ├── process.py          # ProcessUserCodeLauncher
│   │   └── utils.py
│   └── api/
│       ├── __init__.py
│       ├── types.py            # Request/response types (OSS version of DagsterCloudApi)
│       └── queries.py          # GraphQL queries for webserver
├── tests/
└── pyproject.toml
```

#### 0.2 Webserver Database Schema

Create Alembic migration `003_create_agent_tables.py`:

- `agent_heartbeats` table
- `agent_requests` table
- `code_location_definitions` table
- `code_location_snapshots` table

Add corresponding SQLAlchemy models in `dagster_webserver/database/models.py`.

#### 0.3 Core Data Types

Define OSS-compatible versions of Cloud types:

```python
# dagster_agent/api/types.py

from enum import Enum
from typing import Any

class DagsterAgentApi(str, Enum):
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


@record
class AgentRequest:
    request_id: str
    request_api: DagsterAgentApi
    request_body: str           # Serialized args
    location_name: str | None   # Target code location

@record
class AgentHeartbeat:
    agent_id: str
    agent_type: str | None
    timestamp: float
    metadata: dict[str, Any] | None
    errors: list[str] | None
    code_server_heartbeats: list[str] | None
```

#### 0.4 Deliverables

- [ ] Project structure created
- [ ] Database migration applied
- [ ] Core data types defined and tested
- [ ] `pyproject.toml` with dependencies

______________________________________________________________________

## Phase 1: Webserver Agent APIs (Weeks 3-4)

### Objective

Implement the webserver-side GraphQL mutations and REST endpoints that agents need.

### Tasks

#### 1.1 GraphQL Schema Extensions

Add agent resolvers to `dagster_webserver/graphql.py` (or new `agent_graphql.py`):

- `Query.agentWorkspaceEntries(agentLabels, locationNames)` — Return code locations
- `Mutation.popAgentRequests(agentId, agentLabels, limit)` — Pop work items from queue
- `Mutation.submitAgentHeartbeat(...)` — Record agent heartbeat
- `Mutation.uploadCodeLocationData(...)` — Store loaded repository data
- `Mutation.enqueueAgentRequest(...)` — Add work to the queue (for daemon/UI)

#### 1.2 REST Endpoints

Create `dagster_webserver/api/agent.py`:

```python
@router.get("/check-snapshot")
async def check_snapshot(location_name, snapshot_type, sha1, size):
    """Check if snapshot already exists."""

@router.post("/upload-snapshot")
async def upload_snapshot(location_name, snapshot_type, data):
    """Upload a serialized snapshot."""

@router.get("/workspace-entries")
async def get_workspace_entries(agent_labels: list[str]):
    """REST alternative for workspace entry queries."""
```

Mount the router in `dagster_webserver/app.py`:

```python
from dagster_webserver.api.agent import router as agent_router
app.include_router(agent_router, prefix="/api/agent")
```

#### 1.3 Code Location CRUD

Add admin portal integration for managing code locations:

- Create/edit/remove code location definitions.
- View agent assignments and health status.

#### 1.4 Request Response Tracking

Extend `agent_requests` table or add a `request_responses` table to store agent responses for request-reply pattern used by the daemon.

#### 1.5 Deliverables

- [ ] All GraphQL mutations working
- [ ] REST endpoints tested
- [ ] Code location CRUD via admin portal
- [ ] Integration tests with mock agent

______________________________________________________________________

## Phase 2: DagsterAgentInstance (Weeks 5-6)

### Objective

Create the instance class that agents use to connect to the webserver.

### Tasks

#### 2.1 Instance Class

```python
# dagster_agent/instance.py

class DagsterAgentInstance(DagsterInstance):
    """Instance for dagster-agent processes."""

    def __init__(self, *args, agent: dict, user_code_launcher=None, **kwargs):
        ...

    @property
    def webserver_url(self) -> str: ...

    @property
    def graphql_client(self): ...

    @property
    def user_code_launcher(self) -> DagsterUserCodeLauncher: ...

    @classmethod
    def config_schema(cls): ...

    @staticmethod
    def get(): ...
```

#### 2.2 Configuration Schema

Define the full `dagster.yaml` schema as documented in `agent-instance-design.md`.

#### 2.3 Storage Defaults

Configure storage backends to point at shared database.

#### 2.4 Deliverables

- [ ] `DagsterAgentInstance` class with all properties
- [ ] Configuration loading from `dagster.yaml`
- [ ] GraphQL client creation and testing
- [ ] Instance can be created via `DagsterAgentInstance.get()`

______________________________________________________________________

## Phase 3: User Code Launcher — Process (Weeks 7-8)

### Objective

Implement the abstract base class and `ProcessUserCodeLauncher`.

### Tasks

#### 3.1 Abstract Base Class

```python
# dagster_agent/user_code_launcher/base.py

class DagsterUserCodeLauncher(AbstractContextManager, Generic[ServerHandle]):
    """Abstract base for user code launchers."""

    # Core state
    _desired_entries: dict[DeploymentAndLocation, UserCodeLauncherEntry]
    _actual_entries: dict[DeploymentAndLocation, UserCodeLauncherEntry]
    _grpc_servers: dict[DeploymentAndLocation, DagsterGrpcServer | SerializableErrorInfo]

    # Abstract methods
    @property
    @abstractmethod
    def user_code_deployment_type(self) -> str: ...

    @abstractmethod
    def _start_new_server_spinup(self, deployment, location, entry): ...

    @abstractmethod
    async def _wait_for_new_server_ready(self, deployment, location, entry, handle, endpoint): ...

    @abstractmethod
    def _remove_server_handle(self, handle): ...

    @abstractmethod
    def run_launcher(self) -> RunLauncher: ...

    # Concrete methods (shared across all launchers)
    def start(self): ...
    def reconcile(self): ...
    def update_grpc_metadata(self, desired_map, **kwargs): ...
    def _reconcile_thread(self, shutdown_event): ...
```

#### 3.2 ProcessUserCodeLauncher

Implement the simplest launcher — local subprocesses:

```python
# dagster_agent/user_code_launcher/process.py

class ProcessUserCodeLauncher(DagsterUserCodeLauncher[int], ConfigurableClass):
    """Spawn local gRPC server subprocesses."""
```

Reuse code from `dagster_cloud/workspace/user_code_launcher/process.py` with Cloud-specific imports removed.

#### 3.3 Reconcile Loop

Implement the core reconcile logic:

- Diff desired vs actual entries.
- Add/update/remove servers.
- Health checking and recovery.

#### 3.4 Deliverables

- [ ] Abstract base class with all shared logic
- [ ] `ProcessUserCodeLauncher` fully working
- [ ] Reconcile loop tested with mock locations
- [ ] Server lifecycle (start, health check, stop) verified

______________________________________________________________________

## Phase 4: DagsterAgent Main Loop (Weeks 9-10)

### Objective

Implement the agent process that ties everything together.

### Tasks

#### 4.1 Agent Class

```python
# dagster_agent/agent.py

class DagsterAgent:
    def __init__(self, instance: DagsterAgentInstance):
        self._instance = instance
        self._executor = ThreadPoolExecutor(...)
        self._pending_requests: list[dict] = []
        self._ready_requests: list[dict] = []

    def run_loop(self, user_code_launcher):
        """Main event loop."""
        while True:
            self._poll_workspace(user_code_launcher)
            for error in self.run_iteration(user_code_launcher):
                log_error(error)
            self._send_heartbeat()
            time.sleep(0.5)

    def run_iteration(self, user_code_launcher):
        """Single iteration: pop requests, classify, dispatch."""
        # Pop from queue
        requests = self._instance.graphql_client.execute(POP_REQUESTS_QUERY, ...)
        self._pending_requests.extend(requests)

        # Classify pending → ready
        for req in self._pending_requests:
            if user_code_launcher.has_grpc_endpoint(...):
                self._ready_requests.append(req)

        # Dispatch to thread pool
        for req in self._ready_requests:
            future = self._executor.submit(self._process_request, req, ...)

        # Collect completed futures
        ...

    def _process_request(self, json_request, user_code_launcher):
        """Handle a single API request."""
        request = deserialize_value(json_request["requestBody"], AgentRequest)
        response = self._handle_api_request(request, user_code_launcher)
        self._upload_response(json_request["requestId"], response)

    def _handle_api_request(self, request, user_code_launcher):
        """Route to appropriate handler based on request API."""
        if request.request_api == DagsterAgentApi.LAUNCH_RUN:
            return self._handle_launch_run(request, user_code_launcher)
        elif request.request_api == DagsterAgentApi.TERMINATE_RUN:
            return self._handle_terminate_run(request, user_code_launcher)
        # ... etc
```

#### 4.2 CLI

```python
# dagster_agent/cli.py

@click.command()
@click.option("--dagster-home", type=click.Path(), default=None)
@click.option("--log-level", default="info")
def run(dagster_home, log_level):
    """Run the Dagster agent."""
    if dagster_home:
        os.environ["DAGSTER_HOME"] = str(dagster_home)

    with DagsterAgentInstance.get() as instance:
        launcher = instance.user_code_launcher
        launcher.start()

        with DagsterAgent(instance) as agent:
            agent.run_loop(launcher)
```

Entry point in `pyproject.toml`:

```toml
[project.scripts]
dagster-agent = "dagster_agent.cli:run"
```

#### 4.3 Deliverables

- [ ] `DagsterAgent` class with main loop
- [ ] CLI entry point (`dagster-agent run`)
- [ ] Request processing for all API types
- [ ] End-to-end test: agent starts, loads location, processes request

______________________________________________________________________

## Phase 5: K8s and Docker Launchers (Weeks 11-12)

### Objective

Implement `K8sUserCodeLauncher` and `DockerUserCodeLauncher`.

### Tasks

#### 5.1 K8sUserCodeLauncher

Port from `dagster_cloud/workspace/kubernetes/launcher.py`:

- Deploy code locations as Kubernetes Deployments + Services.
- Use standard k8s Python client.
- Configure resource limits, image pull policy, etc.

```python
# dagster_agent/user_code_launcher/k8s.py

class K8sUserCodeLauncher(DagsterUserCodeLauncher[K8sHandle], ConfigurableClass):
    """Deploy code locations as Kubernetes Deployments."""
```

#### 5.2 DockerUserCodeLauncher

Port from `dagster_cloud/workspace/docker/__init__.py`:

- Run code locations as Docker containers.
- Port mapping for gRPC access.

```python
# dagster_agent/user_code_launcher/docker.py

class DockerUserCodeLauncher(DagsterUserCodeLauncher[DockerContainer], ConfigurableClass):
    """Run code locations as Docker containers."""
```

#### 5.3 Deliverables

- [ ] `K8sUserCodeLauncher` tested on k8s cluster
- [ ] `DockerUserCodeLauncher` tested with Docker daemon
- [ ] Both launchers pass reconcile loop tests

______________________________________________________________________

## Phase 6: Daemon Integration (Weeks 13-14)

### Objective

Enable the daemon to work with agents through the request queue.

### Tasks

#### 6.1 Hybrid Mode (Phase 1 of daemon changes)

Create `AgentQueuedRunCoordinatorDaemon`:

- Dequeues runs from the database.
- Enqueues `LAUNCH_RUN` requests to the agent queue instead of launching directly.

```python
# dagster_agent/daemon/run_coordinator.py

class AgentQueuedRunCoordinatorDaemon(QueuedRunCoordinatorDaemon):
    def dequeue_runs(self, instance, ...):
        runs = get_queued_runs(instance)
        for run in runs:
            enqueue_agent_request(
                request_api="LAUNCH_RUN",
                request_body=serialize_value(LaunchRunArgs(dagster_run=run)),
                location_name=get_location_name(run),
            )
```

#### 6.2 Agent Request Client

Create a client for the daemon to make synchronous requests through the agent queue:

```python
# dagster_agent/daemon/client.py

class AgentRequestClient:
    async def request(self, api, body, location, timeout=120):
        """Submit request and poll for response."""
```

#### 6.3 Deliverables

- [ ] `AgentQueuedRunCoordinatorDaemon` working
- [ ] Daemon can enqueue runs that agents process
- [ ] End-to-end test: schedule fires → daemon enqueues → agent launches run

______________________________________________________________________

## Phase 7: Production Hardening (Weeks 15-16)

### Objective

Add production-ready features and polish.

### Tasks

#### 7.1 Authentication

Implement agent-to-webserver authentication:

- API token validation in webserver middleware.
- Token configuration in agent `dagster.yaml`.

#### 7.2 Monitoring and Observability

- Agent metrics endpoint (Prometheus-compatible).
- Structured logging throughout.
- Health check endpoints for k8s probes.

#### 7.3 Error Handling and Recovery

- Stale request re-queuing (expired requests).
- Agent crash recovery (unassigned processing requests → pending).
- Code server crash detection and auto-restart.

#### 7.4 Documentation

- User guide: deploying agents with each launcher type.
- Architecture documentation.
- Configuration reference.

#### 7.5 Testing

- Integration tests with real webserver + agent.
- Load testing with multiple agents and locations.
- Chaos testing (agent crash, server crash, network partition).

#### 7.6 Deliverables

- [ ] Authentication working
- [ ] Metrics and health checks
- [ ] Comprehensive test suite
- [ ] Documentation complete
- [ ] Production deployment guide

______________________________________________________________________

## Dependencies Between Phases

```
Phase 0 (Foundations)
    │
    ├──→ Phase 1 (Webserver APIs) ──┐
    │                                │
    ├──→ Phase 2 (Instance) ─────────┼──→ Phase 4 (Agent Main Loop)
    │                                │         │
    ├──→ Phase 3 (Process Launcher) ─┘         │
                                               ▼
                                         Phase 5 (K8s/Docker Launchers)
                                               │
                                               ▼
                                         Phase 6 (Daemon Integration)
                                               │
                                               ▼
                                         Phase 7 (Production Hardening)
```

**Critical path**: Phase 0 → Phase 1 → Phase 2 → Phase 3 → Phase 4

Phases 5, 6, and 7 can proceed in parallel after their dependencies are met.

______________________________________________________________________

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
| ----------------------------------------------------------- | ---------- | ------ | -------------------------------------------------------------- |
| dagster-graphql schema extension breaks on upstream updates | Medium | High | Pin to specific dagster version; use monkey-patching carefully |
| Request queue contention under high load | Low | Medium | Use database-level optimistic locking; add rate limiting |
| Code server startup failures in production | Medium | Medium | Robust reconcile loop with exponential backoff |
| Agent crash leaves runs in limbo | Low | High | Stale request re-queuing + monitoring alerts |
| Large snapshot uploads timeout | Medium | Low | Chunked uploads + presigned URLs if needed |

______________________________________________________________________

## Success Criteria

### MVP (After Phase 4)

- [ ] `dagster-agent run` starts and connects to webserver.
- [ ] Agent loads code locations from webserver.
- [ ] Agent processes `LAUNCH_RUN` requests successfully.
- [ ] Process launcher works for local development.

### Full Release (After Phase 7)

- [ ] All three launchers (process, k8s, docker) working.
- [ ] Daemon integrates with agent queue.
- [ ] Production deployment documented and tested.
- [ ] Monitoring, auth, and error handling in place.
