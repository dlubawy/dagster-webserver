# DagsterCloudAgent — Detailed Analysis

## Source Location

`/tmp/github/dagster-io/dagster/python_modules/dagster-cloud/dagster_cloud/agent/dagster_cloud_agent.py`\
`/tmp/github/dagster-io/dagster/python_modules/dagster-cloud/dagster_cloud/agent/cli/__init__.py`

______________________________________________________________________

## 1. CLI Entry Point (`dagster-cloud agent run`)

### Two Modes of Operation

The CLI supports two modes:

**Mode A — Ephemeral (token + deployment from CLI flags):**

```bash
dagster-cloud agent run \
  --agent-token <TOKEN> \
  --deployment prod \
  --user-code-launcher dagster_cloud.workspace.kubernetes.K8sUserCodeLauncher
```

Creates a temporary `dagster.yaml` in a temp directory with the provided config.

**Mode B — File-based (DAGSTER_HOME):**

```bash
DAGSTER_HOME=/path/to/config dagster-cloud agent run
```

Reads full configuration from `dagster.yaml`.

### CLI Options

| Option | Purpose |
| ------------------------------- | ---------------------------------------------- |
| `--agent-token` / `-a` | Auth token for Dagster Cloud API |
| `--deployment` / `-d` | Deployment name to serve |
| `--agent-label` / `-l` | Optional agent label (for multi-agent routing) |
| `--user-code-launcher` | Fully qualified class name of launcher |
| `--config` | JSON instance config override |
| `--user-code-launcher-config` | JSON launcher config override |
| `--agent-logging-config-path` | YAML logging config file |
| `--agent-logging-config-string` | Inline JSON logging config |

### Startup Flow

```python
# 1. Create DagsterCloudAgentInstance from dagster.yaml
with DagsterCloudAgentInstance.get() as inst:
    # 2. Start the user code launcher (spins up reconcile thread)
    user_code_launcher = instance.user_code_launcher
    user_code_launcher.start()

    # 3. Create and run the agent
    with DagsterCloudAgent(instance) as agent:
        agent.run_loop(user_code_launcher, agent_uuid=instance.instance_uuid)
```

______________________________________________________________________

## 2. `DagsterCloudAgent` — Main Loop

### Constructor

```python
class DagsterCloudAgent:
    def __init__(
        self,
        instance: DagsterCloudAgentInstance,
        pending_requests_limit: int = 100,
    ):
        # ThreadPoolExecutor for parallel request processing
        self._executor = FuturesAwareThreadPoolExecutor(
            max_workers=AGENT_MAX_THREADPOOL_WORKERS  # CPU_COUNT * 10
        )
        # Request tracking
        self._request_ids_to_futures: dict[str, Future] = {}
        # Pending/ready request queues
        self._pending_requests: list[dict] = []       # waiting for code server
        self._ready_requests: list[dict] = []         # ready to dispatch
        # Deployment tracking
        self._active_deployments: set[tuple[str, bool]] = set()  # (name, is_branch)
```

### `run_loop()` — The Heartbeat

```python
def run_loop(self, user_code_launcher, agent_uuid):
    # One-time setup
    if no_deployment_configured:
        fetch_deployment_name_from_server()

    self._check_initial_deployment_names()

    # Initial workspace load
    self._check_update_workspace(user_code_launcher, upload_outdated=True)

    while True:
        try:
            for error in self.run_iteration(user_code_launcher):
                log_error(error)
        except Exception:
            log_exception()

        # Check interrupts (Ctrl+C handling)
        with raise_interrupts_as(KeyboardInterrupt): pass

        if user_code_launcher.ready_to_serve_requests:
            self._check_add_heartbeat(agent_uuid, interval=30)

        # Detect stuck reconcile thread
        self._check_for_long_running_reconcile(user_code_launcher)

        # Periodic workspace check (every 5 seconds)
        self._check_update_workspace(user_code_launcher, upload_outdated=False)

        # Write liveness sentinel (for k8s readiness probes)
        self._write_liveness_sentinel_if_overdue(sentinel_dir)

        with raise_interrupts_as(KeyboardInterrupt): pass
        time.sleep(0.5)  # configurable via env var
```

### `run_iteration()` — Per-Cycle Work

Each iteration:

1. **Pop requests from queue** (if below pending limit):

   ```python
   result = self._instance.organization_scoped_graphql_client().execute(
       GET_USER_CLOUD_REQUESTS_QUERY,  # popUserCloudAgentRequests mutation
       {"forBranchDeployments": ..., "forFullDeployments": ...}
   )
   self._pending_requests.extend(result["data"]["userCloudAgent"]["popUserCloudAgentRequests"])
   ```

1. **Classify pending requests** as ready or still waiting:

   - If the request's code location has a gRPC endpoint → move to `_ready_requests`
   - Otherwise → stays in `_pending_requests` (next workspace update will spin up the server)

1. **Dispatch ready requests** to thread pool:

   ```python
   for json_request in self._ready_requests:
       future = self._executor.submit(
           self._process_api_request, json_request, user_code_launcher, timestamp
       )
       self._request_ids_to_futures[request_id] = future
   ```

1. **Collect completed futures** and yield errors.

### `_check_update_workspace()` — Workspace Reconciliation

Called every 5 seconds. Delegates to `_query_for_workspace_updates()`:

1. Query `WORKSPACE_ENTRIES_QUERY` from Dagster Cloud GraphQL API.
1. For each deployment, get workspace entries with:
   - `locationName`
   - `serializedDeploymentMetadata` (CodeLocationDeployData)
   - `hasOutdatedData`, `hasLoadError`, `metadataTimestamp`
1. Feed the desired state to `user_code_launcher.update_grpc_metadata()`.

The user code launcher's reconcile thread then:

- Spins up new servers for locations not yet running.
- Tears down servers for removed locations.
- Restarts servers when metadata changes.
- Uploads loaded repository manifests back to Dagster Cloud.

______________________________________________________________________

## 3. Request Processing (`_handle_api_request`)

The agent handles these `DagsterCloudApi` request types:

| API | Purpose | Implementation |
| -------------------------------------- | ------------------------------ | ------------------------------------------------------------------------------- |
| `LAUNCH_RUN` | Launch a Dagster run | Get gRPC client → call `launch_run_from_grpc_client()` or isolated run launcher |
| `TERMINATE_RUN` | Cancel a running execution | Report canceling + send gRPC `CancelExecutionRequest` |
| `GET_EXTERNAL_EXECUTION_PLAN` | Build execution plan snapshot | gRPC call to code server |
| `GET_EXTERNAL_SCHEDULE_EXECUTION_DATA` | Evaluate schedule | gRPC call with instance ref |
| `GET_EXTERNAL_SENSOR_EXECUTION_DATA` | Evaluate sensor | gRPC call with instance ref |
| `GET_EXTERNAL_PARTITION_*` | Partition queries | Various gRPC calls |
| `PING_LOCATION` | Keep location alive (TTL bump) | No-op, just updates TTL tracking |
| `REFRESH_COMPONENT_STATE` | Reload defs state | gRPC call to refresh component state |
| `CHECK_FOR_WORKSPACE_UPDATES` | Re-upload outdated locations | Trigger workspace re-upload |

### Isolated vs Non-Isolated Runs

```python
if is_isolated_run(run):
    # Run executes in its own container/pod (via the launcher's run_launcher)
    launcher.launch_run(LaunchRunContext(dagster_run=run, workspace=None))
else:
    # Run executes via the code location's gRPC server (in-process)
    server = user_code_launcher.get_grpc_server(deployment_name, location_name)
    launcher.launch_run_from_grpc_client(instance, run, server.server_endpoint.create_client())
```

______________________________________________________________________

## 4. Heartbeat Mechanism

Every 30 seconds (configurable), the agent sends a heartbeat:

```python
heartbeats = {
    deployment_name: AgentHeartbeat(
        timestamp=now.timestamp(),
        agent_id=agent_uuid,
        agent_label=self._instance.dagster_cloud_api_agent_label,
        agent_type=type(self._instance.user_code_launcher).__name__,
        metadata={
            "version": __version__,
            "utilization_metrics": {...},
            "allowed_full_deployment_locations": {...},
        },
        run_worker_statuses=...,
        code_server_heartbeats=...,
        agent_queues_config=self._instance.agent_queues_config,
    )
    for deployment_name in self._active_deployment_names
}

# Sent via GraphQL mutation
self._instance.organization_scoped_graphql_client().execute(
    ADD_AGENT_HEARTBEATS_MUTATION,
    {"serializedAgentHeartbeats": ..., "serializedErrors": ...}
)
```

### Heartbeat Contents

- Agent version and image tag.
- Utilization metrics (CPU/memory from cgroups, thread pool usage).
- Resource limits for the agent's deployment type.
- Run worker statuses per deployment.
- Code server heartbeats (per-location gRPC health).
- Error log (up to 25 recent errors).

______________________________________________________________________

## 5. GraphQL Queries Used by the Agent

All queries are in `dagster_cloud/agent/queries.py`:

### `GET_USER_CLOUD_REQUESTS_QUERY`

```graphql
mutation GetUserCloudRequests(
    $forBranchDeployments: Boolean
    $forFullDeployments: [String!]
    $limit: Int
    $agentQueues: [String]
) {
    userCloudAgent {
        popUserCloudAgentRequests(limit: $limit, ...) {
            requestId
            requestApi
            requestBody
            deploymentName
            isBranchDeployment
        }
    }
}
```

### `WORKSPACE_ENTRIES_QUERY`

```graphql
query WorkspaceEntries(
    $deploymentNames: [String!]!
    $includeAllServerlessDeployments: Boolean!
    $agentQueues: [String]
) {
    deployments(deploymentNames: $deploymentNames, ...) {
        deploymentName
        isBranchDeployment
        workspaceEntries(agentQueues: $agentQueues) {
            locationName
            serializedDeploymentMetadata
            hasOutdatedData
            hasLoadError
            metadataTimestamp
        }
    }
}
```

### `ADD_AGENT_HEARTBEATS_MUTATION`

```graphql
mutation AddAgentHeartbeats(
    $serializedAgentHeartbeats: [AgentHeartbeatInput!]
    $serializedErrors: [String!]
) {
    userCloudAgent {
        addAgentHeartbeats(...) { ok }
    }
}
```

### `DEPLOYMENTS_QUERY` / `DEPLOYMENT_INFO_QUERY`

Used for initial deployment name resolution.

### `GET_AGENTS_QUERY`

Used to discover active agent IDs (for cleaning up orphaned code servers).

______________________________________________________________________

## 6. DagsterCloudAgentInstance — Instance Configuration

Source: `dagster_cloud/instance/__init__.py`

### Key Properties

| Property | Purpose |
| ----------------------------------------------- | --------------------------------------------- |
| `dagster_cloud_url` | Base URL derived from agent token |
| `dagster_cloud_graphql_url` | `{cloud_url}/graphql` |
| `dagster_cloud_upload_workspace_entry_url` | Upload loaded location data |
| `dagster_cloud_upload_api_response_url` | Upload API responses |
| `dagster_cloud_check_snapshot_url` | Check if snapshot already stored |
| `dagster_cloud_confirm_upload_url` | Confirm presigned URL upload |
| `dagster_cloud_code_location_update_result_url` | Report code location updates |
| `dagster_cloud_agent_token` | Auth token from config |
| `deployment_names` | Which deployments this agent serves |
| `includes_branch_deployments` | Whether to serve branch deployments |
| `agent_queues_config` | Queue routing configuration |
| `is_using_isolated_agents` | Whether agents can't see each other's servers |

### Config Schema

```python
@classmethod
def config_schema(cls):
    return {
        "dagster_cloud_api": Field(dagster_cloud_api_config(), is_required=True),
        "user_code_launcher": config_field_for_configurable_class(),
        "isolated_agents": Field({...}, is_required=False),
        "agent_queues": Field({...}, is_required=False),
        "allowed_full_deployment_locations": Field(Map(String, Array(String)), ...),
        "allowed_branch_deployment_locations": Field(Array(String), ...),
    }
```

### `dagster_cloud_api_config()` Schema

From `dagster_cloud/storage/client.py`:

- `agent_token` (required) — Auth token.
- `deployment` / `deployments` — Deployment names to serve.
- `url` — Override the auto-derived URL.
- `branch_deployments` — Whether to serve branch deployments.
- `all_serverless_deployments` — Serve all serverless deployments.
- `agent_label` — Label for this agent instance.
- `retries`, `backoff_factor`, `timeout` — HTTP retry config.
- `proxies` — HTTP proxy settings.

______________________________________________________________________

## 7. Key Observations for OSS Port

### What Must Be Replicated

1. **Agent main loop** (`run_loop` + `run_iteration`) — The polling pattern is generic and can be reused directly.
1. **Request classification** (pending → ready) — Generic logic, no Cloud-specific code.
1. **Thread pool dispatch** — Generic concurrency pattern.
1. **Heartbeat mechanism** — Needs to target our webserver instead of Cloud.

### What Must Be Replaced

1. **GraphQL queries/mutations** — All `dagster_cloud/agent/queries.py` queries need OSS equivalents on the webserver.
1. **DagsterCloudAgentInstance** → `DagsterAgentInstance` — New instance class connecting to our webserver.
1. **Code location upload** (`_update_workspace_entry`) — Instead of uploading to Cloud blob storage, report to webserver.
1. **Deployment/queue routing** — Simplified for OSS (no branch deployments, no multi-organization).

### What Can Be Reused As-Is

1. **DagsterCloudUserCodeLauncher base class** — The reconcile loop, server lifecycle management, and gRPC endpoint tracking are generic. May need minor adjustments.
1. **Process/Docker/K8s launchers** — These implement the abstract methods of the base launcher. Mostly reusable.
1. **ServerEndpoint / DagsterCloudGrpcServer** — Generic data structures for representing running servers.

### What Is Cloud-Specific (Remove or Abstract)

1. `DagsterCloudApi` enum and request/response types — Define our own equivalents.
1. Presigned URL upload flow — Use direct GraphQL/HTTP instead.
1. PEX metadata / multipex server support — Optional, can be dropped for initial implementation.
1. Branch deployment logic — Dagster+ specific feature.
1. Agent queue routing — Can be simplified or made optional.
1. Run worker monitoring thread — Depends on Cloud-specific run launcher health checks.
