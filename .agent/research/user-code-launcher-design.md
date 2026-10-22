# User Code Launcher Design — OSS Abstraction

## Goal

Create a `DagsterUserCodeLauncher` base class and concrete implementations (process, k8s, docker) that manage the lifecycle of gRPC code servers. This replaces `DagsterCloudUserCodeLauncher` without Dagster Cloud dependencies.

______________________________________________________________________

## 1. Architecture Overview

```
DagsterAgent
    │
    ├─── DagsterUserCodeLauncher (base, abstract)
    │       │
    │       ├─── ProcessUserCodeLauncher   ← local subprocesses
    │       ├─── K8sUserCodeLauncher       ← Kubernetes Deployments
    │       └─── DockerUserCodeLauncher    ← Docker containers
    │
    └─── Reconcile Loop (background thread)
            │
            ├── desired state: from webserver workspace entries
            ├── actual state: currently running servers
            └── diff → add / update / remove servers
```

______________________________________________________________________

## 2. Base Class Design

### `DagsterUserCodeLauncher` (Abstract Base)

Modeled after `DagsterCloudUserCodeLauncher` but without Cloud-specific imports.

#### Core Data Structures

```python
# Key type for identifying a code server instance
DeploymentAndLocation = tuple[str, str]  # (deployment_name, location_name)

@record
class UserCodeLauncherEntry:
    """Desired state for a single code location."""
    code_location_deploy_data: CodeLocationDeployData  # how to launch the server
    update_timestamp: float                            # when was this last updated?

@record
class ServerEndpoint:
    """Network endpoint of a running gRPC server."""
    host: str
    port: int | None       # TCP port (for non-Unix-socket)
    socket: str | None     # Unix domain socket path
    metadata: list[tuple[str, str]] | None  # gRPC metadata headers

@record
class DagsterGrpcServer:
    """A running gRPC server for a code location."""
    server_handle: Any              # opaque handle (pid, k8s name, container id)
    server_endpoint: ServerEndpoint # how to connect
    code_location_deploy_data: CodeLocationDeployData  # what was deployed
```

#### Abstract Methods (Must Implement Per-Launcher)

| Method | Purpose |
| --------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `user_code_deployment_type` | Enum value identifying launcher type (PROCESS, K8S, DOCKER) |
| `requires_images` | Whether this launcher requires Docker images |
| `_start_new_server_spinup(deployment, location, entry)` | Create a new server process/deployment/container |
| `_wait_for_new_server_ready(deployment, location, entry, handle, endpoint)` | Block until server accepts gRPC connections |
| `_remove_server_handle(handle)` | Tear down the server (kill process, delete deployment, stop container) |
| `_get_standalone_dagster_server_handles_for_location(deployment, location)` | List running handles for a location |
| `_list_server_handles()` | All running server handles across all locations |
| `get_agent_id_for_server(handle)` | Which agent created this server (for cleanup coordination) |
| `get_server_create_timestamp(handle)` | When was the server created (for grace period) |
| `run_launcher()` | Return a `RunLauncher` for launching isolated runs |
| `get_code_server_resource_limits(deployment, location)` | Resource limits of the running server |

#### Concrete Methods (Shared Across All Launchers)

The base class provides:

1. **Reconcile loop** (`_reconcile_thread`) — Runs in a background thread, diffs desired vs actual state every 1 second.
1. **`update_grpc_metadata(desired_map)`** — Called by agent to set desired state from webserver.
1. **`reconcile()`** — Core reconciliation logic: compute diff, add/update/remove servers.
1. **Server health checking** — Ping running servers, detect failures, trigger recovery restarts.
1. **Graceful shutdown** — Wait for in-progress runs before tearing down servers.
1. **TTL management** — Track when each server was last queried; shut down idle servers.

______________________________________________________________________

## 3. ProcessUserCodeLauncher

### Purpose

Spawn local subprocesses running `dagster api grpc`. This is the simplest launcher and mirrors what the OSS daemon does via `GrpcServerRegistry`.

### Configuration

```yaml
user_code_launcher:
  module: dagster_agent.user_code_launcher.process
  class: ProcessUserCodeLauncher
  config:
    server_process_startup_timeout: 180 # seconds to wait for server ready
    server_ttl:
      enabled: true
      ttl_seconds: 3600
      max_servers: 25
```

### Implementation Details

```python
class ProcessUserCodeLauncher(DagsterUserCodeLauncher[int], ConfigurableClass):
    """int = PID of the subprocess"""

    # Map: PID → ProcessUserCodeEntry (process, client, heartbeat)
    _process_entries: dict[int, ProcessUserCodeEntry]

    # Map: (deployment, location) → set of PIDs
    _active_pids: dict[DeploymentAndLocation, set[int]]

    def _start_new_server_spinup(self, deployment, location, entry):
        metadata = entry.code_location_deploy_data

        # Find free port or create Unix socket
        if IS_WINDOWS:
            port = find_free_port()
            socket_path = None
        else:
            port = None
            socket_path = safe_tempfile_path_unmanaged()

        # Build command: dagster api grpc --port/--socket
        cmd = metadata.get_grpc_server_command() + [
            "--heartbeat",
            "--heartbeat-timeout", "60",
        ]

        # Launch subprocess
        server_process = open_ipc_subprocess(cmd, env=build_env(metadata, ...))
        pid = server_process.pid

        # Create gRPC client pointing at the server
        client = DagsterGrpcClient(port=port, socket=socket_path, host="localhost")

        # Start heartbeat thread to keep process alive
        heartbeat_event = threading.Event()
        heartbeat_thread = threading.Thread(
            target=client_heartbeat_thread, args=(client, heartbeat_event), daemon=True
        )
        heartbeat_thread.start()

        # Track the process
        self._process_entries[pid] = ProcessUserCodeEntry(
            server_process, client, heartbeat_event, heartbeat_thread
        )
        self._active_pids[(deployment, location)].add(pid)

        return DagsterGrpcServer(
            server_handle=pid,
            server_endpoint=ServerEndpoint("localhost", port, socket_path),
            code_location_deploy_data=metadata,
        )

    def _remove_server_handle(self, pid):
        entry = self._process_entries[pid]
        # Stop heartbeat → process will die from timeout
        entry.heartbeat_shutdown_event.set()
        entry.heartbeat_thread.join()
        del self._process_entries[pid]
        # Clean up PID tracking
        for pids in self._active_pids.values():
            pids.discard(pid)

    def run_launcher(self):
        return DefaultRunLauncher()  # Runs via gRPC server (non-isolated)
```

### Key Differences from Cloud Version

- No PEX/multipex server support (simplification for OSS).
- No `CloudProcessRunLauncher` — use standard `DefaultRunLauncher`.
- No container resource limit detection from cgroups.

______________________________________________________________________

## 4. K8sUserCodeLauncher

### Purpose

Deploy code locations as Kubernetes Deployments with associated Services. Each code location gets its own Deployment + Service pair.

### Configuration

```yaml
user_code_launcher:
  module: dagster_agent.user_code_launcher.k8s
  class: K8sUserCodeLauncher
  config:
    namespace: "dagster-code-locations"
    image_pull_policy: "Always"
    deployment_startup_timeout: 300
    labels:
      app: "dagster-agent"
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
      limits:
        cpu: "1000m"
        memory: "1Gi"
```

### Implementation Details

```python
class K8sUserCodeLauncher(DagsterUserCodeLauncher[K8sHandle], ConfigurableClass):
    """K8sHandle = (namespace, name, labels, creation_timestamp)"""

    def _start_new_server_spinup(self, deployment, location, entry):
        metadata = entry.code_location_deploy_data
        image = self._resolve_image(metadata)  # Required for K8s

        # Build unique resource name
        name = unique_k8s_resource_name(deployment, location)

        # Create Deployment
        deployment_obj = construct_code_location_deployment(
            namespace=self._namespace,
            name=name,
            image=image,
            env_vars=self._build_env(metadata, deployment),
            resources=self._resources,
            labels={**self._labels, "dagster/location": location},
            ...
        )
        self._k8s_apps_api_client().create_namespaced_deployment(
            namespace=self._namespace, body=deployment_obj
        )

        # Create Service (ClusterIP pointing at the Deployment)
        service_obj = construct_code_location_service(
            namespace=self._namespace,
            name=f"{name}-svc",
            port=get_code_server_port(),
            selector={"app": name},
        )
        self._k8s_core_api_client().create_namespaced_service(
            namespace=self._namespace, body=service_obj
        )

        # Return endpoint pointing at the Service
        return DagsterGrpcServer(
            server_handle=K8sHandle(self._namespace, name, {...}, time.time()),
            server_endpoint=ServerEndpoint(
                host=f"{name}-svc.{self._namespace}.svc.cluster.local",
                port=get_code_server_port(),
                socket=None,
            ),
            code_location_deploy_data=metadata,
        )

    def _remove_server_handle(self, handle: K8sHandle):
        # Delete Deployment (cascades to Pods)
        self._k8s_apps_api_client().delete_namespaced_deployment(
            namespace=handle.namespace, name=handle.name
        )
        # Delete Service
        self._k8s_core_api_client().delete_namespaced_service(
            namespace=handle.namespace, name=f"{handle.name}-svc"
        )

    def run_launcher(self):
        return CloudK8sRunLauncher(...)  # Launches runs as K8s Jobs
```

### Key Differences from Cloud Version

- No `instance_config_map` parameter (config comes from webserver URL).
- Simplified env var injection (no cloud context env vars).
- Can use standard `dagster-k8s` library helpers.

______________________________________________________________________

## 5. DockerUserCodeLauncher

### Purpose

Run code locations as Docker containers. Each location gets its own container with a mapped port for gRPC.

### Configuration

```yaml
user_code_launcher:
  module: dagster_agent.user_code_launcher.docker
  class: DockerUserCodeLauncher
  config:
    networks: ["dagster-network"]
    env_vars: ["DAGSTER_HOME=/opt/dagster"]
    container_kwargs:
      mem_limit: "1g"
      cpus: 1.0
```

### Implementation Details

```python
class DockerUserCodeLauncher(DagsterUserCodeLauncher[DockerContainer]):
    """DockerContainer wraps a docker.models.containers.Container"""

    def _start_new_server_spinup(self, deployment, location, entry):
        metadata = entry.code_location_deploy_data
        image = self._resolve_image(metadata)  # Required for Docker

        container_name = unique_docker_resource_name(deployment, location)

        client = docker_client_from_env()

        container = client.containers.create(
            image=image,
            name=container_name,
            detach=True,
            ports={get_code_server_port(): None},  # Auto-assign host port
            environment=self._build_env(metadata),
            command=metadata.get_grpc_server_command(),
            labels={
                GRPC_SERVER_LABEL: "1",
                AGENT_LABEL: self._instance.instance_uuid,
                SERVER_TIMESTAMP_LABEL: str(time.time()),
            },
        )
        container.start()

        # Get the mapped port
        host_port = container.attrs["NetworkSettings"]["Ports"][
            f"{get_code_server_port()}/tcp"
        ][0]["HostPort"]

        return DagsterGrpcServer(
            server_handle=DockerContainer(container),
            server_endpoint=ServerEndpoint("localhost", int(host_port), None),
            code_location_deploy_data=metadata,
        )

    def _remove_server_handle(self, handle: DockerContainer):
        handle.container.stop(timeout=30)
        handle.container.remove()

    def run_launcher(self):
        return DockerRunLauncher(...)  # Launches runs as Docker containers
```

______________________________________________________________________

## 6. Reconciliation Loop (Shared by All Launchers)

The base class `DagsterUserCodeLauncher` provides a reconcile thread that:

### Every ~1 Second:

```python
def reconcile(self):
    with self._metadata_lock:
        desired = self._desired_entries.copy()
        upload_locations = self._upload_locations.copy()
        self._upload_locations = set()

    if desired is None:
        return  # Wait for first metadata push

    # Periodic cleanup of orphaned servers (every N reconciles)
    if time_to_cleanup():
        self._graceful_cleanup_servers(include_own_servers=False)

    # Refresh actual entries from running servers
    if time_to_refresh():
        self._refresh_actual_entries(...)

    # Compute diff: desired vs actual
    diff = diff_serializable_namedtuple_map(
        desired, self._actual_entries,
        update_key_fn=lambda entry: entry.update_timestamp,
    )

    has_changes = diff.to_add or diff.to_update or diff.to_remove or upload_locations
    if not has_changes:
        return

    # --- ADD new servers ---
    for key in diff.to_add:
        server = self._start_new_server_spinup(*key, desired[key])
        self._wait_for_new_server_ready_and_upload(key, server, desired[key], should_upload=True)

    # --- UPDATE changed servers ---
    for key in diff.to_update:
        old_server = self._grpc_servers.get(key)
        new_server = self._start_new_server_spinup(*key, desired[key])
        self._wait_for_new_server_ready_and_upload(key, new_server, desired[key], should_upload=True)
        self._graceful_remove_server_handle(old_server.server_handle)

    # --- REMOVE deleted servers ---
    for key in diff.to_remove:
        server = self._grpc_servers.get(key)
        if server:
            self._graceful_remove_server_handle(server.server_handle)

    # --- UPLOAD locations marked for upload ---
    for key in upload_locations:
        server = self._grpc_servers.get(key)
        if server and not isinstance(server, SerializableErrorInfo):
            self._upload_location_data(*key, server, desired[key].code_location_deploy_data)

    # Swap actual entries to reflect current state
    self._actual_entries = desired.copy()
```

### Server Health Checking (Periodic)

Every reconcile cycle, ping all running servers:

- If a server fails health check → track `first_unavailable_time`.
- If unavailable for > timeout → trigger recovery restart.

### Graceful Server Removal

Before removing a server:

1. Check if there are non-isolated runs executing on it.
1. If yes, add to `_pending_delete_grpc_server_handles` and wait.
1. Periodically check pending handles; remove when no active runs.

______________________________________________________________________

## 7. `CodeLocationDeployData` — Reusable As-Is

The `CodeLocationDeployData` record from `dagster_cloud_cli` is already OSS-compatible:

```python
@record
class CodeLocationDeployData:
    image: str | None                    # Docker image (required for k8s/docker)
    python_file: str | None              # Path to Python file with defs
    module_name: str | None              # Python module name
    working_directory: str | None        # Working directory for the server
    executable_path: str | None          # Custom Python executable
    attribute: str | None                # Attribute to load (e.g., "definitions")
    container_context: Mapping[str, Any] # Container-specific config
    agent_queue: AgentQueue | None       # Which agent queue this location belongs to
```

This can be loaded from the webserver's workspace definition and passed to the launcher.

______________________________________________________________________

## 8. What We Don't Need (Cloud-Specific Features)

| Feature | Cloud Purpose | OSS Decision |
| ------------------------------ | ------------------------------------------------------------------ | ------------------------------------------------- |
| PEX / Multipex server support | Run multiple code locations in one container for PEX-based deploys | Drop initially; add later if needed |
| Branch deployments | Git branch-based ephemeral environments | Not applicable to OSS |
| Presigned URL uploads | Upload large snapshots to S3 via signed URLs | Use direct HTTP/GraphQL upload |
| `CloudContextEnv` injection | Inject Cloud-specific env vars | Simplify to standard env vars |
| Run worker monitoring thread | Monitor isolated run workers in Cloud | May be needed for k8s/docker; simpler for process |
| Agent queue routing | Route requests to specific agent subsets | Optional — can use labels instead |
| Utilization metrics collection | Report CPU/memory to Cloud dashboard | Optional — local logging only |
