# DagsterAgentInstance — Configuration and Instance Design

## Goal

Design `DagsterAgentInstance`, a new `DagsterInstance` subclass that provides the agent process with everything it needs to:

1. Connect to the webserver (our fork).
1. Configure storage backends (runs, events) pointing at the shared database.
1. Manage the user code launcher lifecycle.

______________________________________________________________________

## 1. Class Hierarchy

```
DagsterInstance              ← Base class from OSS dagster
    │
    └── DagsterAgentInstance  ← New: agent-specific instance (our fork)
            │
            ├── Connects to webserver GraphQL API
            ├── Provides agent queue operations
            ├── Manages user code launcher
            └── Configures shared storage backends
```

This mirrors `DagsterCloudAgentInstance` but targets our webserver instead of Dagster Cloud.

______________________________________________________________________

## 2. Configuration Schema (`dagster.yaml`)

### Minimal Example

```yaml
instance_class:
  module: dagster_agent.instance
  class: DagsterAgentInstance

agent:
  webserver_url: "http://localhost:3000"
  agent_id: "agent-prod-1" # Optional — auto-generated UUID if omitted
  labels: # Optional — used for request routing
    deployment: "production"

user_code_launcher:
  module: dagster_agent.user_code_launcher.process
  class: ProcessUserCodeLauncher
  config:
    server_ttl:
      enabled: true
      ttl_seconds: 3600
```

### Full Configuration Schema

```python
@classmethod
def config_schema(cls):
    return {
        # ── Agent connection to webserver ──────────────────────
        "agent": Field({
            "webserver_url": Field(
                str,
                is_required=True,
                description="URL of the dagster-webserver instance (with /graphql endpoint)."
            ),
            "agent_id": Field(
                str,
                is_required=False,
                description="Unique identifier for this agent. Auto-generated UUID if omitted."
            ),
            "labels": Field(
                Map(str, str),
                is_required=False,
                description="Labels for request routing. Agents only pick up requests matching their labels."
            ),
            "poll_interval_seconds": Field(
                int,
                is_required=False,
                default_value=5,
                description="How often to poll the webserver for new requests."
            ),
            "heartbeat_interval_seconds": Field(
                int,
                is_required=False,
                default_value=30,
                description="How often to send heartbeats to the webserver."
            ),
            "request_timeout_seconds": Field(
                int,
                is_required=False,
                default_value=30,
                description="HTTP request timeout for webserver API calls."
            ),
            "retries": Field(
                int,
                is_required=False,
                default_value=3,
                description="Number of retries for failed HTTP requests."
            ),
        }, is_required=True),

        # ── User code launcher (how to deploy code servers) ───
        "user_code_launcher": config_field_for_configurable_class(),

        # ── Optional: scope this agent to specific locations ──
        "allowed_locations": Field(
            Array(str),
            is_required=False,
            description="If set, only manage code locations whose names are in this list."
        ),

        # ── Optional: isolated agents mode ────────────────────
        "isolated_agents": Field({
            "enabled": Field(
                bool,
                is_required=False,
                default_value=False,
                description="If True, agents cannot see each other's gRPC servers. Requires isolated run launcher."
            ),
        }, is_required=False),

        # ── Optional: sentinel directory (for k8s readiness probes) ──
        "sentinel_dir": Field(
            str,
            is_required=False,
            description="Directory to write liveness/readiness sentinel files. Used by k8s probes."
        ),
    }
```

______________________________________________________________________

## 3. Class Design

### Constructor and Initialization

```python
class DagsterAgentInstance(DagsterInstance):
    def __init__(self, *args, agent: dict, user_code_launcher=None, **kwargs):
        super().__init__(*args, **kwargs)

        self._agent_config = agent
        self._user_code_launcher_data = configurable_class_data(user_code_launcher)

        if not self._user_code_launcher_data:
            raise DagsterInvariantViolationError(
                "user_code_launcher is required for DagsterAgentInstance."
            )

        # Unique identifier for this agent process
        self._instance_uuid = agent.get("agent_id") or str(uuid.uuid4())

        # Exit stack for cleanup
        self._exit_stack = ExitStack()

        # Lazy-loaded components
        self._user_code_launcher: DagsterUserCodeLauncher | None = None
        self._graphql_client: GraphQLClient | None = None
        self._http_session: Session | None = None
```

### Key Properties

```python
@property
def webserver_url(self) -> str:
    """Base URL of the webserver."""
    return self._agent_config["webserver_url"].rstrip("/")

@property
def graphql_url(self) -> str:
    """GraphQL endpoint URL."""
    return f"{self.webserver_url}/graphql"

@property
def agent_api_url(self) -> str:
    """REST API base URL for agent operations."""
    return f"{self.webserver_url}/api/agent"

@property
def instance_uuid(self) -> str:
    """Unique identifier for this agent process."""
    return self._instance_uuid

@property
def agent_labels(self) -> dict[str, str] | None:
    """Labels used for request routing."""
    return self._agent_config.get("labels")

@property
def allowed_locations(self) -> list[str] | None:
    """If set, only manage these code locations."""
    return self._agent_config.get("allowed_locations")

@property
def poll_interval_seconds(self) -> int:
    return self._agent_config.get("poll_interval_seconds", 5)

@property
def heartbeat_interval_seconds(self) -> int:
    return self._agent_config.get("heartbeat_interval_seconds", 30)

@property
def request_timeout_seconds(self) -> int:
    return self._agent_config.get("request_timeout_seconds", 30)

@property
def is_using_isolated_agents(self) -> bool:
    iso = self._agent_config.get("isolated_agents", {})
    return iso.get("enabled", False) if isinstance(iso, dict) else False

@property
def sentinel_dir(self) -> str | None:
    return self._agent_config.get("sentinel_dir")
```

### GraphQL Client

```python
@property
def graphql_client(self):
    """HTTP client for webserver GraphQL API."""
    if self._graphql_client is None:
        from dagster_cloud_cli.core.graphql_client import create_graphql_requests_session

        session = self._exit_stack.enter_context(create_graphql_requests_session(
            adapter_kwargs=dict(
                max_retries=Retry(
                    total=self._agent_config.get("retries", 3),
                    backoff_factor=0.5,
                    status_forcelist=[500, 502, 503, 504],
                ),
            )
        ))

        self._graphql_client = GraphQLClient(
            session=session,
            url=self.graphql_url,
            timeout=self.request_timeout_seconds,
        )
    return self._graphql_client

@property
def http_session(self) -> Session:
    """HTTP session for REST API calls (uploads, etc.)."""
    if self._http_session is None:
        from dagster_cloud_cli.core.graphql_client import create_graphql_requests_session

        self._http_session = self._exit_stack.enter_context(create_graphql_requests_session())
    return self._http_session
```

### User Code Launcher

```python
@property
def user_code_launcher(self) -> DagsterUserCodeLauncher:
    """Lazy-loaded user code launcher."""
    if not self._user_code_launcher:
        from dagster_agent.user_code_launcher import DagsterUserCodeLauncher

        self._user_code_launcher = self._exit_stack.enter_context(
            self._user_code_launcher_data.rehydrate(as_type=DagsterUserCodeLauncher)
        )
        self._user_code_launcher.register_instance(self)
    return self._user_code_launcher
```

______________________________________________________________________

## 4. Storage Defaults

The agent instance needs storage backends pointing at the same database as the webserver and daemon. These are configured via `dagster.yaml` or set as defaults:

```python
@staticmethod
def config_defaults(base_dir):
    defaults = InstanceRef.config_defaults(base_dir)

    # Point all storage at the shared PostgreSQL/SQLite database
    # (same connection string as the webserver uses)
    defaults["run_storage"] = ConfigurableClassData(
        "dagster._core.storage.runs", "SqlRunStorage", yaml.dump({})
    )
    defaults["event_log_storage"] = ConfigurableClassData(
        "dagster._core.storage.event_log", "SqlEventLogStorage", yaml.dump({})
    )
    defaults["schedule_storage"] = ConfigurableClassData(
        "dagster._core.storage.schedules", "SqlScheduleStorage", yaml.dump({})
    )

    return defaults
```

### Required `dagster.yaml` Storage Configuration

The agent's `dagster.yaml` must specify the same database as the webserver:

```yaml
instance_class:
  module: dagster_agent.instance
  class: DagsterAgentInstance

run_storage:
  module: dagster._core.storage.runs
  class: SqlRunStorage
  config:
    postgres_url: "postgresql+psycopg2://user:pass@host:5432/dagster"

event_log_storage:
  module: dagster._core.storage.event_log
  class: SqlEventLogStorage
  config:
    postgres_url: "postgresql+psycopg2://user:pass@host:5432/dagster"

schedule_storage:
  module: dagster._core.storage.schedules
  class: SqlScheduleStorage
  config:
    postgres_url: "postgresql+psycopg2://user:pass@host:5432/dagster"

agent:
  webserver_url: "http://dagster-webserver:3000"

user_code_launcher:
  module: dagster_agent.user_code_launcher.process
  class: ProcessUserCodeLauncher
```

______________________________________________________________________

## 5. Daemon Types

The agent does not run any daemons itself (sensors, schedules, etc. are handled by the separate `dagster-daemon` process):

```python
def get_required_daemon_types(self) -> Sequence[str]:
    return []  # No daemons — handled by dagster-daemon process
```

______________________________________________________________________

## 6. Factory Method

```python
@staticmethod
def get():
    """Create a DagsterAgentInstance from DAGSTER_HOME dagster.yaml."""
    instance = DagsterInstance.get()
    if not isinstance(instance, DagsterAgentInstance):
        raise DagsterInvariantViolationError(
            "DagsterInstance.get() did not return a DagsterAgentInstance. "
            "Make sure your dagster.yaml is configured with:\n"
            "instance_class:\n"
            "  module: dagster_agent.instance\n"
            "  class: DagsterAgentInstance"
        )
    return instance

def dispose(self):
    super().dispose()
    self._exit_stack.close()
```

______________________________________________________________________

## 7. Comparison with DagsterCloudAgentInstance

| Feature | DagsterCloudAgentInstance | DagsterAgentInstance (OSS) |
| ------------------ | ---------------------------------- | ----------------------------------------- |
| Auth mechanism | Agent token (decoded from JWT) | None by default; optional API key or mTLS |
| URL derivation | Auto-derived from token org/region | Explicit `webserver_url` config |
| Deployment concept | Multiple deployments per org | Single workspace (simplified) |
| Branch deployments | Supported via flag | Not applicable |
| Agent queues | Complex queue routing | Optional labels for filtering |
| Storage backends | GraphQL proxies to Cloud DB | Direct SQL connections |
| Snapshot uploads | Presigned URLs → S3 | Direct HTTP/GraphQL upload |
| Heartbeat endpoint | Cloud-specific mutation | Webserver API endpoint |

______________________________________________________________________

## 8. Example `dagster.yaml` Files

### Process Launcher (Development)

```yaml
instance_class:
  module: dagster_agent.instance
  class: DagsterAgentInstance

run_storage:
  module: dagster._core.storage.runs
  class: SqlRunStorage
  config:
    postgres_url: "postgresql+psycopg2://dagster:dagster@localhost:5432/dagster"

event_log_storage:
  module: dagster._core.storage.event_log
  class: SqlEventLogStorage
  config:
    postgres_url: "postgresql+psycopg2://dagster:dagster@localhost:5432/dagster"

schedule_storage:
  module: dagster._core.storage.schedules
  class: SqlScheduleStorage
  config:
    postgres_url: "postgresql+psycopg2://dagster:dagster@localhost:5432/dagster"

agent:
  webserver_url: "http://localhost:3000"
  agent_id: "dev-agent-1"

user_code_launcher:
  module: dagster_agent.user_code_launcher.process
  class: ProcessUserCodeLauncher
  config:
    server_ttl:
      enabled: false
```

### K8s Launcher (Production)

```yaml
instance_class:
  module: dagster_agent.instance
  class: DagsterAgentInstance

run_storage:
  module: dagster._core.storage.runs
  class: SqlRunStorage
  config:
    postgres_url: "postgresql+psycopg2://dagster:dagster@postgres:5432/dagster"

event_log_storage:
  module: dagster._core.storage.event_log
  class: SqlEventLogStorage
  config:
    postgres_url: "postgresql+psycopg2://dagster:dagster@postgres:5432/dagster"

schedule_storage:
  module: dagster._core.storage.schedules
  class: SqlScheduleStorage
  config:
    postgres_url: "postgresql+psycopg2://dagster:dagster@postgres:5432/dagster"

agent:
  webserver_url: "http://dagster-webserver.default.svc.cluster.local:3000"
  agent_id: "prod-agent-us-east-1"
  labels:
    deployment: "production"
    region: "us-east-1"
  allowed_locations:
    - "etl-pipeline"
    - "feature-engineering"
    - "reporting"

user_code_launcher:
  module: dagster_agent.user_code_launcher.k8s
  class: K8sUserCodeLauncher
  config:
    namespace: "dagster-code-locations"
    image_pull_policy: "Always"
    deployment_startup_timeout: 300
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
      limits:
        cpu: "1000m"
        memory: "1Gi"
    server_ttl:
      enabled: true
      ttl_seconds: 3600
      max_servers: 25

isolated_agents:
  enabled: true

sentinel_dir: /tmp/dagster-agent-sentinel
```
