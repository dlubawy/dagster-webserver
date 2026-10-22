# Implementation Plan — DagsterAgent for OSS

## Scope

Build a `dagster-agent` process that replaces `dagster-cloud agent run` for DAGSTER OSS. The agent connects to our forked `dagster-webserver`, manages code location gRPC servers via pluggable launchers, and processes work requests from a database-backed queue.

**Research references**: All design details are in `.agent/research/*.md`:

- `dagster-agent-overview.md` — Architecture and goals
- `dagster-cloud-agent-analysis.md` — Cloud agent internals analysis
- `user-code-launcher-design.md` — Launcher abstraction design
- `agent-instance-design.md` — Instance configuration design
- `webserver-integration.md` — Webserver API design
- `daemon-considerations.md` — Daemon integration analysis
- `implementation-roadmap.md` — Phased timeline

______________________________________________________________________

## Phase 1: Database Schema and Models

**Objective**: Create all database tables and SQLAlchemy ORM models that the agent system depends on.

### Task 1.1: Alembic Migration `003_create_agent_tables`

Create `dagster_webserver/database/alembic/versions/003_create_agent_tables.py`:

**New tables:**

- **`agent_heartbeats`** — Tracks agent health (agent_id, labels, type, timestamp, metadata, errors)
- **`agent_requests`** — Work queue for agents (id, request_api, request_body, location_name, labels, status, assigned_to_agent, created_at, popped_at, expires_at, response_body)
- **`code_location_definitions`** — Workspace entries managed by webserver (location_name, deploy_metadata JSONB, labels, status, last_loaded_at, load_error, loaded_manifest)
- **`code_location_snapshots`** — Loaded repository data with SHA1 dedup (location_name, snapshot_type, sha1_hash, size_bytes, data BYTEA). **Note**: BYTEA storage is the initial implementation. The storage backend can be swapped to file-based or object storage later by introducing a `SnapshotStorage` abstract class with pluggable implementations.
- **`api_keys`** — User-owned API keys for programmatic access (see below)
- **`user_role_assignments`** — Per-location role assignments (see below)

```sql
-- Generic API keys owned by users (service accounts or regular users)
CREATE TABLE api_keys (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),  -- Owner of the key
    key_hash VARCHAR(128) NOT NULL,                 -- SHA-256 hash of the API key
    name VARCHAR(128) NOT NULL,                     -- Human-readable label (e.g. "prod-agent")
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,            -- Optional expiration
    last_used_at TIMESTAMP WITH TIME ZONE           -- Updated on each successful auth
);
CREATE UNIQUE INDEX idx_api_keys_hash ON api_keys (key_hash);

-- Per-location role assignments. Replaces the single users.role_id for
-- location-scoped permission resolution.
CREATE TABLE user_role_assignments (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    role_id INTEGER NOT NULL REFERENCES roles(id),
    code_location_name VARCHAR(256),  -- NULL = global assignment (all locations)
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, role_id, code_location_name)
);
CREATE INDEX idx_user_role_assignments_user ON user_role_assignments (user_id);
```

**Modifications to existing tables:**

- **`users`**: Add `is_service_account BOOLEAN NOT NULL DEFAULT FALSE` column. Service accounts can only authenticate via API key — they cannot use password or OIDC login.
- **Migration of existing data**: For each user with a non-null `role_id`, insert a row into `user_role_assignments` with `code_location_name = NULL` (global). The existing `users.role_id` column is kept for backward compatibility but becomes read-only (new code uses `user_role_assignments`).

Full SQL DDL for agent tables is in `webserver-integration.md §2`.

**Outcome**: Running `dagster-webserver db migrate` creates all six new tables, adds `is_service_account` to users, and migrates existing role assignments.

### Task 1.2: SQLAlchemy ORM Models

Add the following new models to `dagster_webserver/database/models.py`:

- `AgentHeartbeat(Base)` — maps to `agent_heartbeats`
- `AgentRequest(Base)` — maps to `agent_requests`
- `CodeLocationDefinition(Base)` — maps to `code_location_definitions`
- `CodeLocationSnapshot(Base)` — maps to `code_location_snapshots`
- `ApiKey(Base)` — maps to `api_keys` (with relationship to `User`)
- `UserRoleAssignment(Base)` — maps to `user_role_assignments` (with relationships to `User` and `Role`)

Modify existing models:

- `User`: Add `is_service_account: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)`
- `User`: Add `role_assignments: Mapped[list["UserRoleAssignment"]] = relationship(...)`
- `Role`: Add `assignments: Mapped[list["UserRoleAssignment"]] = relationship(...)`

Follow the existing model patterns in the file (DeclarativeBase, mapped_column, Mapped types). Existing models use sync SQLAlchemy (`sqlalchemy.orm.DeclarativeBase`) while `engine.py` provides async sessions. The agent tables follow the same dual-pattern — sync models for ORM definition, async sessions at access time.

**Outcome**: Models are importable from `dagster_webserver.database.models`.

### Task 1.3: Database Access Helpers

Create `dagster_webserver/database/agent_queries.py` with async helper functions that wrap common agent table operations:

- `get_workspace_entries(session, agent_labels, location_names)` → list[CodeLocationDefinition]
- `pop_requests(session, agent_id, agent_labels, limit)` → list[AgentRequest] (with optimistic locking)
- `submit_heartbeat(session, heartbeat_data)` → AgentHeartbeat
- `upsert_code_location(session, definition)` → CodeLocationDefinition
- `store_snapshot(session, location_name, snapshot_type, sha1, data)` → CodeLocationSnapshot

**Outcome**: All agent database operations go through these helpers.

______________________________________________________________________

## Phase 2: Webserver REST API and API Key Auth

**Objective**: Implement the REST endpoints under `/api/agent` and extend the existing auth system to support user-owned API keys with per-location role assignments.

### Task 2.1: API Key Authentication (Integrated into All Providers)

Extend the existing auth system so that all provider types support API key authentication via `Authorization: Bearer <key>` headers. This unifies authentication — one `User` model, one set of roles, per-location permission resolution.

**Design**: Service accounts are regular `User` rows with `is_service_account=True`. They cannot log in via password or OIDC (the login route rejects them). They authenticate exclusively via API key. Regular users can also have API keys for programmatic access. API key support is built into every provider type — not a separate provider.

**Changes to `auth/users.py`**:

- Add abstract method to `UserBackend` ABC:
  ```python
  async def get_user_by_api_key(self, api_key: str) -> AuthUser | None:
      """Look up a user by their API key (hashed and matched against stored keys)."""
      ...
  ```
- Extend `AuthUser` dataclass with new fields:
  ```python
  is_service_account: bool = False
  ```

**Changes to `auth/db_backend.py`** (`DatabaseUserBackend`):

- Implement `get_user_by_api_key(token)`:

  1. SHA-256 hash the token
  1. Look up in `api_keys` table (joined with `users` and `user_role_assignments`)
  1. Verify `is_active=True` and not expired
  1. Update `last_used_at`
  1. Return `AuthUser` with resolved role info from assignments

- Add API key CRUD methods:

  - `create_api_key(user_id, name, expires_at) -> tuple[ApiKey, str]` — returns both the ORM object and the plaintext key (shown once)
  - `list_api_keys(user_id) -> list[ApiKey]`
  - `revoke_api_key(key_id) -> bool`

- Add service account creation:

  - `create_service_account(username, role_name, display_name=None) -> AuthUser` — creates a user with `is_service_account=True`, empty `password_hash`, and a global role assignment in `user_role_assignments`

- Add per-location role assignment methods:

  - `assign_role_to_location(user_id, role_name, code_location_name) -> UserRoleAssignment`
  - `remove_role_from_location(user_id, role_name, code_location_name) -> bool`
  - `get_user_roles_for_location(user_id, location_name) -> list[Role]` — returns all roles where `code_location_name IS NULL OR code_location_name = location_name`

**Changes to `auth/provider.py`**:

- **`SessionAuthProvider.authenticate_request()`**: Before checking session, check for `Authorization: Bearer` header. If present, call `self._user_backend.get_user_by_api_key(token)`. If a user is returned, set `request.state.user` and return. If the Bearer header is present but invalid, return `None` (don't fall through to session).
- **`HybridSessionAuthProvider.authenticate_request()`**: Same change — API key check first, then OIDC/session fallback.
- **Remove `--auth-provider=api-key` CLI option** from `cli.py`. The `ApiKeyAuthProvider` class remains in the codebase for backward compatibility but is no longer exposed as a standalone provider option.

**Changes to `auth/roles.py`**:

- Add location-aware permission resolution:
  ```python
  def resolve_permissions_for_location(
      role_assignments: list[tuple[Role, str | None]],
      location_name: str | None,
  ) -> dict[str, PermissionResult]:
      """Merge permissions from all applicable role assignments.

      A role assignment applies if its code_location_name is NULL (global)
      or matches the requested location_name. When multiple roles apply,
      permissions are merged (OR logic — if any applicable role grants a
      permission, it's enabled).
      """
  ```

**Changes to `auth/middleware.py`**:

- No structural changes needed. The middleware already calls `provider.authenticate_request()` and checks `_is_api_request()`. API key auth is transparently handled inside the provider.
- Add code location scope enforcement for `/api/agent/*` routes: after auth, extract the target location from query params or request body, call `resolve_permissions_for_location()`, and reject if the user has no applicable role for that location.

**Changes to `cli.py`**:

- Remove `--auth-provider=api-key` option from the webserver command.
- Update `_build_auth_provider()` — no longer returns `ApiKeyAuthProvider`.

**Outcome**: All auth providers support API key authentication transparently. Per-location permission resolution works via `user_role_assignments`. Service accounts can only use API keys.

### Task 2.2: CLI Commands for Service Accounts and API Keys

Add subcommands under `dagster-webserver db` (following existing CLI patterns):

```
# Create a service account (agent user)
dagster-webserver db create-service-account --username svc/agent-prod --role agent-manager --database-url ...

# Assign a role to a specific code location
dagster-webserver db assign-role --user svc/agent-prod --role launcher --location my-location --database-url ...

# Create an API key for any user (service account or regular)
dagster-webserver db create-api-key --user svc/agent-prod --name "prod-key" [--expires-in 90d] --database-url ...
# Outputs the plaintext key once — it is stored only as a SHA-256 hash

# List API keys for a user
dagster-webserver db list-api-keys --user svc/agent-prod --database-url ...

# Revoke (deactivate) an API key
dagster-webserver db revoke-api-key --key-id 123 --database-url ...
```

**Outcome**: Admins can create service accounts, assign per-location roles, and manage API keys via CLI.

### Task 2.3: Agent REST Router

Create `dagster_webserver/api/__init__.py` and `dagster_webserver/api/agent.py`:

Define a Starlette `APIRouter` with the following endpoints:

| Method | Path | Handler | Purpose |
| ------ | ------------------------------ | --------------------------- | ------------------------------------------------------------------- |
| GET | `/api/agent/workspace-entries` | `get_workspace_entries` | Return code locations for this agent (filtered by role assignments) |
| POST | `/api/agent/enqueue-request` | `enqueue_request` | Add work to the queue (for UI/daemon) |
| POST | `/api/agent/pop-requests` | `pop_requests` | Agent pops pending work items (filtered by role assignments) |
| POST | `/api/agent/heartbeat` | `submit_heartbeat` | Agent reports health |
| POST | `/api/agent/upload-data` | `upload_code_location_data` | Agent uploads loaded manifests |
| GET | `/api/agent/check-snapshot` | `check_snapshot` | Check if snapshot already stored |
| POST | `/api/agent/upload-snapshot` | `upload_snapshot` | Upload a serialized snapshot |

**Design notes**:

- Use async SQLAlchemy sessions from `engine.get_session_factory()` (existing pattern).
- All endpoints require authentication (via API key or session). The existing `AuthMiddleware` handles this — `/api/agent/*` is already covered by `_is_api_request()`.
- Code location scoping: handlers check that the authenticated user has a role assignment for the requested location (via `get_user_roles_for_location()`). If no applicable role, return 403.
- Mount the router in `DagsterWebserver.build_routes()` alongside existing routes.

Full endpoint specifications are in `webserver-integration.md §4`.

**Outcome**: All seven REST endpoints respond correctly with proper HTTP status codes and location-scoped access control.

### Task 2.4: Code Location CRUD Endpoints

Add admin-facing endpoints for managing code location definitions:

| Method | Path | Handler | Purpose |
| ------ | ---------------------------------- | ---------------------- | ------------------------------------- |
| POST | `/api/agent/code-locations` | `create_code_location` | Create a new code location definition |
| PUT | `/api/agent/code-locations/{name}` | `update_code_location` | Update deploy metadata / labels |
| DELETE | `/api/agent/code-locations/{name}` | `delete_code_location` | Remove a code location |
| GET | `/api/agent/code-locations` | `list_code_locations` | List all code locations with status |

These endpoints require admin-level RBAC permissions. They use the existing user auth (session or API key with appropriate role).

**Outcome**: Code locations can be managed via REST API by admins.

### Task 2.5: Admin Portal — Code Locations and Service Accounts

Extend the existing admin portal (`dagster_webserver/admin/portal.py`) with new sections:

- Add a new "Code Locations" admin panel section in `admin/views.py`.
  - Create template files for code location list/create/edit views (reuse existing template patterns from `admin/templates/`).
  - Wire up to the CRUD endpoints from Task 2.4.
- Add a "Service Accounts & API Keys" view alongside existing user/role management.
  - List service accounts with their active API keys and assigned locations.
  - Create/revoke API keys from the UI.

**Outcome**: Admin portal at `/admin` includes code location and service account management.

### Task 2.5b: Admin Portal — Scoped Role Assignment UI

Update existing admin portal user management views to reflect per-location role assignments:

- **User detail page**: Replace the single "role" dropdown with a "Role Assignments" table showing:
  - Columns: Role | Scope (Global / location name) | Actions (Remove)
  - An "Add Assignment" form: select role + optional code location (leave blank for global).
  - Existing users migrated from `users.role_id` show as a single global row.
- **Code location detail page** (new): Show which users/service accounts have access to this location and at what role level. Allows adding/removing assignments scoped to that location.
- **Role listing page**: Add a column showing how many users/locations each role is assigned to.

**Backend endpoints needed** (add to `api/agent.py` or a new `api/admin.py`):

| Method | Path | Purpose |
| ------ | ---------------------------------------------- | -------------------------------------------------------------------------- |
| GET | `/api/admin/users/{username}/role-assignments` | List all role assignments for a user |
| POST | `/api/admin/users/{username}/role-assignments` | Add a role assignment (body: `{"role": "launcher", "location": "my-loc"}`) |
| DELETE | `/api/admin/role-assignments/{id}` | Remove a specific assignment |
| GET | `/api/agent/code-locations/{name}/access` | List users with access to this location and their roles |

These endpoints call the `assign_role_to_location()`, `remove_role_from_location()`, and `get_user_roles_for_location()` methods from Task 2.1.

**Outcome**: Admins can manage per-location role assignments through the admin portal UI.

### Task 2.6: Workspace Entry Source — Database + YAML Support

Support two sources for code location definitions:

1. **Database** (primary): Code locations defined via admin portal / REST API, stored in `code_location_definitions` table.
1. **YAML file** (optional, for backcompat): If a `workspace.yaml` exists at `DAGSTER_HOME/workspace.yaml`, load entries from it on webserver startup and upsert them into the database table. This mirrors the standard OSS workspace loading pattern.

The YAML format:

```yaml
locations:
  - name: my-location
    python_file: /path/to/file.py
    working_directory: /path/to/dir
    labels:
      deployment: production
```

Implementation: Create `dagster_webserver/api/workspace_loader.py` with a function that loads from YAML and upserts into the database. Call it during webserver startup (in the lifespan or CLI).

**Outcome**: Code locations can be defined via admin portal, REST API, or `workspace.yaml`.

### Task 2.7: Login Route — Reject Service Accounts

Update the login route handler to reject authentication attempts for service accounts:

- In the password login handler: after looking up the user by username, check `is_service_account`. If true, return a generic "invalid credentials" error (don't leak that the account exists).
- In the OIDC callback: if an OIDC sub maps to a service account user, reject the link.

**Outcome**: Service accounts can only authenticate via API key, never via password or OIDC.

______________________________________________________________________

## Phase 3: Agent Package — Core Types and CLI

**Objective**: Create the `dagster_agent` package with core data types, HTTP client, and CLI entry point. Lives in this repo under `dagster_agent/`.

### Task 3.1: Package Structure

Create the following files:

```
dagster_agent/
├── __init__.py
├── cli.py                    # CLI entry point
├── agent.py                  # DagsterAgent main class (Phase 5)
├── instance.py               # DagsterAgentInstance (Phase 4)
├── api/
│   ├── __init__.py
│   ├── types.py              # Core data types
│   └── client.py             # HTTP client for webserver REST API
├── user_code_launcher/
│   ├── __init__.py
│   ├── base.py               # DagsterUserCodeLauncher abstract base (Phase 4)
│   └── process.py            # ProcessUserCodeLauncher (Phase 5)
└── tests/
    ├── __init__.py
    ├── test_types.py
    └── conftest.py
```

Add `dagster_agent` as a package in the existing `pyproject.toml` via `[tool.hatch.build.targets.wheel.packages]`. Add CLI entry point:

```toml
[project.scripts]
dagster-agent = "dagster_agent.cli:main"
```

**Outcome**: Package is importable, `dagster-agent --help` works.

### Task 3.2: Core Data Types (`api/types.py`)

Define OSS-compatible versions of Cloud types (see `webserver-integration.md §3` and `user-code-launcher-design.md §2`):

```python
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
    request_body: str
    location_name: str | None

@record
class AgentHeartbeat:
    agent_id: str
    agent_type: str | None
    timestamp: float
    metadata: dict[str, Any] | None
    errors: list[str] | None
    code_server_heartbeats: list[str] | None

@record
class CodeLocationDeployData:
    image: str | None
    python_file: str | None
    module_name: str | None
    working_directory: str | None
    executable_path: str | None
    attribute: str | None
```

**Outcome**: Types are serializable/deserializable via `dagster._serdes`.

### Task 3.3: HTTP Client (`api/client.py`)

Create an HTTP client for the agent to communicate with the webserver REST API:

```python
class AgentAPIClient:
    """HTTP client for dagster-agent ↔ webserver communication."""

    def __init__(self, base_url: str, api_key: str): ...

    async def get_workspace_entries(self) → list[dict]: ...
    async def pop_requests(self, limit: int = 10) → list[dict]: ...
    async def submit_heartbeat(self, heartbeat: AgentHeartbeat) → bool: ...
    async def upload_code_location_data(self, location_name: str, manifest: str) → dict: ...
    async def upload_response(self, request_id: str, response: str) → bool: ...
    async def check_snapshot(self, location_name: str, snapshot_type: str, sha1: str, size: int) → dict: ...
    async def upload_snapshot(self, location_name: str, snapshot_type: str, data: bytes) → dict: ...
```

Uses `httpx.AsyncClient` with retry logic (exponential backoff). Attaches `Authorization: Bearer <api_key>` header to all requests.

**Outcome**: Agent has a typed HTTP client for all webserver interactions.

### Task 3.4: CLI (`cli.py`)

Implement the `dagster-agent` CLI with a single command:

```python
@click.group(name="dagster-agent")
def cli(): ...

@cli.command(name="run", help="Start the Dagster agent process.")
@click.option("--dagster-home", type=click.Path(), default=None)
@click.option("--log-level", default="info")
def run(dagster_home, log_level): ...
```

The `run` command:

1. Sets `DAGSTER_HOME` env var if provided.
1. Creates `DagsterAgentInstance.get()` (Phase 4).
1. Starts the user code launcher's reconcile thread.
1. Runs the agent main loop (Phase 5).

**Outcome**: `dagster-agent run` starts up and fails gracefully with clear error messages (since dependent components don't exist yet).

______________________________________________________________________

## Phase 4: DagsterAgentInstance and Launcher Base

**Objective**: Create the instance class that agents use to connect to the webserver, and the abstract launcher base class.

### Task 4.1: `DagsterAgentInstance` (`instance.py`)

Implement the instance subclass (see `agent-instance-design.md §3`):

```python
class DagsterAgentInstance(DagsterInstance):
    # Properties
    webserver_url, agent_api_url
    instance_uuid, agent_labels, allowed_locations, api_key
    poll_interval_seconds, heartbeat_interval_seconds, request_timeout_seconds

    # Lazy-loaded components
    user_code_launcher → rehydrated from config

    # Config schema (see agent-instance-design.md §2)
    config_schema()
    config_defaults(base_dir)

    # Factory
    get() → self
    dispose()
```

Key design decisions:

- Uses `dagster._core.instance.DagsterInstance` as base class.
- `user_code_launcher` is a `ConfigurableClassData` rehydrated from YAML config (same pattern as Cloud).
- HTTP client (`AgentAPIClient`) is created by the agent, not the instance. The instance provides the URL and API key.
- No GraphQL client — agent communicates via REST endpoints.

**Outcome**: `DagsterAgentInstance.get()` creates a valid instance from `dagster.yaml`.

### Task 4.2: Launcher Base Class (`user_code_launcher/base.py`)

Implement the abstract base class (see `user-code-launcher-design.md §2`):

```python
class DagsterUserCodeLauncher(AbstractContextManager, Generic[ServerHandle]):
    # Core state
    _desired_entries: dict[tuple[str, str], UserCodeLauncherEntry]
    _actual_entries: dict[tuple[str, str], UserCodeLauncherEntry]
    _grpc_servers: dict[tuple[str, str], DagsterGrpcServer | SerializableErrorInfo]

    # Abstract methods (concrete launchers implement)
    user_code_deployment_type → str
    requires_images → bool
    _start_new_server_spinup(deployment, location, entry) → DagsterGrpcServer
    _wait_for_new_server_ready(deployment, location, entry, handle, endpoint) → None
    _remove_server_handle(handle) → None
    run_launcher() → RunLauncher

    # Concrete methods (shared across all launchers)
    start() → None  # Starts reconcile thread
    stop() → None   # Graceful shutdown
    reconcile() → None  # Diff desired vs actual, add/update/remove servers
    update_grpc_metadata(desired_map) → None  # Set desired state from webserver
    has_grpc_endpoint(deployment, location) → bool  # Check if server is ready
    get_grpc_server(deployment, location) → DagsterGrpcServer | None
```

**Outcome**: Base class is importable and all abstract methods are documented. Concrete launchers can subclass it.

______________________________________________________________________

## Phase 5: Process Launcher and Agent Main Loop (MVP)

**Objective**: Implement `ProcessUserCodeLauncher` and the agent main loop. This delivers a working MVP where `dagster-agent run` can manage local code location processes.

### Task 5.1: `ProcessUserCodeLauncher` (`user_code_launcher/process.py`)

Implement the simplest launcher — local subprocesses (see `user-code-launcher-design.md §3`):

```python
class ProcessUserCodeLauncher(DagsterUserCodeLauncher[int], ConfigurableClass):
    """Spawn local gRPC server subprocesses. Handle type = PID."""
```

Concrete methods to implement:

- `_start_new_server_spinup()` — Build `dagster api grpc` command, launch subprocess, create Unix socket or find free port, start heartbeat thread.
- `_wait_for_new_server_ready()` — Poll gRPC client until server responds (with timeout).
- `_remove_server_handle()` — Stop heartbeat → process dies from timeout.
- `run_launcher()` — Return `DefaultRunLauncher()`.

Reuse patterns from:

- `dagster_cloud/workspace/user_code_launcher/process.py` (Cloud version)
- `dagster._core.remote_representation.grpc_server_registry.GrpcServerRegistry` (OSS daemon)

**Outcome**: Process launcher can start, health-check, and stop local gRPC server subprocesses.

### Task 5.2: Agent Main Loop (`agent.py`)

Implement the agent process that ties everything together (see `dagster-cloud-agent-analysis.md §2`):

```python
class DagsterAgent:
    def __init__(self, instance: DagsterAgentInstance): ...

    def run_loop(self, user_code_launcher: DagsterUserCodeLauncher) → None:
        """Main event loop. Runs until KeyboardInterrupt."""
        # 1. Initial workspace load
        # 2. Loop: poll requests → classify → dispatch → heartbeat → sleep

    def _poll_workspace(self, user_code_launcher) → None:
        """Query webserver for workspace entries, feed to launcher."""

    def _pop_requests(self) → list[dict]:
        """Pop pending requests from the webserver queue."""

    def _classify_requests(self, requests, user_code_launcher) → tuple[list, list]:
        """Split into ready (server exists) and pending (waiting for server)."""

    def _dispatch_requests(self, requests, user_code_launcher) → None:
        """Submit ready requests to thread pool executor."""

    def _process_request(self, json_request, user_code_launcher) → dict:
        """Handle a single API request. Route to handler based on request_api."""

    def _handle_launch_run(self, request, user_code_launcher) → dict: ...
    def _handle_terminate_run(self, request, user_code_launcher) → dict: ...
    # ... handlers for all DagsterAgentApi types

    def _send_heartbeat(self) → None:
        """Report agent health to webserver."""

    def _upload_response(self, request_id, response) → None:
        """Upload processed request response back to webserver."""
```

**Outcome**: Agent main loop runs continuously, polls workspace, processes requests.

### Task 5.3: Request Handlers

Implement handlers for each `DagsterAgentApi` type:

| API | Handler | Implementation |
| -------------------------------------- | ---------------------------- | -------------------------------------------------------------------- |
| `LAUNCH_RUN` | `_handle_launch_run` | Get gRPC client from launcher → call `launch_run_from_grpc_client()` |
| `TERMINATE_RUN` | `_handle_terminate_run` | Get gRPC client → send `CancelExecutionRequest` |
| `GET_EXTERNAL_EXECUTION_PLAN` | `_handle_execution_plan` | gRPC call to code server |
| `GET_EXTERNAL_SCHEDULE_EXECUTION_DATA` | `_handle_schedule_execution` | gRPC call with instance ref |
| `GET_EXTERNAL_SENSOR_EXECUTION_DATA` | `_handle_sensor_execution` | gRPC call with instance ref |
| `PING_LOCATION` | `_handle_ping` | No-op, updates TTL tracking |
| `CHECK_FOR_WORKSPACE_UPDATES` | `_handle_workspace_update` | Trigger workspace re-upload |

See `dagster-cloud-agent-analysis.md §3` for the Cloud implementation patterns to follow.

**Outcome**: All request types are handled and responses uploaded to webserver.

### Task 5.4: CLI Integration

Wire up `cli.py run` command to use `DagsterAgentInstance` + `DagsterAgent`:

```python
@cli.command(name="run")
def run(dagster_home, log_level):
    if dagster_home:
        os.environ["DAGSTER_HOME"] = str(dagster_home)

    with DagsterAgentInstance.get() as instance:
        launcher = instance.user_code_launcher
        launcher.start()

        with DagsterAgent(instance) as agent:
            agent.run_loop(launcher)
```

**Outcome**: `dagster-agent run --dagster-home /path/to/config` starts the full agent process.

### Task 5.5: MVP Tests

Create integration tests that verify the MVP works end-to-end:

- Test: Agent starts and connects to webserver (mock).
- Test: Process launcher spawns a gRPC server subprocess.
- Test: Agent pops a `LAUNCH_RUN` request and processes it.
- Test: Heartbeat is submitted to webserver.
- Test: API key auth validates/rejects keys correctly for service accounts.
- Test: Per-location role scoping enforces access (403 for unassigned locations).

**Outcome**: CI passes with all MVP tests green.

______________________________________________________________________

## Phase 6: Daemon Integration (Core)

**Objective**: Enable the daemon to enqueue work through the agent queue. The `enqueue-request` REST endpoint from Phase 2 is already in place; this phase connects the daemon side.

### Task 6.1: Daemon-Side Agent Request Client

Create `dagster_agent/daemon/client.py`:

```python
class DaemonAgentClient:
    """Client for the daemon to enqueue work through the agent queue."""

    def __init__(self, webserver_url: str, api_key: str): ...

    async def enqueue_request(self, request_api: str, request_body: dict, location_name: str) → str:
        """Enqueue a request and return its ID."""

    async def get_request_status(self, request_id: str) → dict:
        """Poll for request status (for request-reply pattern)."""
```

Uses `httpx.AsyncClient` with the API key. Calls `/api/agent/enqueue-request`.

**Outcome**: Daemon can enqueue requests to the agent queue.

### Task 6.2: Request-Reply Support in Agent

Extend the agent to support the request-reply pattern needed by the daemon (see `daemon-considerations.md §4`):

- Add a new REST endpoint `/api/agent/request-status/{request_id}` for polling response status.
- The agent's `_upload_response()` method stores responses in the `response_body` column of `agent_requests`.
- The daemon client polls this endpoint until the request is completed or failed.

**Outcome**: Daemon can make synchronous (from its perspective) requests through the agent queue.

### Task 6.3: Hybrid Daemon Mode — Run Launching Through Queue

Create a demonstration/wrapper that shows how `QueuedRunCoordinatorDaemon` would enqueue runs instead of launching directly (see `daemon-considerations.md §6`). This doesn't require forking the daemon yet — it's a proof-of-concept module:

```python
# dagster_agent/daemon/run_coordinator.py
class AgentRunEnqueuer:
    """Demonstrates enqueuing runs through the agent queue."""

    def __init__(self, client: DaemonAgentClient): ...

    async def enqueue_run(self, run, location_name: str) → str: ...
```

**Outcome**: Queue-based run launching is demonstrated and tested end-to-end. Full daemon fork can be created later based on this pattern.

### Task 6.4: Stale Request Re-queuing

Implement background task on the webserver that re-queues expired requests:

- Add to the Starlette lifespan as a periodic background task.
- Find `agent_requests` with `status='processing'` and `expires_at < NOW()`.
- Set status back to `'pending'`, clear `assigned_to_agent`.

**Outcome**: Crashed agents don't leave requests permanently stuck.

______________________________________________________________________

## Phase 7: K8s and Docker Launchers (Post-MVP)

**Objective**: Implement additional launcher types for production deployments.

### Task 7.1: `K8sUserCodeLauncher` (`user_code_launcher/k8s.py`)

Deploy code locations as Kubernetes Deployments + Services (see `user-code-launcher-design.md §4`).

**Outcome**: Agent can manage code locations on k8s clusters.

### Task 7.2: `DockerUserCodeLauncher` (`user_code_launcher/docker.py`)

Run code locations as Docker containers (see `user-code-launcher-design.md §5`).

**Outcome**: Agent can manage code locations in Docker.

______________________________________________________________________

## Phase 8: Production Hardening (Post-MVP)

**Objective**: Add production-ready features and polish.

### Task 8.1: Sentinel Files (k8s Probes)

Implement liveness/readiness sentinel file writing (see `agent-instance-design.md §2`):

- Agent writes to `sentinel_dir/liveness` and `sentinel_dir/readiness` on each loop iteration.
- Configurable via `sentinel_dir` in agent config.

**Outcome**: k8s probes can monitor agent health.

### Task 8.2: Monitoring and Observability

- Structured logging throughout the agent process.
- Agent metrics (requests processed, server uptime, errors) logged periodically.

**Outcome**: Agent operations are observable.

______________________________________________________________________

## Dependency Graph

```
Phase 1 (DB Schema + Models) ─────────────┐
                                           ├──→ Phase 2 (REST API + API Key Auth) ──┐
Phase 3 (Agent Package Core) ──────────────┤                                         │
                                           └──→ Phase 4 (Instance + Base) ──────────┤
                                                                                      ├──→ Phase 5 (MVP: Process Launcher + Main Loop)
                                                                                       │
                                                                                       ├──→ Phase 6 (Daemon Integration — Core)
                                                                                       │
                                                                                       ├──→ Phase 7 (K8s/Docker Launchers — Post-MVP)
                                                                                       │
                                                                                       └──→ Phase 8 (Production Hardening — Post-MVP)
```

**Critical path to MVP**: Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5
**Critical path to full queue-based run launching**: Critical path + Phase 6

______________________________________________________________________

## File Change Summary

| File | Action | Phase | Description |
| ------------------------------------------------------ | ------ | ----- | ------------------------------------------------------------------------------------------------------------------------------ |
| `database/alembic/versions/003_create_agent_tables.py` | Create | 1 | Migration for 6 new tables + column addition to users + role assignment migration |
| `database/models.py` | Edit | 1 | Add AgentHeartbeat, AgentRequest, CodeLocationDefinition, CodeLocationSnapshot, ApiKey, UserRoleAssignment models; extend User |
| `database/agent_queries.py` | Create | 1 | Async helper functions for agent DB operations |
| `auth/users.py` | Edit | 2 | Add `get_user_by_api_key()` to UserBackend ABC; extend AuthUser with `is_service_account` |
| `auth/db_backend.py` | Edit | 2 | Implement API key lookup/CRUD, service account creation, per-location role assignment methods |
| `auth/provider.py` | Edit | 2 | Add API key check to SessionAuthProvider and HybridSessionAuthProvider; remove ApiKeyAuthProvider from provider options |
| `auth/roles.py` | Edit | 2 | Add `resolve_permissions_for_location()` helper for per-location permission merging |
| `api/__init__.py` | Create | 2 | Package init |
| `api/agent.py` | Create | 2 | REST endpoints (7 agent + 4 CRUD handlers) |
| `api/workspace_loader.py` | Create | 2 | Load workspace entries from YAML and upsert to DB |
| `admin/views.py` | Edit | 2 | Add code location and service account management views |
| `admin/templates/code_locations.html` | Create | 2 | Admin portal templates for code locations |
| `admin/templates/service_accounts.html` | Create | 2 | Admin portal templates for service accounts & API keys |
| `webserver.py` | Edit | 2 | Mount agent API router in build_routes() |
| `cli.py` | Edit | 2 | Remove `--auth-provider=api-key`; add service account and API key management subcommands |
| `pyproject.toml` | Edit | 3 | Add dagster_agent package + CLI entry point |
| `dagster_agent/__init__.py` | Create | 3 | Package init |
| `dagster_agent/api/types.py` | Create | 3 | Core data types (DagsterAgentApi, AgentRequest, etc.) |
| `dagster_agent/api/client.py` | Create | 3 | HTTP client for webserver REST API |
| `dagster_agent/cli.py` | Create | 3 | CLI entry point |
| `dagster_agent/instance.py` | Create | 4 | DagsterAgentInstance class |
| `dagster_agent/user_code_launcher/__init__.py` | Create | 4 | Package init, exports |
| `dagster_agent/user_code_launcher/base.py` | Create | 4 | DagsterUserCodeLauncher abstract base |
| `dagster_agent/user_code_launcher/process.py` | Create | 5 | ProcessUserCodeLauncher |
| `dagster_agent/agent.py` | Create | 5 | DagsterAgent main class + request handlers |
| `dagster_agent/daemon/client.py` | Create | 6 | DaemonAgentClient for enqueueing requests |
| `dagster_agent/daemon/run_coordinator.py` | Create | 6 | AgentRunEnqueuer proof-of-concept |
| `dagster_agent/user_code_launcher/k8s.py` | Create | 7 | K8sUserCodeLauncher |
| `dagster_agent/user_code_launcher/docker.py` | Create | 7 | DockerUserCodeLauncher |

______________________________________________________________________

## Decisions Made (from Q&A)

| Question | Decision |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Agent package location | Lives in this repo under `dagster_agent/`, same `pyproject.toml` |
| Agent communication protocol | REST endpoints (`/api/agent/*`) — no GraphQL schema extension needed |
| Agent authentication | Service accounts (users with `is_service_account=True`) authenticate via API keys. Single user model, existing RBAC roles extended with per-location assignments via `user_role_assignments` junction table. Regular users can also have API keys for programmatic access. API key auth is integrated into all provider types (not a separate provider). |
| Snapshot storage | BYTEA in PostgreSQL for now. Swappable later via `SnapshotStorage` abstract class. |
| Daemon integration scope | Queue infrastructure is core (Phase 2 + Phase 6). Full daemon fork is post-MVP. |
| Workspace entry source | Database (primary) + optional `workspace.yaml` loading for backcompat (Phase 2.6). |
