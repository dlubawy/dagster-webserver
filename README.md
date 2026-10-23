# Dagster Webserver

Web UI for Dagster, extended with a control plane for **agent-managed code
locations**: optional login with role-based access control (RBAC), an admin
portal, and REST APIs that let one or more `dagster-agent` processes launch
and manage user code servers on your behalf.

## Quick Start

```sh
dagster-webserver start -p 3000
```

Running with a workspace file:

```sh
dagster-webserver start -w path/to/workspace.yaml
```

Running with agent-managed code locations (see
[Agent-managed workspaces](#agent-managed-workspaces)):

```sh
dagster-webserver start --auth-provider hybrid \
  --auth-database-url sqlite+aiosqlite:///auth.db \
  --session-secret my-secret-key \
  --enable-admin-portal \
  --agent-workspaces
```

## Architecture

In a standard OSS deployment, `dagster-webserver` loads code locations from a
static workspace file and talks to their gRPC servers directly. This fork adds
an optional **multi-agent architecture** modelled after Dagster+:

```
┌──────────────────────────────────────────────────────┐
│              dagster-webserver (fork)                │
│  ┌───────────┐  ┌───────────┐  ┌──────────────────┐  │
│  │  GraphQL   │  │ Agent API │  │  Admin Portal    │  │
│  │  UI        │  │ /api/agent│  │  /admin          │  │
│  └─────┬──────┘  └─────┬─────┘  └────────┬─────────┘  │
│  ┌─────▼───────────────▼──────────────────▼─────────┐ │
│  │         DagsterInstance (shared DB)              │ │
│  │  Runs, schedules · agent queue · code location   │ │
│  │  definitions · heartbeats · API keys             │ │
│  └───────────────────────┬──────────────────────────┘ │
└──────────────────────────┼────────────────────────────┘
                           │ REST (Bearer API key)
            ┌──────────────┼──────────────┐
      ┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐
      │ Agent 1   │  │ Agent 2   │  │ Agent N   │
      │ (process) │  │ (k8s)     │  │ (docker)  │
      └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
            ▼              ▼              ▼
          [gRPC]        [gRPC]         [gRPC]
       code servers   Deployments   containers

      ┌──────────────────┐
      │ dagster-daemon   │  reads agent_workspace.yaml,
      │ (unmodified OSS) │  runs schedules/sensors/backfills
      └──────────────────┘
```

- **dagster-webserver** — central control plane. Stores code location
  definitions, the work-request queue, and agent heartbeats in its database;
  serves the UI, GraphQL API, admin portal, and `/api/agent/*` REST endpoints.
- **dagster-agent** (new process) — connects to the webserver, launches and
  maintains gRPC code servers via a pluggable *user code launcher*
  (`process`, `k8s`, or `docker`), reports heartbeats with the live gRPC
  endpoints, and processes work requests.
- **dagster-daemon** — the standard OSS daemon, unmodified. It consumes the
  workspace file the webserver maintains and runs schedules, sensors, and
  backfills against the agent-managed code servers.

Design details live in `.agent/research/*.md`; the implementation plan is in
[PLAN.md](./PLAN.md).

## Agent-managed workspaces

Enable agent-managed code locations with `--agent-workspaces`:

```sh
dagster-webserver start --auth-provider hybrid \
  --auth-database-url sqlite+aiosqlite:///auth.db \
  --session-secret my-secret-key \
  --enable-admin-portal \
  --agent-workspaces
```

The flag requires `--auth-database-url` (or `--admin-database-url`) and is
mutually exclusive with explicit workspace options (`-w`, `-f`, `-m`, `-a`,
`-d`). Instead of a static workspace file, the webserver builds its workspace
dynamically from **gRPC endpoints reported by agents** in their heartbeats:

1. An admin defines code locations in the admin portal (or via the REST API).
2. Agents poll `GET /api/agent/workspace-entries`, which returns all
   locations with status `active`.
3. Each agent launches a gRPC server per location using its configured
   launcher and reports the endpoints (`host:port`) in periodic heartbeats.
4. The webserver reloads its workspace whenever heartbeats change, so the UI
   and GraphQL API see exactly the locations that are actually running.

### Managing code locations

Code locations are managed in the admin portal under **Code Locations** (or
via `GET/POST/PUT/DELETE /api/agent/code-locations`). Each location has:

| Field | Description |
| ---------------------- | ---------------------------------------------------------------------- |
| `location_name` | Unique name of the code location. |
| `image` | Container image for `k8s`/`docker` launchers (e.g. `myrepo/myapp:latest`). |
| `python_file` | Path to a Python file containing the repository definition. |
| `module_name` | Python module name containing the repository definition. |
| `working_directory` | Working directory for loading the code location. |
| `executable_path` | Python executable used to start the gRPC server. |
| `attribute` | Attribute name locating the repository definition. |
| `status` | Lifecycle status: `active` or `paused`. |

**Status is a lifecycle signal, not a deletion marker.** Agents only receive
locations with status `active`:

- **active** — included in workspace entries; agents launch and keep its
  server running.
- **paused** — hidden from agents; their launcher tears the server down.
  Setting the location back to `active` brings it up again on the next sync.
- **Delete** — removes the definition entirely (hard delete); agents stop
  managing the location.

### Daemon integration

When `--agent-workspaces` is set, the webserver periodically writes a
workspace file that external processes can consume:

```sh
# Default location: $DAGSTER_HOME/agent_workspace.yaml
dagster-daemon run --workspace $DAGSTER_HOME/agent_workspace.yaml
```

The file is rewritten atomically on each refresh and contains one
`grpc_server` entry per live code location:

```yaml
load_from:
  - grpc_server:
      location_name: my-location
      host: 127.0.0.1
      port: 32841
```

Customize the path with `--agent-workspace-file` (env var
`DAGSTER_AGENT_WORKSPACE_FILE`).

## Dagster Agent

The agent is a standalone process shipped in this package, installed with the
`agent` extra:

```sh
pip install dagster-webserver[agent]
```

### 1. Create credentials

Agents authenticate to the webserver with an **API key** owned by a
**service account**. In the admin portal (`/admin`):

1. **Service Accounts** → create an account (e.g. `prod-agent`). Service
   accounts can only authenticate via API key — they cannot log in with a
   password or OIDC.
2. **API Keys** → create a key for that service account. Copy the key now;
   it is shown only once (only a hash is stored).

### 2. Configure `dagster.yaml`

The agent reads its configuration from the `agent:` section of
`$DAGSTER_HOME/dagster.yaml`:

```yaml
agent:
  # Webserver connection
  webserver_url: http://localhost:3000
  api_key: <service-account-api-key>

  # Identity / routing
  labels:                    # matched against request queue routing
    region: us-east-1
  allowed_locations: []      # empty = manage all locations

  # Timing
  poll_interval_seconds: 5       # work-request polling
  heartbeat_interval_seconds: 30 # health reporting
  request_timeout_seconds: 300   # per-request processing timeout

  # How code servers are launched
  user_code_launcher:
    type: process            # process | k8s | docker
    config: {}

  # Optional
  sentinel_dir: /tmp/agent-sentinels   # liveness/readiness files (k8s probes)
  metrics_log_interval_seconds: 60
```

`webserver_url` and `api_key` may instead be supplied via environment
variables (used when the corresponding YAML key is absent):

| Variable | Description |
| ------------------------- | -------------------------------------------------------------------- |
| `DAGSTER_HOME` | Directory containing `dagster.yaml`. |
| `DAGSTER_AGENT_WEBSERVER_URL` | Fallback for `agent.webserver_url`. |
| `DAGSTER_AGENT_API_KEY` | Fallback for `agent.api_key`. |

### 3. Run it

```sh
dagster-agent run --dagster-home /path/to/dagster_home --log-level info
```

On startup the agent:

1. Fetches workspace entries (all `active` code locations, filtered by its
   `allowed_locations`).
2. Reconciles its launcher — starting servers for new locations and tearing
   down servers for locations that disappeared or were paused.
3. Enters its main loop: polling the work-request queue, processing requests
   against the gRPC servers, and sending heartbeats (including each code
   server's endpoint) so the webserver can build its workspace.

### User code launchers

The launcher strategy is selected with `user_code_launcher.type`:

#### `process` (default)

Spawns a local gRPC server subprocess per location.

| Config option | Description |
| ------------------------------ | --------------------------------------------------------------- |
| `server_process_startup_timeout` | Seconds to wait for gRPC readiness (default `60`). |
| `server_port_range` | `(start, end)` port range for gRPC servers (default: auto). |
| `extra_env` | Extra environment variables for child processes. |

#### `k8s`

Deploys each location as a Kubernetes Deployment + Service. Requires the
Kubernetes API to be reachable from the agent and an image built from your
code (set the location's `image` field in the admin portal).

| Config option | Description |
| ---------------------------- | -------------------------------------------------------------- |
| `namespace` | Namespace for code location resources (default `dagster`). |
| `image_pull_policy` | Image pull policy for pods (default `IfNotPresent`). |
| `deployment_startup_timeout` | Seconds to wait for pod readiness (default `300`). |
| `labels` / `annotations` | Base labels/annotations applied to all resources. |
| `resources` | K8s resource requirements (requests/limits). |
| `node_selector` | Node selector for pod scheduling. |
| `service_account_name` | Service account for the pods. |
| `env_vars` | Base environment variables for all containers. |

#### `docker`

Runs each location as a Docker container on the agent host. Requires a Docker
daemon and an image (set the location's `image` field).

| Config option | Description |
| ------------------------------ | --------------------------------------------------------------- |
| `networks` | Docker network names to attach containers to. |
| `env_vars` | Base environment variables for all containers. |
| `container_kwargs` | Extra kwargs for `containers.run()` (e.g. `mem_limit`, `cpus`). |
| `image_registry` | Optional registry prefix (e.g. `ghcr.io/myorg/`). |
| `server_process_startup_timeout` | Seconds to wait for gRPC readiness (default `60`). |
| `container_name_prefix` | Prefix for container names (default `dagster-loc-`). |

### Agent REST API

All endpoints live under `/api/agent/*`, require authentication (API key via
`Authorization: Bearer <key>` or an active session), and are scoped per
request by the caller's labels / allowed locations.

| Endpoint | Method | Purpose |
| --------------------------------------- | ------ | ------------------------------------------------------ |
| `/api/agent/workspace-entries` | GET | Code locations this agent should manage (status `active`). |
| `/api/agent/enqueue-request` | POST | Enqueue a work request (used by the UI/daemon). |
| `/api/agent/pop-requests` | POST | Pop pending work requests for this agent. |
| `/api/agent/request-status/{id}` | GET | Check the status of a work request. |
| `/api/agent/complete-request/{id}` | POST | Mark a work request complete with its response. |
| `/api/agent/fail-request/{id}` | POST | Mark a work request failed. |
| `/api/agent/heartbeat` | POST | Report agent health and code server endpoints. |
| `/api/agent/upload-data` | POST | Upload loaded repository data for a location. |
| `/api/agent/check-snapshot` | GET | Check whether a stored snapshot matches (SHA1 dedup). |
| `/api/agent/upload-snapshot` | POST | Store a repository snapshot. |
| `/api/agent/code-locations` | GET | List code location definitions. |
| `/api/agent/code-locations` | POST | Create a code location definition. |
| `/api/agent/code-locations/{name}` | PUT | Update a code location (deploy metadata, labels, status). |
| `/api/agent/code-locations/{name}` | DELETE | Delete a code location definition. |

## Authentication and RBAC

![login page](./assets/login.png)

![main page](./assets/main.png)

![admin dashboard](./assets/dashboard.png)

dagster-webserver supports optional user login with role-based access control.
Auth is **disabled by default** — enable it with the `--auth-provider` flag.

### Auth Provider Options

| Provider | Description |
| ---------- | ----------------------------------------------------------------------------------------------- |
| `none` | No authentication (default) |
| `session` | Cookie-based sessions with username/password (file or in-memory store) |
| `api-key` | Bearer token auth for programmatic access |
| `database` | Cookie-based sessions backed by a relational database (SQLite / PostgreSQL) |
| `hybrid` | Database-backed sessions with full OIDC support (Google, Okta, Auth0, Azure AD, Keycloak, etc.) |

### Session-based auth (file-backed)

```sh
dagster-webserver start --auth-provider session --session-secret my-secret-key
```

This starts the webserver with cookie-based sessions. A default `admin` user
(with password `admin`) is created. On first visit the browser will be
redirected to a login page at `/login`.

### Specifying users from a file

Create a YAML file (e.g. `users.yaml`):

```yaml
users:
  admin:
    password: changeme
    role: admin
    email: admin@example.com
  editor:
    password: editor-pass
    role: editor
  viewer:
    password: viewer-pass
    role: viewer
```

Then start the webserver:

```sh
dagster-webserver start --auth-provider session --users-file users.yaml --session-secret my-secret-key
```

### Roles

Five built-in roles are available, modelled after Dagster+ cloud:

| Permission | catalog_viewer | viewer | launcher | editor | admin |
| ---------------- | -------------- | ------ | -------- | ------ | ----- |
| Launch runs | ✗ | ✗ | ✓ | ✓ | ✓ |
| Re-execute | ✗ | ✗ | ✓ | ✓ | ✓ |
| Terminate runs | ✗ | ✗ | ✓ | ✓ | ✓ |
| Start schedule | ✗ | ✗ | ✗ | ✓ | ✓ |
| Edit sensors | ✗ | ✗ | ✗ | ✓ | ✓ |
| Reload workspace | ✗ | ✗ | ✗ | ✗ | ✓ |
| Wipe assets | ✗ | ✗ | ✗ | ✗ | ✓ |

Custom permissions can be defined per user by setting `role: custom` and
providing an explicit `custom_permissions` map in the users file.

### Database-backed auth

For deployments that need runtime user management (create, update, delete
users without restarting), use the `database` auth provider:

```sh
# SQLite (dev)
dagster-webserver start --auth-provider database \
  --auth-database-url sqlite+aiosqlite:///auth.db \
  --session-secret my-secret-key

# PostgreSQL (production)
dagster-webserver start --auth-provider database \
  --auth-database-url postgresql+asyncpg://user:pass@host/db \
  --session-secret my-secret-key
```

The first time the server starts, it creates the `roles` and `users`
tables and seeds the five built-in roles automatically.

Bootstrap the first admin user:

```sh
dagster-webserver db init-admin \
  --username admin \
  --password changeme \
  --database-url sqlite+aiosqlite:///auth.db
```

#### Managing custom roles

Custom roles are first-class entities stored in the database. Create one:

```sh
dagster-webserver db create-role \
  --name analyst \
  --permissions '{"LAUNCH_PIPELINE_EXECUTION": true, "LAUNCH_PIPELINE_REEXECUTION": true}' \
  --database-url sqlite+aiosqlite:///auth.db
```

List all roles (built-in and custom):

```sh
dagster-webserver db list-roles --database-url sqlite+aiosqlite:///auth.db
```

Update or delete custom roles:

```sh
dagster-webserver db update-role --name analyst --permissions '{...}' --database-url sqlite+aiosqlite:///auth.db
dagster-webserver db delete-role --name analyst --database-url sqlite+aiosqlite:///auth.db
```

#### Database migrations

Manage the auth database schema with Alembic:

```sh
# Run all pending migrations
dagster-webserver db migrate --database-url sqlite+aiosqlite:///auth.db

# Upgrade/downgrade to a specific revision
dagster-webserver db upgrade -r head --database-url sqlite+aiosqlite:///auth.db
dagster-webserver db downgrade -r -1 --database-url sqlite+aiosqlite:///auth.db

# Check migration status
dagster-webserver db current --database-url sqlite+aiosqlite:///auth.db
dagster-webserver db history --database-url sqlite+aiosqlite:///auth.db
dagster-webserver db check --database-url sqlite+aiosqlite:///auth.db

# Stamp the database with a revision (skip migration execution)
dagster-webserver db stamp -r head --database-url sqlite+aiosqlite:///auth.db
```

#### Dependencies

- SQLite (dev): `pip install dagster-webserver[auth]`
- PostgreSQL (production): `pip install dagster-webserver[auth-db]`

#### Default role fallback

If a user is created without an explicit role assignment (`role_id` is
`NULL`), the `--default-role` flag determines their effective role.
This defaults to `viewer`.

### Hybrid auth with OIDC

The `hybrid` auth provider extends database-backed auth with OpenID Connect
support. Users can authenticate via external identity providers (Google, Okta,
Auth0, Azure AD, Keycloak, etc.) in addition to username/password login.
OIDC provider configurations are stored in the database and managed through
the admin portal. OIDC buttons appear dynamically on the login page.

```sh
dagster-webserver start --auth-provider hybrid \
  --auth-database-url sqlite+aiosqlite:///auth.db \
  --session-secret my-secret-key \
  --enable-admin-portal
```

Install OIDC dependencies:

```sh
pip install dagster-webserver[auth-oidc]
```

#### Configuring OIDC providers

After starting with `--enable-admin-portal`, navigate to `/admin` and use the
**OIDC Providers** section to add providers. Each provider requires:

- **Name** — internal identifier (e.g. `google`, `okta`)
- **Display name** — human-readable name (e.g. `Google`)
- **Issuer URL** — OIDC issuer URL (e.g. `https://accounts.google.com`)
- **Client ID** — from your IdP application registration
- **Client Secret** — from your IdP application registration
- **Scopes** — space-separated OIDC scopes (default: `openid email profile`)

Providers support PKCE, state-based CSRF protection, and auto-provisioning of
new users. Existing users can be linked to OIDC via email matching.

### API key auth

For programmatic access (CI/CD, scripts, agents), use the `api-key` provider:

```sh
dagster-webserver start --auth-provider api-key --users-file users.yaml
```

Then authenticate requests with a `Bearer` token:

```sh
curl -H "Authorization: Bearer <api-token>" http://localhost:3000/graphql
```

With `database`/`hybrid` auth, API keys are managed per user (or service
account) in the admin portal under **API Keys** and work against both the
GraphQL endpoint and the `/api/agent/*` REST API.

### Admin portal

The admin portal provides a web UI for managing users, roles, OIDC providers,
service accounts, API keys, and code locations. It is accessible at `/admin`
and requires the `database` or `hybrid` auth provider.

```sh
dagster-webserver start --auth-provider hybrid \
  --auth-database-url sqlite+aiosqlite:///auth.db \
  --session-secret my-secret-key \
  --enable-admin-portal
```

You can also use a separate database for the admin portal:

```sh
dagster-webserver start --auth-provider hybrid \
  --auth-database-url sqlite+aiosqlite:///auth.db \
  --admin-database-url sqlite+aiosqlite:///admin.db \
  --session-secret my-secret-key \
  --enable-admin-portal
```

The portal shares the main app's design tokens and follows the theme selected
in Settings (light/dark).

#### Portal sections

| Section | Purpose |
| ------------------ | --------------------------------------------------------------------- |
| **Dashboard** | Landing page with links to each section. |
| **Users** | Create, edit, and delete human users. |
| **Service Accounts** | Machine identities for agents; API-key auth only. |
| **API Keys** | Issue and revoke Bearer tokens per user/service account. |
| **Roles** | Built-in and custom roles with permission sets. |
| **OIDC Providers** | External identity provider configurations. |
| **Code Locations** | Definitions managed by agents (see [Managing code locations](#managing-code-locations)). |

#### Admin permissions

Admin permissions control access to the portal:

| Permission | Description |
| ---------------------------- | ------------------------------------------------------ |
| `ADMIN_VIEW_USERS` | View the user list |
| `ADMIN_EDIT_USERS` | Create, edit, and delete users (implies view) |
| `ADMIN_VIEW_ROLES` | View the role list |
| `ADMIN_EDIT_ROLES` | Create, edit, and delete roles (implies view) |
| `ADMIN_VIEW_OIDC` | View OIDC providers |
| `ADMIN_EDIT_OIDC` | Create, edit, and delete OIDC providers (implies view) |
| `ADMIN_VIEW_CODE_LOCATIONS` | View the code location list |
| `ADMIN_EDIT_CODE_LOCATIONS` | Create, edit, and delete code locations (implies view) |

By default, only the `admin` role has all admin permissions enabled.

### CLI options for auth

| Option | Description |
| -------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--auth-provider {session,api-key,database,hybrid,none}` | Authentication mode. `none` is the default (no auth). |
| `--auth-database-url URL` | SQLAlchemy connection string for the auth database (e.g. `sqlite+aiosqlite:///auth.db` or `postgresql+asyncpg://...`). Required when `--auth-provider=database` or `--auth-provider=hybrid`. |
| `--users-file PATH` | Path to a YAML or JSON file defining users, passwords, and roles. |
| `--session-secret SECRET` | Secret key for signing session cookies. Also available via the `DAGSTER_WEBSERVER_SESSION_SECRET` environment variable. |
| `--default-role {catalog_viewer,viewer,launcher,editor,admin}` | Fallback role for users with no explicit role. Defaults to `viewer`. |
| `--enable-admin-portal` | Enable the admin portal at `/admin`. Requires `--auth-provider=database` or `--auth-provider=hybrid` with `--auth-database-url`. |
| `--admin-database-url URL` | SQLAlchemy URL for the admin portal database. Defaults to `--auth-database-url` if not specified. |
| `--agent-workspaces` | Load code locations from agent heartbeats instead of a workspace file. Requires `--auth-database-url`. Mutually exclusive with explicit workspace options. Env var: `DAGSTER_WEBSERVER_AGENT_WORKSPACES`. |
| `--agent-workspace-file PATH` | Path of the maintained workspace file for external consumers (e.g. `dagster-daemon`). Defaults to `$DAGSTER_HOME/agent_workspace.yaml`. Env var: `DAGSTER_AGENT_WORKSPACE_FILE`. |

### Sign out

When auth is enabled, a **Sign out** button appears in the left navigation
sidebar (between _Collapse_ and _Settings_). Clicking it clears the session
and redirects to the login page.

## CLI Reference

### `dagster-webserver start`

Start the Dagster webserver.

| Option | Description |
| ---------------------------------- | ------------------------------------------------------------------------------------------- |
| `-h, --host HOST` | Host to run server on (default: `127.0.0.1`) |
| `-p, --port PORT` | Port to run server on (default: `3000`) |
| `-l, --path-prefix PREFIX` | Path prefix where server will be hosted (e.g. `/dagster-webserver`) |
| `--read-only` | Start in read-only mode (no run launches or schedule mutations) |
| `--suppress-warnings` | Filter all warnings |
| `--uvicorn-log-level LEVEL` | Log level for the uvicorn server (`critical`, `error`, `warning`, `info`, `debug`, `trace`) |
| `--dagster-log-level LEVEL` | Log level for dagster log events (`critical`, `error`, `warning`, `info`, `debug`) |
| `--log-format {colored,json,rich}` | Format of log output (default: `colored`) |
| `--code-server-log-level LEVEL` | Log level for code servers (default: `info`) |
| `--live-data-poll-rate MS` | Rate at which the UI polls for updated asset data in milliseconds (default: `2000`) |
| `--db-statement-timeout MS` | Timeout in milliseconds for database statements (default: `15000`) |
| `--db-pool-recycle SECS` | Max age of a SQLAlchemy pool connection in seconds (default: `3600`) |
| `--db-pool-max-overflow N` | Max overflow size of the SQLAlchemy pool (default: `20`) |

Workspace options (via `-w`, `-f`, `-m`, `-a`, `-d`; ignored when
`--agent-workspaces` is set):

| Option | Description |
| ------------------------------ | -------------------------------------------------------- |
| `-w, --workspace PATH` | Path to a workspace YAML file |
| `-f, --python-file PATH` | Path to a Python file containing a repository definition |
| `-m, --module-name NAME` | Python module name containing a repository definition |
| `-a, --attribute NAME` | Attribute name to locate the repository definition |
| `-d, --working-directory PATH` | Working directory for loading code locations |

Auth and agent options are documented in [CLI options for auth](#cli-options-for-auth).

Environment variables prefixed with `DAGSTER_WEBSERVER` can be used for most options. For example:

```sh
DAGSTER_WEBSERVER_PORT=3333 DAGSTER_WEBSERVER_SESSION_SECRET=mysecret dagster-webserver start
```

### `dagster-webserver db`

Manage the auth database.

| Subcommand | Description |
| ------------- | ------------------------------------------------------------- |
| `init-admin` | Bootstrap the first admin user |
| `create-role` | Create a new custom role |
| `list-roles` | List all roles (built-in and custom) |
| `update-role` | Update a custom role's permissions |
| `delete-role` | Delete a custom role |
| `migrate` | Run all pending database migrations |
| `upgrade` | Upgrade to a specific revision |
| `downgrade` | Downgrade to a specific revision |
| `current` | Show the current database revision |
| `history` | List all available migration revisions |
| `stamp` | Stamp the database with a revision without running migrations |
| `check` | Check if the database schema is up to date |

All `db` subcommands require the `--database-url` option (or the
`DAGSTER_AUTH_DATABASE_URL` environment variable).

### `dagster-agent run`

Start the Dagster agent process (see [Dagster Agent](#dagster-agent) for
setup and configuration).

| Option | Description |
| ---------------------- | ------------------------------------------------------------- |
| `--dagster-home PATH` | Path to DAGSTER_HOME containing `dagster.yaml`. Env var: `DAGSTER_HOME`. |
| `--log-level {debug,info,warning,error}` | Logging level (default: `info`). |
