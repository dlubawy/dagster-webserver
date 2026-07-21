# Dagster Webserver

Web UI for Dagster.

## Quick Start

```sh
dagster-webserver start -p 3000
```

Running with a workspace file:

```sh
dagster-webserver start -w path/to/workspace.yaml
```

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

For programmatic access (CI/CD, scripts), use the `api-key` provider:

```sh
dagster-webserver start --auth-provider api-key --users-file users.yaml
```

Then authenticate requests with a `Bearer` token:

```sh
curl -H "Authorization: Bearer <api-token>" http://localhost:3000/graphql
```

### Admin portal

The admin portal provides a web UI for managing users, roles, and (with hybrid
auth) OIDC providers. It is accessible at `/admin` and requires the
`database` or `hybrid` auth provider.

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

#### Admin permissions

Four admin permissions control access to the portal:

| Permission | Description |
| ------------------ | ------------------------------------------------------ |
| `ADMIN_VIEW_USERS` | View the user list |
| `ADMIN_EDIT_USERS` | Create, edit, and delete users (implies view) |
| `ADMIN_VIEW_ROLES` | View the role list |
| `ADMIN_EDIT_ROLES` | Create, edit, and delete roles (implies view) |
| `ADMIN_VIEW_OIDC` | View OIDC providers |
| `ADMIN_EDIT_OIDC` | Create, edit, and delete OIDC providers (implies view) |

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

Workspace options (via `-w`, `-f`, `-m`, `-a`, `-d`):

| Option | Description |
| ------------------------------ | -------------------------------------------------------- |
| `-w, --workspace PATH` | Path to a workspace YAML file |
| `-f, --python-file PATH` | Path to a Python file containing a repository definition |
| `-m, --module-name NAME` | Python module name containing a repository definition |
| `-a, --attribute NAME` | Attribute name to locate the repository definition |
| `-d, --working-directory PATH` | Working directory for loading code locations |

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
