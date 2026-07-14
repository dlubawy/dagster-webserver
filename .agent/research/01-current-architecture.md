# Current Dagster Webserver Architecture Analysis

## Overview

The Dagster webserver (`dagster-webserver`) is a standalone Starlette-based ASGI application that serves the Dagster UI. It provides a GraphQL API backed by `dagster-graphql` and a React SPA frontend. Currently, **there is no user authentication or user-level authorization** — access control is a single global binary flag (`read_only`).

## Key Source Files

```
dagster_webserver/
├── app.py              # Entry point: create_app_from_workspace_process_context()
├── webserver.py        # DagsterWebserver class — routes, middleware, endpoints
├── graphql.py          # GraphQLServer base class — HTTP/WS GraphQL endpoints
├── cli.py              # CLI entry point (dagster-webserver command)
├── external_assets.py  # Asset materialization/check/observation endpoints
├── debug.py            # Debug CLI helpers
├── templates/          # GraphiQL template
└── version.py          # Version info
```

## Request Flow

```
HTTP Request
  → Starlette ASGI App (created by DagsterWebserver.create_asgi_app())
    → Middleware stack:
        1. DagsterTracedCounterMiddleware (performance counters only)
    → Route matching (build_routes()):
        - /graphql (HTTP + WebSocket)
        - /server_info, /dagit_info
        - /logs/{path}, /notebook, /download_debug/{run_id}
        - /report_asset_materialization/{asset_key}
        - /report_asset_check/{asset_key}
        - /report_asset_observation/{asset_key}
        - Static files (webapp/build/)
        - /* → index_html_endpoint (SPA fallback)
```

### GraphQL Request Flow (detail)

1. `graphql_http_endpoint()` in `GraphQLServer` receives the HTTP request
1. Calls `execute_graphql_request()` → `graphql_execution_thread()`
1. `request_context(request)` context manager creates a `WorkspaceRequestContext` via `_make_request_context(conn)`
1. `_make_request_context()` delegates to `process_context.create_request_context(conn)`
1. The `WorkspaceRequestContext` is passed as `context_value` to the GraphQL executor
1. GraphQL resolvers access permissions via `graphene_info.context.has_permission(Permissions.XXX)`

### WebSocket Request Flow

1. `graphql_ws_endpoint()` accepts WebSocket connections with `graphql-ws` subprotocol
1. Same `request_context(websocket)` pattern for creating `WorkspaceRequestContext`
1. Subscription results streamed back via WebSocket messages

## Current Permission System

### Binary Global Flag

The current permission model is a **single boolean** controlled at process startup:

```python
# cli.py -- the --read-only flag
@click.option("--read-only", ...)
def dagster_webserver(..., read_only: bool, ...):
    with WorkspaceProcessContext(instance, read_only=read_only, ...) as ctx:
        ...
```

### Permission Resolution Chain

```
WorkspaceProcessContext._read_only (bool, set at startup)
  → WorkspaceRequestContext._read_only (inherited)
    → WorkspaceRequestContext.permissions property
      → get_user_permissions(read_only) [from dagster._core.workspace.permissions]
        → VIEWER_PERMISSIONS (all False) if read_only=True
        → EDITOR_PERMISSIONS (all True) if read_only=False
```

### Built-in Permission Keys (`dagster._core.workspace.permissions.Permissions`)

21 permission keys exist:

| Permission | Description |
| ------------------------------ | ---------------------------- |
| `LAUNCH_PIPELINE_EXECUTION` | Start runs |
| `LAUNCH_PIPELINE_REEXECUTION` | Re-execute runs |
| `START_SCHEDULE` | Enable schedules |
| `STOP_RUNNING_SCHEDULE` | Disable schedules |
| `SCHEDULE_DRY_RUN` | Dry-run schedules |
| `EDIT_SENSOR` | Create/edit sensors |
| `SENSOR_DRY_RUN` | Dry-run sensors |
| `UPDATE_SENSOR_CURSOR` | Reset sensor cursors |
| `TERMINATE_PIPELINE_EXECUTION` | Cancel runs |
| `DELETE_PIPELINE_RUN` | Delete run records |
| `RELOAD_REPOSITORY_LOCATION` | Reload code locations |
| `RELOAD_WORKSPACE` | Reload entire workspace |
| `WIPE_ASSETS` | Clear asset materializations |
| `REPORT_RUNLESS_ASSET_EVENTS` | Report asset events |
| `LAUNCH_PARTITION_BACKFILL` | Start backfills |
| `CANCEL_PARTITION_BACKFILL` | Cancel backfills |
| `EDIT_DYNAMIC_PARTITIONS` | Manage dynamic partitions |
| `TOGGLE_AUTO_MATERIALIZE` | Toggle auto-materialization |
| `EDIT_CONCURRENCY_LIMIT` | Edit concurrency limits |
| `EDIT_APP_MANAGED_COMPONENTS` | Edit app-managed components |

### Location-Scoped Permissions

A subset of permissions (`LOCATION_SCOPED_PERMISSIONS`) can be restricted per code location. The `WorkspaceRequestContext` supports `read_only_locations: Mapping[str, bool]` for per-location overrides.

### Owner-Based Permissions

The `BaseWorkspaceRequestContext` supports `permissions_for_owner(owner: str)` and `has_permission_for_owners()`, but the default `WorkspaceRequestContext.permissions_for_owner()` returns `{}` (empty) — owner-based permissions are a hook for Dagster+ cloud.

### GraphQL Permission Enforcement

In `dagster_graphql/schema/roots/mutation.py`, mutations are decorated with:

```python
@require_permission_check(Permissions.LAUNCH_PIPELINE_EXECUTION)
@check_permission(Permissions.RELOAD_WORKSPACE)
```

These decorators call `graphene_info.context.has_permission(Permissions.XXX)` which delegates to the `WorkspaceRequestContext`.

## Context Classes

### `IWorkspaceProcessContext` (Abstract)

Process-scoped. Created once at startup. Creates `WorkspaceRequestContext` per request.

Key methods:

- `create_request_context(source)` → `TRequestContext`
- `reload_code_location(name)`, `reload_workspace()`, `refresh_workspace()`
- `instance` property → `DagsterInstance`

### `WorkspaceProcessContext` (Concrete)

The default implementation. Holds:

- `_read_only: bool` — the global read-only flag
- `_current_workspace: CurrentWorkspace` — snapshot of all code locations
- gRPC server registry for code servers
- Watch threads for location monitoring

### `BaseWorkspaceRequestContext` (Abstract)

Request-scoped. Created per HTTP/WebSocket request. Provides:

- `permissions` → `Mapping[str, PermissionResult]`
- `has_permission(permission: str)` → `bool`
- `permissions_for_location(location_name)` → per-location permissions
- `permissions_for_owner(owner)` → owner-based permissions
- `has_permission_for_selector(permission, selector)` → asset/job/schedule/sensor level

### `WorkspaceRequestContext` (Concrete)

The default implementation. Permissions are derived from `_read_only` flag:

```python
@property
def permissions(self) -> Mapping[str, PermissionResult]:
    return get_user_permissions(self._read_only)  # All True or all False
```

## What Is Missing for User Login + RBAC

1. **No authentication mechanism** — no login, no sessions, no tokens
1. **No user identity** — no concept of "who is making this request"
1. **No per-user roles** — permissions are global, not per-user
1. **No user storage** — no user database, no credential storage
1. **No login/logout endpoints** — no `/login`, `/logout` routes
1. **No session management** — no cookie/session middleware
1. **`source` parameter is unused for auth** — `WorkspaceRequestContext._source` stores the HTTP connection but is never used for authorization decisions

## Integration Points for Adding Auth

1. **`DagsterWebserver.build_middleware()`** — add auth middleware to the Starlette middleware stack
1. **`DagsterWebserver._make_request_context()`** — inject user identity into the request context
1. **`DagsterWebserver.build_routes()`** — add `/login`, `/logout` routes
1. **`WorkspaceRequestContext` subclass** — override `permissions` to return per-user permissions
1. **`WorkspaceProcessContext` subclass** — inject auth provider, create authenticated request contexts
1. **`app.py`** — pass auth configuration to the webserver
1. **`cli.py`** — add CLI options for auth configuration (e.g., `--auth-provider`, `--users-file`)
