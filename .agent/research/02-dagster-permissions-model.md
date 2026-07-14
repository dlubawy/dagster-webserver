# Dagster Permissions Model Deep Dive

## Source: `dagster._core.workspace.permissions`

### Permission Keys Enum

```python
@unique
class Permissions(str, Enum):
    LAUNCH_PIPELINE_EXECUTION = "launch_pipeline_execution"
    LAUNCH_PIPELINE_REEXECUTION = "launch_pipeline_reexecution"
    START_SCHEDULE = "start_schedule"
    STOP_RUNNING_SCHEDULE = "stop_running_schedule"
    SCHEDULE_DRY_RUN = "schedule_dry_run"
    EDIT_SENSOR = "edit_sensor"
    SENSOR_DRY_RUN = "sensor_dry_run"
    UPDATE_SENSOR_CURSOR = "update_sensor_cursor"
    TERMINATE_PIPELINE_EXECUTION = "terminate_pipeline_execution"
    DELETE_PIPELINE_RUN = "delete_pipeline_run"
    RELOAD_REPOSITORY_LOCATION = "reload_repository_location"
    RELOAD_WORKSPACE = "reload_workspace"
    WIPE_ASSETS = "wipe_assets"
    REPORT_RUNLESS_ASSET_EVENTS = "report_runless_asset_events"
    LAUNCH_PARTITION_BACKFILL = "launch_partition_backfill"
    CANCEL_PARTITION_BACKFILL = "cancel_partition_backfill"
    EDIT_DYNAMIC_PARTITIONS = "edit_dynamic_partitions"
    TOGGLE_AUTO_MATERIALIZE = "toggle_auto_materialize"
    EDIT_CONCURRENCY_LIMIT = "edit_concurrency_limit"
    EDIT_APP_MANAGED_COMPONENTS = "edit_app_managed_components"
```

### Permission Result Type

```python
class PermissionResult(
    NamedTuple("_PermissionResult", [("enabled", bool), ("disabled_reason", str | None)])
):
    def __bool__(self):
        raise Exception("Don't check a PermissionResult for truthiness - check the `enabled` property")
```

### Permission Maps

Two built-in permission sets:

1. **`VIEWER_PERMISSIONS`**: All 21 permissions = `False`
1. **`EDITOR_PERMISSIONS`**: All 21 permissions = `True`

### Location-Scoped Permissions

```python
LOCATION_SCOPED_PERMISSIONS = {
    Permissions.LAUNCH_PIPELINE_EXECUTION,
    Permissions.LAUNCH_PIPELINE_REEXECUTION,
    Permissions.START_SCHEDULE,
    Permissions.STOP_RUNNING_SCHEDULE,
    Permissions.SCHEDULE_DRY_RUN,
    Permissions.EDIT_SENSOR,
    Permissions.SENSOR_DRY_RUN,
    Permissions.UPDATE_SENSOR_CURSOR,
    Permissions.TERMINATE_PIPELINE_EXECUTION,
    Permissions.DELETE_PIPELINE_RUN,
    Permissions.RELOAD_REPOSITORY_LOCATION,
    Permissions.LAUNCH_PARTITION_BACKFILL,
    Permissions.CANCEL_PARTITION_BACKFILL,
    Permissions.EDIT_DYNAMIC_PARTITIONS,
    Permissions.REPORT_RUNLESS_ASSET_EVENTS,
    Permissions.WIPE_ASSETS,
    Permissions.EDIT_APP_MANAGED_COMPONENTS,
}
```

17 of 21 permissions can be scoped per code location. The 4 that are NOT location-scoped:

- `RELOAD_WORKSPACE` (global)
- `WIPE_ASSETS` (listed above, but also global in practice)
- `TOGGLE_AUTO_MATERIALIZE` (global)
- `EDIT_CONCURRENCY_LIMIT` (global)
- `EDIT_APP_MANAGED_COMPONENTS` (both global and location-scoped)

### Helper Functions

```python
def get_user_permissions(read_only: bool) -> Mapping[str, PermissionResult]:
    """Returns all permissions enabled=True or all enabled=False based on read_only."""

def get_location_scoped_user_permissions(read_only: bool) -> Mapping[str, PermissionResult]:
    """Returns only the LOCATION_SCOPED_PERMISSIONS subset."""
```

## Permission Resolution in Context Classes

### `BaseWorkspaceRequestContext` (abstract)

```python
@property
@abstractmethod
def permissions(self) -> Mapping[str, PermissionResult]: ...

@abstractmethod
def has_permission(self, permission: str) -> bool: ...

def has_permission_for_location(self, permission: str, location_name: str) -> bool:
    """Check location-scoped permission, fall back to global."""

def has_permission_for_selector(self, permission: str, selector) -> bool:
    """Multi-level check: global → location → owner-based."""

def has_permission_for_owners(self, permission: str, owners: Sequence[str]) -> bool:
    """Check if any owner grants the permission."""
```

### `WorkspaceRequestContext` (concrete, default)

```python
@property
def permissions(self) -> Mapping[str, PermissionResult]:
    return get_user_permissions(self._read_only)  # Binary: all True or all False

def has_permission(self, permission: str) -> bool:
    self._checked_permissions.add(permission)
    return self.permissions[permission].enabled

def permissions_for_owner(self, *, owner: str) -> Mapping[str, PermissionResult]:
    return {}  # Empty — owner-based permissions not implemented in OSS
```

## How GraphQL Uses Permissions

### Query Resolver

```python
# dagster_graphql/schema/roots/query.py
def resolve_permissions(self, graphene_info: ResolveInfo):
    permissions = graphene_info.context.permissions
    return [GraphenePermission(permission, value) for permission, value in permissions.items()]

def resolve_can_terminate_runs(self, graphene_info: ResolveInfo):
    return graphene_info.context.has_permission(Permissions.TERMINATE_PIPELINE_EXECUTION)
```

### Mutation Decorators

```python
# dagster_graphql/schema/roots/mutation.py
@require_permission_check(Permissions.LAUNCH_PIPELINE_EXECUTION)
@require_permission_check(Permissions.DELETE_PIPELINE_RUN)
@require_permission_check(Permissions.TERMINATE_PIPELINE_EXECUTION)
@check_permission(Permissions.RELOAD_WORKSPACE)
@check_permission(Permissions.TOGGLE_AUTO_MATERIALIZE)
```

Two decorator patterns:

- `@require_permission_check` — raises error if permission denied
- `@check_permission` — returns early/null if permission denied

### GraphQL Schema Permission Type

```python
# dagster_graphql/schema/permissions.py
class GraphenePermission(graphene.ObjectType):
    permission = graphene.NonNull(graphene.String)
    value = graphene.NonNull(graphene.Boolean)
    disabledReason = graphene.Field(graphene.String)
```

## Extension Points for RBAC

### 1. Custom Permission Maps

Instead of binary `VIEWER_PERMISSIONS`/`EDITOR_PERMISSIONS`, we need granular per-user permission maps. The `PermissionResult` type already supports this — we just need to populate it differently.

### 2. Role Definitions

We need a `Role` concept that maps to permission sets. The Dagster+ cloud enums provide a reference model:

```python
# From dagster_rest_resources enums
class PermissionGrant(str, Enum):
    CATALOG_VIEWER = "CATALOG_VIEWER"
    VIEWER = "VIEWER"
    LAUNCHER = "LAUNCHER"
    EDITOR = "EDITOR"
    ADMIN = "ADMIN"
    AGENT = "AGENT"
    CUSTOM = "CUSTOM"
```

### 3. Owner-Based Permissions

The `permissions_for_owner()` hook is designed for this but returns empty in OSS. We could implement it to check if the user owns specific assets/jobs.

### 4. Location-Scoped Permissions

Already supported via `read_only_locations: Mapping[str, bool]`. We'd extend this to support per-location role assignments.

## Design Recommendations

### Role Hierarchy (inspired by Dagster+)

```
CATALOG_VIEWER (minimal — read-only catalog browsing)
    ↓
VIEWER (read-only — can view everything but do nothing)
    ↓
LAUNCHER (can launch runs, view everything)
    ↓
EDITOR (can edit schedules, sensors, partitions, etc.)
    ↓
ADMIN (full access including workspace reload, wipe assets)
```

### Permission Mapping

| Permission | CATALOG_VIEWER | VIEWER | LAUNCHER | EDITOR | ADMIN |
| ---------------------------- | -------------- | ------ | -------- | ------ | ----- |
| LAUNCH_PIPELINE_EXECUTION | ✗ | ✗ | ✓ | ✓ | ✓ |
| LAUNCH_PIPELINE_REEXECUTION | ✗ | ✗ | ✓ | ✓ | ✓ |
| START_SCHEDULE | ✗ | ✗ | ✗ | ✓ | ✓ |
| STOP_RUNNING_SCHEDULE | ✗ | ✗ | ✗ | ✓ | ✓ |
| EDIT_SENSOR | ✗ | ✗ | ✗ | ✓ | ✓ |
| TERMINATE_PIPELINE_EXECUTION | ✗ | ✗ | ✓ | ✓ | ✓ |
| DELETE_PIPELINE_RUN | ✗ | ✗ | ✗ | ✓ | ✓ |
| RELOAD_REPOSITORY_LOCATION | ✗ | ✗ | ✗ | ✗ | ✓ |
| RELOAD_WORKSPACE | ✗ | ✗ | ✗ | ✗ | ✓ |
| WIPE_ASSETS | ✗ | ✗ | ✗ | ✗ | ✓ |
| EDIT_DYNAMIC_PARTITIONS | ✗ | ✗ | ✗ | ✓ | ✓ |
| TOGGLE_AUTO_MATERIALIZE | ✗ | ✗ | ✗ | ✓ | ✓ |
| EDIT_CONCURRENCY_LIMIT | ✗ | ✗ | ✗ | ✗ | ✓ |
| LAUNCH_PARTITION_BACKFILL | ✗ | ✗ | ✓ | ✓ | ✓ |
| CANCEL_PARTITION_BACKFILL | ✗ | ✗ | ✓ | ✓ | ✓ |
| REPORT_RUNLESS_ASSET_EVENTS | ✗ | ✗ | ✗ | ✓ | ✓ |
