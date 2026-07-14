# Dagster+ Cloud RBAC Model Reference

## Source: `dagster_rest_resources/__generated__/enums.py`

The Dagster+ cloud API provides a comprehensive RBAC model that we can reference for designing OSS-compatible roles.

## PermissionGrant Enum (Built-in Roles)

```python
class PermissionGrant(str, Enum):
    CATALOG_VIEWER = "CATALOG_VIEWER"  # Minimal read-only catalog access
    VIEWER = "VIEWER"                   # Full read-only access
    LAUNCHER = "LAUNCHER"               # Can launch runs + viewer access
    EDITOR = "EDITOR"                   # Can edit schedules, sensors, etc.
    ADMIN = "ADMIN"                     # Full administrative access
    AGENT = "AGENT"                     # Machine-to-machine access
    CUSTOM = "CUSTOM"                   # User-defined permission set
```

## PermissionDeploymentScope Enum

```python
class PermissionDeploymentScope(str, Enum):
    DEPLOYMENT = "DEPLOYMENT"                        # Scoped to one deployment
    ORGANIZATION = "ORGANIZATION"                    # Scoped to entire org
    ALL_BRANCH_DEPLOYMENTS = "ALL_BRANCH_DEPLOYMENTS" # All branch deployments
```

## CustomRolePermission Enum (Granular Permissions)

34 fine-grained permissions for custom roles:

| Permission | Category |
| --------------------------------- | ---------------- |
| `EDIT_INSIGHTS_METRICS` | Analytics |
| `EDIT_USERS_AND_TEAMS` | User Management |
| `EDIT_CUSTOM_ROLES` | Role Management |
| `MANAGE_SSO_AND_SCIM` | SSO/Identity |
| `READ_AND_EDIT_AGENT_TOKENS` | API Access |
| `READ_AND_EDIT_ALL_USER_TOKENS` | API Access |
| `MANAGE_SERVICE_USERS` | Service Accounts |
| `MANAGE_BILLING` | Billing |
| `MANAGE_FULL_DEPLOYMENTS` | Deployment |
| `READ_AUDIT_LOG` | Audit |
| `EDIT_CODE_LOCATIONS` | Code Management |
| `REDEPLOY_CODE_LOCATIONS` | Code Management |
| `EDIT_ALERTS` | Alerting |
| `TOGGLE_SCHEDULES` | Scheduling |
| `TOGGLE_SENSORS` | Sensing |
| `EDIT_SENSOR_CURSORS` | Sensing |
| `EDIT_DEPLOYMENT_SETTINGS` | Deployment |
| `EDIT_DEPLOYMENT_PERMISSIONS` | RBAC |
| `EDIT_DYNAMIC_PARTITIONS` | Partitions |
| `START_AND_STOP_RUNS` | Execution |
| `DELETE_RUNS` | Execution |
| `WIPE_ASSETS` | Assets |
| `READ_SECRET_VALUES` | Secrets |
| `EDIT_SECRETS` | Secrets |
| `REPORT_ASSET_EVENTS` | Assets |
| `EDIT_CONCURRENCY_LIMITS` | Execution |
| `EDIT_ALL_CATALOG_VIEWS` | Catalog |
| `EDIT_OTHER_USERS_CATALOG_VIEWS` | Catalog |
| `MANAGE_BRANCH_DEPLOYMENTS` | Deployment |
| `EDIT_EXTERNAL_ASSET_CONNECTIONS` | Integrations |
| `EDIT_ISSUES` | Issues |
| `EDIT_APP_MANAGED_COMPONENTS` | Components |

## AuditLogEventType (Auth-Related Events)

```python
class AuditLogEventType(str, Enum):
    CHANGE_USER_PERMISSIONS = "CHANGE_USER_PERMISSIONS"
    CREATE_USER_TOKEN = "CREATE_USER_TOKEN"
    REVOKE_USER_TOKEN = "REVOKE_USER_TOKEN"
    CREATE_AGENT_TOKEN = "CREATE_AGENT_TOKEN"
    REVOKE_AGENT_TOKEN = "REVOKE_AGENT_TOKEN"
    UPDATE_AGENT_TOKEN_PERMISSIONS = "UPDATE_AGENT_TOKEN_PERMISSIONS"
    LOG_IN = "LOG_IN"
    IFRAME_LOG_IN = "IFRAME_LOG_IN"
    CREATE_SERVICE_USER = "CREATE_SERVICE_USER"
    UPDATE_SERVICE_USER = "UPDATE_SERVICE_USER"
    DELETE_SERVICE_USER = "DELETE_SERVICE_USER"
    CHANGE_SERVICE_USER_PERMISSIONS = "CHANGE_SERVICE_USER_PERMISSIONS"
    CREATE_SERVICE_TOKEN = "CREATE_SERVICE_TOKEN"
    REVOKE_SERVICE_TOKEN = "REVOKE_SERVICE_TOKEN"
```

## Feature Gates Related to RBAC

```python
class FeatureGateKey(str, Enum):
    CUSTOM_RBAC_ENABLED = "CUSTOM_RBAC_ENABLED"
    ENABLE_AUDIT_LOG_ACCESS = "ENABLE_AUDIT_LOG_ACCESS"
    ENABLE_ORG_SETTINGS_TEAMS_PAGE = "ENABLE_ORG_SETTINGS_TEAMS_PAGE"
    ENABLE_SCIM_PROVISIONING_PAGE = "ENABLE_SCIM_PROVISIONING_PAGE"
    ENABLE_SERVICE_USERS = "ENABLE_SERVICE_USERS"
    EDITOR_ADMIN_LIMIT = "EDITOR_ADMIN_LIMIT"
```

## Mapping Dagster+ Roles to OSS Permissions

### CATALOG_VIEWER → OSS

- All `Permissions.*` = False
- Can only view asset catalog (read-only GraphQL queries)

### VIEWER → OSS

- All `Permissions.*` = False (maps to `VIEWER_PERMISSIONS`)
- Full read-only access to all UI features

### LAUNCHER → OSS

- `LAUNCH_PIPELINE_EXECUTION` = True
- `LAUNCH_PIPELINE_REEXECUTION` = True
- `TERMINATE_PIPELINE_EXECUTION` = True
- `LAUNCH_PARTITION_BACKFILL` = True
- `CANCEL_PARTITION_BACKFILL` = True
- All others = False

### EDITOR → OSS

- All `Permissions.*` = True (maps to `EDITOR_PERMISSIONS`)
- Full access to all operations

### ADMIN → OSS

- All `Permissions.*` = True
- Additionally: workspace reload, wipe assets, edit concurrency limits
- In OSS, this is the same as EDITOR since there's no user management

### AGENT → OSS

- Machine-to-machine access via API tokens
- `LAUNCH_PIPELINE_EXECUTION` = True
- `REPORT_RUNLESS_ASSET_EVENTS` = True
- Minimal permissions for agent operations

### CUSTOM → OSS

- User-defined subset of any permissions
- Stored as explicit permission map

## Key Takeaways for OSS Implementation

1. **Role names should match Dagster+** for familiarity and potential migration path
1. **Permission granularity** should use the existing 21 OSS `Permissions` enum values
1. **Custom roles** should be supported as explicit permission maps
1. **Audit logging** is not needed for OSS MVP but the hook should exist
1. **Service users / API tokens** are important for CI/CD integration
1. **Scope** in OSS is per-deployment (no org/multi-deployment concept)
1. **SSO/SCIM** are out of scope for initial implementation
