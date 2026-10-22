# Webserver Integration — APIs for DagsterAgent Support

## Goal

Extend the forked `dagster-webserver` with GraphQL mutations and REST endpoints that the `DagsterAgent` process needs to:

1. Query workspace entries (code locations to manage).
1. Pop work requests from a queue.
1. Submit heartbeats.
1. Upload code location data (loaded repository manifests).

______________________________________________________________________

## 1. Current Webserver Architecture

The webserver fork already extends the OSS `DagsterWebserver` with:

- **Auth system**: Session-based auth, OIDC, RBAC roles.
- **Admin portal**: User/role management UI.
- **Database layer**: SQLAlchemy + Alembic for auth data.

The webserver uses `dagster-graphql`'s schema (`create_schema()`) which provides the standard OSS GraphQL API. We need to extend this schema with agent-specific resolvers.

______________________________________________________________________

## 2. Required New Database Tables

### `agent_heartbeats` — Track Agent Health

```sql
CREATE TABLE agent_heartbeats (
    id SERIAL PRIMARY KEY,
    agent_id VARCHAR(64) NOT NULL,           -- Unique agent identifier
    agent_labels JSONB,                       -- Agent labels at heartbeat time
    agent_type VARCHAR(64),                   -- e.g., "ProcessUserCodeLauncher"
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    metadata JSONB,                           -- Version, utilization metrics, etc.
    errors JSONB,                             -- Recent errors (array of error objects)
    code_server_heartbeats JSONB,             -- Per-location server health
    run_worker_statuses JSONB                 -- Run worker health per deployment

    -- Index for querying recent heartbeats
);
CREATE INDEX idx_agent_heartbeats_agent_id_timestamp ON agent_heartbeats (agent_id, timestamp DESC);
```

### `agent_requests` — Work Queue for Agents

```sql
CREATE TABLE agent_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_api VARCHAR(64) NOT NULL,         -- e.g., "LAUNCH_RUN", "GET_EXTERNAL_SENSOR_EXECUTION_DATA"
    request_body TEXT NOT NULL,               -- Serialized DagsterCloudApiRequest body
    location_name VARCHAR(256),               -- Target code location (nullable for some APIs)
    labels JSONB,                             -- Routing labels (agent must match)
    status VARCHAR(16) NOT NULL DEFAULT 'pending',  -- pending → processing → completed
    assigned_to_agent VARCHAR(64),            -- Agent ID that popped this request
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    popped_at TIMESTAMP WITH TIME ZONE,       -- When agent claimed the request
    expires_at TIMESTAMP WITH TIME ZONE,      -- TTL for stale request cleanup
    response_body TEXT                        -- Serialized response (for debugging)
);
CREATE INDEX idx_agent_requests_status_labels ON agent_requests (status, labels);
CREATE INDEX idx_agent_requests_expires_at ON agent_requests (expires_at) WHERE status = 'pending';
```

### `code_location_definitions` — Workspace Entries Managed by Webserver

This table stores code location definitions that agents should deploy. It replaces the Dagster Cloud concept of "workspace entries" stored in the control plane.

```sql
CREATE TABLE code_location_definitions (
    id SERIAL PRIMARY KEY,
    location_name VARCHAR(256) NOT NULL UNIQUE,
    deploy_metadata JSONB NOT NULL,           -- Serialized CodeLocationDeployData
    labels JSONB,                             -- Routing labels for agent assignment
    status VARCHAR(32) NOT NULL DEFAULT 'active',  -- active, paused, removed
    last_loaded_at TIMESTAMP WITH TIME ZONE,  -- When data was last uploaded by agent
    load_error JSONB,                         -- Last error from the agent (if any)
    loaded_manifest JSONB,                    -- Repository manifest from last successful load
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_code_location_definitions_status ON code_location_definitions (status);
```

### `code_location_snapshots` — Loaded Repository Data

Stores the repository/job snapshots uploaded by agents. This replaces the Cloud blob storage approach.

```sql
CREATE TABLE code_location_snapshots (
    id SERIAL PRIMARY KEY,
    location_name VARCHAR(256) NOT NULL,
    snapshot_type VARCHAR(32) NOT NULL,       -- 'repository', 'job', 'error'
    sha1_hash VARCHAR(40) NOT NULL,           -- Content hash for dedup
    size_bytes BIGINT NOT NULL,
    data BYTEA,                               -- Serialized snapshot content (can be large)
    stored_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(location_name, snapshot_type, sha1_hash)
);
CREATE INDEX idx_snapshots_hash ON code_location_snapshots (sha1_hash);
```

______________________________________________________________________

## 3. New GraphQL Resolvers

### 3.1 `workspaceEntries` Query

Returns the code locations that agents should manage.

```graphql
type Query {
  """
  Return workspace entries (code location definitions) for agents.
  """
  agentWorkspaceEntries(
    agentLabels: [AgentLabelInput!] # Filter by labels this agent serves
    locationNames: [String!] # Optionally filter to specific locations
  ): [AgentWorkspaceEntry!]!
}

input AgentLabelInput {
  key: String!
  value: String!
}

type AgentWorkspaceEntry {
  locationName: String!
  serializedDeploymentMetadata: String! # Serialized CodeLocationDeployData
  hasOutdatedData: Boolean! # True if agent should re-upload data
  hasLoadError: Boolean! # True if last load had an error
  metadataTimestamp: Float! # Unix timestamp of last update
  labels: [AgentLabelInput!] # Routing labels for this location
}
```

**Resolver logic:**

```python
def resolve_agent_workspace_entries(self, info, agent_labels=None, location_names=None):
    query = CodeLocationDefinition.query.filter_by(status='active')

    if location_names:
        query = query.filter(CodeLocationDefinition.location_name.in_(location_names))

    # Filter by agent labels — only return locations the agent is allowed to serve
    if agent_labels:
        label_dict = {l.key: l.value for l in agent_labels}
        # Match locations whose labels are a subset of the agent's labels
        query = query.filter(
            CodeLocationDefinition.labels.contains(label_dict) |
            (CodeLocationDefinition.labels == null())  # No labels = all agents can serve
        )

    entries = query.all()
    return [
        AgentWorkspaceEntry(
            locationName=entry.location_name,
            serializedDeploymentMetadata=serialize_value(entry.deploy_metadata),
            hasOutdatedData=is_data_outdated(entry),
            hasLoadError=entry.load_error is not None,
            metadataTimestamp=entry.updated_at.timestamp(),
            labels=entry.labels,
        )
        for entry in entries
    ]
```

### 3.2 `popAgentRequests` Mutation

Agents pop work items from the queue. Uses optimistic locking to ensure each request is processed by exactly one agent.

```graphql
type Mutation {
  """
  Pop pending requests from the agent work queue.
  """
  popAgentRequests(
    agentId: String!
    agentLabels: [AgentLabelInput!]
    limit: Int = 10
  ): [AgentRequest!]!
}

type AgentRequest {
  requestId: String! # UUID
  requestApi: String! # e.g., "LAUNCH_RUN"
  requestBody: String! # Serialized request args
  locationName: String # Target code location (nullable)
}
```

**Resolver logic:**

```python
def resolve_pop_agent_requests(self, info, agent_id, agent_labels=None, limit=10):
    with get_session() as session:
        query = AgentRequest.query.filter_by(status='pending')

        # Filter by labels — only return requests matching this agent's labels
        if agent_labels:
            label_dict = {l.key: l.value for l in agent_labels}
            query = query.filter(
                AgentRequest.labels.contains(label_dict) |
                (AgentRequest.labels == null())  # No labels = any agent can process
            )

        query = query.order_by(AgentRequest.created_at.asc()).limit(limit)

        # Optimistic lock: atomically claim the requests
        now = datetime.now(timezone.utc)
        rows = query.with_for_update().all()

        for row in rows:
            row.status = 'processing'
            row.assigned_to_agent = agent_id
            row.popped_at = now

        session.commit()

        return [
            AgentRequest(
                requestId=str(row.id),
                requestApi=row.request_api,
                requestBody=row.request_body,
                locationName=row.location_name,
            )
            for row in rows
        ]
```

### 3.3 `submitAgentHeartbeat` Mutation

Agents report their health status.

```graphql
type Mutation {
  """
  Submit an agent heartbeat.
  """
  submitAgentHeartbeat(
    agentId: String!
    agentLabels: [AgentLabelInput!]
    agentType: String
    metadata: String # Serialized metadata (version, utilization)
    errors: [String!] # Recent serialized errors
    codeServerHeartbeats: [String!] # Per-location server heartbeats
    runWorkerStatuses: String # Run worker statuses
  ): Boolean!
}
```

**Resolver logic:**

```python
def resolve_submit_agent_heartbeat(self, info, agent_id, **kwargs):
    heartbeat = AgentHeartbeat(
        agent_id=agent_id,
        agent_labels={l.key: l.value for l in kwargs.get("agentLabels", [])},
        agent_type=kwargs.get("agentType"),
        metadata=kwargs.get("metadata"),
        errors=kwargs.get("errors", []),
        code_server_heartbeats=kwargs.get("codeServerHeartbeats", []),
        run_worker_statuses=kwargs.get("runWorkerStatuses"),
    )
    session.add(heartbeat)
    session.commit()
    return True
```

### 3.4 `uploadCodeLocationData` Mutation

Agents upload loaded repository manifests for code locations they manage.

```graphql
type Mutation {
  """
  Upload loaded code location data (repository manifests, etc.).
  """
  uploadCodeLocationData(
    agentId: String!
    locationName: String!
    serializedManifest: String! # Serialized DagsterCloudUploadLocationData or error
    isError: Boolean = false # True if uploading an error instead of data
  ): UploadCodeLocationResult!
}

type UploadCodeLocationResult {
  updated: Boolean! # True if data was stored
  message: String! # Status message
  missingJobSnapshots: [JobSelectorInput!] # Job snapshots that need to be uploaded separately
}
```

**Resolver logic:**

```python
def resolve_upload_code_location_data(self, info, agent_id, location_name, serialized_manifest, is_error=False):
    with get_session() as session:
        # Find or create the code location definition
        entry = CodeLocationDefinition.query.filter_by(
            location_name=location_name
        ).first()

        if not entry:
            return UploadCodeLocationResult(updated=False, message=f"Location {location_name} not found", missingJobSnapshots=[])

        if is_error:
            # Store error info
            entry.load_error = deserialize_value(serialized_manifest)
            entry.loaded_manifest = None
        else:
            # Store manifest
            manifest = deserialize_value(serialized_manifest)
            entry.loaded_manifest = manifest
            entry.last_loaded_at = datetime.now(timezone.utc)
            entry.load_error = None

            # Store individual snapshots for dedup
            for repo_data in manifest.get("upload_repository_datas", []):
                sha1 = hashlib.sha1(repo_data["serialized_repository_data"].encode()).hexdigest()
                existing = CodeLocationSnapshot.query.filter_by(
                    location_name=location_name,
                    snapshot_type='repository',
                    sha1_hash=sha1
                ).first()

                if not existing:
                    session.add(CodeLocationSnapshot(
                        location_name=location_name,
                        snapshot_type='repository',
                        sha1_hash=sha1,
                        size_bytes=len(repo_data["serialized_repository_data"]),
                        data=repo_data["serialized_repository_data"].encode(),
                    ))

        entry.updated_at = datetime.now(timezone.utc)
        session.commit()

        return UploadCodeLocationResult(
            updated=True,
            message=f"Location {location_name} data updated",
            missingJobSnapshots=[]  # Could check for missing job snapshots here
        )
```

______________________________________________________________________

## 4. REST API Endpoints (Optional)

For large data transfers (snapshot uploads), REST endpoints may be more efficient than GraphQL:

### `POST /api/agent/upload-snapshot`

Upload a single snapshot (repository data, job snapshot, error).

```python
@router.post("/upload-snapshot")
async def upload_snapshot(
    location_name: str = Form(...),
    snapshot_type: str = Form(...),  # 'repository', 'job', 'error'
    data: UploadFile = File(...),
):
    """Upload a serialized snapshot for a code location."""
    content = await data.read()
    sha1 = hashlib.sha1(content).hexdigest()

    existing = session.query(CodeLocationSnapshot).filter_by(
        location_name=location_name,
        snapshot_type=snapshot_type,
        sha1_hash=sha1,
    ).first()

    if existing:
        return {"stored": True, "sha1": sha1}

    session.add(CodeLocationSnapshot(
        location_name=location_name,
        snapshot_type=snapshot_type,
        sha1_hash=sha1,
        size_bytes=len(content),
        data=content,
    ))
    session.commit()

    return {"stored": True, "sha1": sha1}
```

### `GET /api/agent/check-snapshot`

Check if a snapshot already exists (before uploading).

```python
@router.get("/check-snapshot")
async def check_snapshot(
    location_name: str = Query(...),
    snapshot_type: str = Query(...),
    sha1: str = Query(...),
    size: int = Query(...),
):
    """Check if a snapshot with the given hash already exists."""
    existing = session.query(CodeLocationSnapshot).filter_by(
        location_name=location_name,
        snapshot_type=snapshot_type,
        sha1_hash=sha1,
    ).first()

    if existing:
        return {"exists": True, "sha1": sha1}
    else:
        return {"exists": False, "uploadUrl": f"/api/agent/upload-snapshot"}
```

______________________________________________________________________

## 5. Request Queue Management

### How Requests Get Into the Queue

The request queue is populated by:

1. **GraphQL mutations from the UI** (e.g., `launchRun`):

   ```graphql
   mutation LaunchRun {
     enqueueAgentRequest(
       requestApi: "LAUNCH_RUN"
       requestBody: "{...serialized DagsterRun...}"
       locationName: "my-location"
       labels: [{ key: "deployment", value: "production" }]
     ) {
       requestId
     }
   }
   ```

1. **Daemon requests** (sensors, schedules):
   The daemon process submits requests to the queue via GraphQL instead of directly calling code servers. This is a key architectural change from OSS.

1. **Direct API calls**:
   External systems can enqueue requests via the REST API.

### Request Lifecycle

```
pending → processing (assigned to agent) → completed/failed
  ↑                                              |
  └────── expired (TTL exceeded, re-queued) ──────┘
```

A background task on the webserver:

1. Finds requests with `status='processing'` and `expires_at < NOW()`.
1. Re-queues them as `pending` (the agent processing them may have crashed).

______________________________________________________________________

## 6. Extending the dagster-graphql Schema

The webserver currently uses `dagster_graphql.schema.create_schema()`. To add our resolvers, we extend it:

```python
# In dagster_webserver/graphql.py or a new module

import graphene
from dagster_graphql.schema import create_schema as create_base_schema


class AgentWorkspaceEntry(graphene.ObjectType):
    location_name = graphene.String(required=True)
    serialized_deployment_metadata = graphene.String(required=True)
    has_outdated_data = graphene.Boolean(required=True)
    has_load_error = graphene.Boolean(required=True)
    metadata_timestamp = graphene.Float(required=True)
    labels = graphene.List(graphene.String, required=False)


class AgentRequest(graphene.ObjectType):
    request_id = graphene.ID(required=True)
    request_api = graphene.String(required=True)
    request_body = graphene.String(required=True)
    location_name = graphene.String(required=False)


class AgentLabelInput(graphene.InputObjectType):
    key = graphene.String(required=True)
    value = graphene.String(required=True)


class QueryExtension(graphene.ObjectType):
    agent_workspace_entries = graphene.List(
        AgentWorkspaceEntry,
        agent_labels=graphene.List(AgentLabelInput),
        location_names=graphene.List(graphene.String),
    )

    def resolve_agent_workspace_entries(self, info, agent_labels=None, location_names=None):
        # Implementation from section 3.1
        ...


class MutationExtension(graphene.ObjectType):
    pop_agent_requests = graphene.List(
        AgentRequest,
        agent_id=graphene.String(required=True),
        agent_labels=graphene.List(AgentLabelInput),
        limit=graphene.Int(default_value=10),
    )

    submit_agent_heartbeat = graphene.Boolean(
        agent_id=graphene.String(required=True),
        agent_labels=graphene.List(AgentLabelInput),
        agent_type=graphene.String(),
        metadata=graphene.String(),
        errors=graphene.List(graphene.String),
        code_server_heartbeats=graphene.List(graphene.String),
        run_worker_statuses=graphene.String(),
    )

    upload_code_location_data = graphene.Field(
        UploadCodeLocationResult,
        agent_id=graphene.String(required=True),
        location_name=graphene.String(required=True),
        serialized_manifest=graphene.String(required=True),
        is_error=graphene.Boolean(default_value=False),
    )

    enqueue_agent_request = graphene.Field(
        AgentRequest,
        request_api=graphene.String(required=True),
        request_body=graphene.String(required=True),
        location_name=graphene.String(),
        labels=graphene.List(AgentLabelInput),
        ttl_seconds=graphene.Int(default_value=600),
    )


# Merge our extensions with the base schema
def create_schema():
    base_schema = create_base_schema()
    return graphene.Schema(
        query=graphene.ObjectType(type("ExtendedQuery", (QueryExtension, base_schema.query), {})),
        mutation=graphene.ObjectType(type("ExtendedMutation", (MutationExtension, base_schema.mutation), {})),
        subscription=base_schema.subscription,
    )
```

______________________________________________________________________

## 7. Code Location Management API

The webserver needs CRUD operations for managing code location definitions. These can be exposed via:

### GraphQL Mutations

```graphql
type Mutation {
  """
  Create or update a code location definition.
  """
  upsertCodeLocation(
    locationName: String!
    deployMetadata: String! # Serialized CodeLocationDeployData
    labels: [AgentLabelInput!] # Routing labels
  ): CodeLocationDefinition!

  """
  Pause a code location (agent will tear down its server).
  """
  pauseCodeLocation(locationName: String!): Boolean!

  """
  Resume a paused code location.
  """
  resumeCodeLocation(locationName: String!): Boolean!

  """
  Remove a code location definition.
  """
  removeCodeLocation(locationName: String!): Boolean!
}
```

### Admin Portal Integration

The existing admin portal can be extended with a "Code Locations" view:

- List all code locations with their status (active/paused/error).
- Create/edit/remove code locations.
- View agent assignments and health.

______________________________________________________________________

## 8. Authentication for Agent-to-Webserver Communication

Options for securing agent → webserver communication:

### Option A: Shared Secret (Simplest)

Agent includes a token in requests; webserver validates it.

```yaml
# Agent config
agent:
  webserver_url: "http://dagster-webserver:3000"
  api_token: "super-secret-agent-token"
```

### Option B: mTLS (Most Secure)

Mutual TLS certificates between agent and webserver. Suitable for k8s deployments with service mesh.

### Option C: API Key + RBAC

Agent authenticates as a service account with specific permissions. Leverages existing RBAC system.

**Recommendation**: Start with Option A (shared secret) for simplicity, add mTLS support later for production deployments.

______________________________________________________________________

## 9. Migration Plan

### Alembic Migration `003_create_agent_tables`

```python
def upgrade():
    op.create_table('agent_heartbeats', ...)
    op.create_table('agent_requests', ...)
    op.create_table('code_location_definitions', ...)
    op.create_table('code_location_snapshots', ...)


def downgrade():
    op.drop_table('code_location_snapshots')
    op.drop_table('code_location_definitions')
    op.drop_table('agent_requests')
    op.drop_table('agent_heartbeats')
```

______________________________________________________________________

## 10. Summary of Changes to Webserver

| Change | File(s) | Description |
| ------------------ | ------------------------------------------------------ | --------------------------------------------------------------------- |
| New DB tables | `database/alembic/versions/003_create_agent_tables.py` | Agent heartbeats, request queue, code location definitions, snapshots |
| ORM models | `database/models.py` | SQLAlchemy models for new tables |
| GraphQL extensions | `graphql.py` or new `agent_graphql.py` | Extended schema with agent resolvers |
| REST endpoints | New `api/agent.py` module | Snapshot upload/check endpoints |
| Request enqueuing | Modify existing `launchRun` mutation | Route through agent queue instead of direct gRPC |
| Background task | New `background_tasks.py` module | Expired request re-queue, stale heartbeat cleanup |
| Auth middleware | Extend existing auth | Agent token validation |
