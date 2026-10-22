# DagsterAgent Overview — Replacing `dagster-cloud agent run` for OSS

## Goal

Create a `dagster-agent` process (replacing `dagster-cloud agent run`) that can be deployed with **Dagster OSS** to manage code location servers. The agent:

1. Reads workspace entries (code locations) from a central source (our forked `dagster-webserver`).
1. Launches / maintains / tears down gRPC code servers per the configured `user_code_launcher` strategy (process, k8s, docker).
1. Serves requests against those code servers: launching runs, executing sensor/schedule logic, partition queries, etc.
1. Reports heartbeats and status back to the webserver.

This enables a **multi-agent architecture** where:

- **dagster-webserver** — Central control plane (our fork) with RBAC, workspace management, APIs.
- **dagster-daemon** — Standard OSS daemon for sensors, schedules, backfills, etc.
- **N × dagster-agent** — Manages the lifecycle of code location servers assigned to it.

______________________________________________________________________

## Why This Is Needed

In Dagster+, the `DagsterCloudAgent` connects to Dagster Cloud's control plane via proprietary APIs (GraphQL mutations like `popUserCloudAgentRequests`, `addAgentHeartbeats`, workspace entry queries). These APIs do not exist in OSS.

Our fork of `dagster-webserver` already adds RBAC, OIDC auth, and admin portal capabilities. To support the multi-agent pattern in OSS, we need:

1. **Control-plane APIs** on the webserver that agents can query (workspace entries, request queue, heartbeat submission).
1. A **new agent process** (`dagster-agent run`) that speaks those APIs instead of Dagster Cloud's proprietary endpoints.
1. A **user code launcher abstraction** identical in spirit to `DagsterCloudUserCodeLauncher` but decoupled from Dagster Cloud internals.

______________________________________________________________________

## High-Level Architecture

```
┌─────────────────────────────────────────────────────┐
│              dagster-webserver (fork)                │
│  ┌───────────┐  ┌───────────┐  ┌────────────────┐  │
│  │  GraphQL   │  │ Agent API │  │  Workspace     │  │
│  │  Schema    │  │ Endpoints │  │  Management    │  │
│  └─────┬──────┘  └─────┬─────┘  └───────┬────────┘  │
│        │               │                 │           │
│  ┌─────▼───────────────▼─────────────────▼────────┐  │
│  │         DagsterInstance (shared DB)            │  │
│  │  - Run storage, Event log, Schedule storage    │  │
│  │  - Agent queue, Code location definitions      │  │
│  └────────────────────────────────────────────────┘  │
└───────────────┬──────────────────────────────────────┘
                │ (HTTP/gRPC)
     ┌──────────┼──────────┐
     │          │          │
┌────▼────┐ ┌───▼───┐ ┌───▼───────────┐
│ Agent 1 │ │Agent 2│ │    Agent N     │
│ (k8s)   │ │(docker)│ │  (process)    │
│         │ │       │ │               │
│ manages │ │manages│ │ manages       │
│ loc A,B │ │loc C  │ │ loc D,E,F     │
└───┬─────┘ └───┬───┘ └───────┬───────┘
    │           │              │
    ▼           ▼              ▼
  [gRPC]      [gRPC]        [gRPC]
  servers     servers       servers
```

______________________________________________________________________

## Key Design Decisions

### 1. Agent Configuration via `dagster.yaml`

The agent reads a `dagster.yaml` file (via `DAGSTER_HOME`) just like the webserver and daemon do:

```yaml
instance_class:
  module: dagster_agent.instance
  class: DagsterAgentInstance

agent:
  webserver_url: "http://localhost:3000"
  agent_id: "agent-1" # unique identifier for this agent instance
  labels: # optional — used for routing/scoping
    deployment: "production"
    region: "us-east-1"

user_code_launcher:
  module: dagster_agent.user_code_launcher.process
  class: ProcessUserCodeLauncher
  config:
    server_ttl:
      enabled: true
      ttl_seconds: 3600
      max_servers: 25
```

### 2. `DagsterAgentInstance` — New Instance Subclass

Analogous to `DagsterCloudAgentInstance`, a new instance subclass that:

- Connects to the webserver's GraphQL API instead of Dagster Cloud APIs.
- Provides the agent queue, workspace entry queries, and heartbeat endpoints.
- Configures storage backends (runs, events) to point at the shared database.

### 3. `DagsterUserCodeLauncher` — OSS Abstraction

A base class mirroring `DagsterCloudUserCodeLauncher` but without Dagster Cloud dependencies:

- **ProcessUserCodeLauncher** — Spawns local gRPC server processes (like OSS daemon).
- **K8sUserCodeLauncher** — Deploys code locations as Kubernetes Deployments.
- **DockerUserCodeLauncher** — Runs code locations in Docker containers.

### 4. Webserver APIs Required

The webserver fork needs new GraphQL/resolver endpoints:

- `workspaceEntries` — Return code location definitions + deploy metadata.
- `popAgentRequests(limit, agentLabels)` — Dequeue work items for the agent.
- `submitAgentHeartbeat(heartbeat)` — Agent reports status.
- `uploadCodeLocationData(data)` — Agent uploads loaded repository manifests.

______________________________________________________________________

## Relationship to Existing Components

| Component | Dagster Cloud Equivalent | OSS Equivalent | Our Implementation |
| ------------------ | ------------------------------------------- | ------------------------------------- | ------------------------------------------- |
| Agent process | `DagsterCloudAgent` | N/A | New: `DagsterAgent` |
| Instance | `DagsterCloudAgentInstance` | `DagsterInstance` | New: `DagsterAgentInstance` |
| User code launcher | `DagsterCloudUserCodeLauncher` + subclasses | `GrpcServerRegistry` (in daemon) | New: `DagsterUserCodeLauncher` + subclasses |
| Control plane API | Dagster Cloud GraphQL + REST | N/A | New webserver endpoints |
| Workspace loading | Control-plane workspace entries | `dagster.yaml` / workspace YAML | Webserver-managed workspace definitions |
| Run launching | Cloud run launcher per agent type | DefaultRunLauncher / k8s job launcher | Per-launcher run launcher |

______________________________________________________________________

## Scope of This Research

The following research documents detail each component:

1. **[dagster-cloud-agent-analysis.md](./dagster-cloud-agent-analysis.md)** — Deep dive into `DagsterCloudAgent` internals.
1. **[user-code-launcher-design.md](./user-code-launcher-design.md)** — Design of the OSS user code launcher abstraction.
1. **[agent-instance-design.md](./agent-instance-design.md)** — Design of `DagsterAgentInstance` and configuration schema.
1. **[webserver-integration.md](./webserver-integration.md)** — APIs the webserver fork must expose.
1. **[daemon-considerations.md](./daemon-considerations.md)** — Analysis of whether a custom daemon is needed.
1. **[implementation-roadmap.md](./implementation-roadmap.md)** — Phased implementation plan.
