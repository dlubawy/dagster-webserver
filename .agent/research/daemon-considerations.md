# Daemon Considerations — Do We Need a Custom Version?

## Question

Does our multi-agent architecture require modifications to `dagster-daemon`, or can the standard OSS daemon work as-is?

______________________________________________________________________

## 1. Standard OSS Daemon Architecture

The `dagster-daemon run` process runs multiple daemon threads:

| Daemon Thread | Purpose |
| ---------------------------- | ----------------------------------------------------- |
| `SchedulerDaemon` | Evaluates schedules, requests runs for matching times |
| `SensorDaemon` | Evaluates sensors, requests runs for new conditions |
| `AssetDaemon` | Auto-materialization logic |
| `BackfillDaemon` | Executes backfill operations |
| `QueuedRunCoordinatorDaemon` | Dequeues and launches queued runs |
| `MonitoringDaemon` | Monitors run health, auto-terminates stuck runs |
| `EventLogConsumerDaemon` | Consumes event logs for downstream processing |
| `FreshnessDaemon` | Evaluates freshness policies |

### How Daemons Interact with Code Locations

In the standard OSS architecture:

1. The daemon loads the workspace (same as webserver) via `WorkspaceProcessContext`.
1. It accesses code locations through `GrpcServerRegistry` which spawns local gRPC processes.
1. When a schedule/sensor fires, the daemon evaluates it by making gRPC calls to the code location server.
1. If a run should be launched, the daemon calls `instance.run_launcher.launch_run()`.

```
┌──────────────────────────┐
│     dagster-daemon       │
│  ┌─────────────────────┐ │
│  │ WorkspaceProcessCtx │ │
│  │  ┌───────────────┐  │ │
│  │  │ GrpcServerReg │  │ │
│  │  │ (local procs) │  │ │
│  │  └───────────────┘  │ │
│  └─────────────────────┘ │
│                          │
│  SchedulerDaemon ──→ gRPC call to code server
│  SensorDaemon    ──→ gRPC call to code server
│  BackfillDaemon  ──→ launch run via instance.run_launcher
└──────────────────────────┘
```

______________________________________________________________________

## 2. The Problem with Multi-Agent Architecture

In our multi-agent design, code location servers are **not** co-located with the daemon. They are managed by separate `dagster-agent` processes, potentially on different machines.

This means the daemon **cannot** directly gRPC-call the code servers because:

1. It doesn't know where they are (the agent manages their lifecycle).
1. They may be in different network namespaces (k8s pods, containers).
1. The daemon's `GrpcServerRegistry` only manages local processes.

### Two Architectural Approaches

#### Approach A: Daemon Routes Through Agent Queue (Recommended)

The daemon submits work requests to the agent queue instead of directly calling code servers.

```
┌──────────────────────┐         ┌──────────────────────────┐
│    dagster-daemon     │         │   dagster-webserver       │
│                       │         │                          │
│  SchedulerDaemon      │───→────→│  agent_requests table    │
│  SensorDaemon         │         │  (work queue)            │
│  AssetDaemon          │────────→│                          │
└──────────────────────┘         └──────────┬───────────────┘
                                            │
                                            ▼
                                   ┌──────────────────┐
                                   │  dagster-agent   │
                                   │                  │
                                   │ pops requests    │
                                   │ calls gRPC server│
                                   │ uploads response │
                                   └──────────────────┘
```

**Changes needed to daemon:** YES — the daemon's evaluation logic needs to enqueue requests instead of making direct gRPC calls.

#### Approach B: Daemon Has Direct Access to Code Servers

The daemon maintains its own `GrpcServerRegistry` alongside the agents. This means code servers are accessible from both the daemon and the agents.

```
┌──────────────────────┐         ┌──────────────────┐
│    dagster-daemon     │         │  dagster-agent   │
│                       │         │                  │
│  GrpcServerRegistry ──┼──→ gRPC →│ (manages same   │
│  (local procs)        │          │  code servers)  │
└──────────────────────┘          └──────────────────┘
```

**Changes needed to daemon:** NO — but this defeats the purpose of multi-agent architecture.

______________________________________________________________________

## 3. Detailed Analysis: What Daemon Threads Need Changes

### 3.1 `SchedulerDaemon` — NEEDS MODIFICATION

**Current behavior:** Evaluates schedules by calling `client.external_schedule_execution()` on the code server's gRPC endpoint.

**Required change:** Instead of direct gRPC calls, enqueue a `GET_EXTERNAL_SCHEDULE_EXECUTION_DATA` request to the agent queue. The agent evaluates it and returns the result.

```python
# Current (direct gRPC):
result = grpc_client.external_schedule_execution(args)

# New (via agent queue):
request_id = await enqueue_agent_request(
    request_api="GET_EXTERNAL_SCHEDULE_EXECUTION_DATA",
    request_body=serialize_value(args),
    location_name=schedule.location_name,
)
result = await wait_for_agent_response(request_id)
```

### 3.2 `SensorDaemon` — NEEDS MODIFICATION

Same pattern as scheduler: evaluates sensors via gRPC → should enqueue requests instead.

### 3.3 `AssetDaemon` — NEEDS MODIFICATION

Auto-materialization logic queries code servers for asset graph data and materialization conditions. Should route through agent queue.

### 3.4 `BackfillDaemon` — PARTIAL MODIFICATION

Backfills launch runs. The run launching can go through the agent queue (`LAUNCH_RUN` request). However, backfill progress tracking and partition management may still need direct database access.

### 3.5 `QueuedRunCoordinatorDaemon` — NEEDS MODIFICATION

When dequeuing a queued run, instead of calling `instance.run_launcher.launch_run()` directly, enqueue a `LAUNCH_RUN` request to the appropriate agent.

### 3.6 `MonitoringDaemon` — MINIMAL CHANGES

Monitors run health by querying the database. May need to route termination requests (`TERMINATE_RUN`) through the agent queue instead of direct gRPC calls.

### 3.7 `EventLogConsumerDaemon` — NO CHANGES

Reads event logs from storage and processes them. Doesn't interact with code servers.

### 3.8 `FreshnessDaemon` — NEEDS MODIFICATION

Similar to scheduler/sensor — evaluates freshness by calling code servers. Should enqueue requests.

______________________________________________________________________

## 4. Request/Response Pattern for Daemon → Agent Communication

The daemon needs a way to:

1. Submit a request and wait for the response (synchronous from daemon's perspective).
1. Handle timeouts and retries.

### Design: Request-Reply via Agent Queue

```python
class AgentRequestClient:
    """Client for making synchronous requests through the agent queue."""

    def __init__(self, webserver_url: str, agent_token: str):
        self.webserver_url = webserver_url
        self.session = create_session(webserver_url, token=agent_token)

    async def request(self, request_api: str, request_body: dict, location_name: str, timeout: int = 120):
        """Submit a request and wait for the response."""
        # Enqueue the request
        request_id = await self._enqueue(request_api, request_body, location_name)

        # Poll for response (with timeout)
        deadline = time.time() + timeout
        while time.time() < deadline:
            response = await self._check_response(request_id)
            if response is not None:
                return response
            await asyncio.sleep(1)

        raise TimeoutError(f"Agent request {request_id} timed out after {timeout}s")

    async def _enqueue(self, request_api, request_body, location_name):
        result = await graphql_mutation(
            self.session,
            """
            mutation EnqueueAgentRequest($input: EnqueueInput!) {
                enqueueAgentRequest(input: $input) { requestId }
            }
            """,
            {"input": {
                "requestApi": request_api,
                "requestBody": serialize_value(request_body),
                "locationName": location_name,
            }}
        )
        return result["enqueueAgentRequest"]["requestId"]

    async def _check_response(self, request_id):
        result = await graphql_query(
            self.session,
            """
            query AgentRequest($requestId: ID!) {
                agentRequest(requestId: $requestId) {
                    status
                    responseBody
                }
            }
            """,
            {"requestId": request_id}
        )
        req = result["agentRequest"]
        if req["status"] == "completed":
            return deserialize_value(req["responseBody"])
        elif req["status"] == "failed":
            raise Exception(req["responseBody"])
        return None
```

### Alternative: WebSocket for Real-Time Responses

Instead of polling, use a WebSocket connection for real-time request/response:

```python
class AgentWebSocketClient:
    async def request(self, request_api, request_body, location_name):
        # Send request over WebSocket
        await self.ws.send_json({
            "type": "request",
            "requestId": str(uuid.uuid4()),
            "requestApi": request_api,
            "requestBody": serialize_value(request_body),
            "locationName": location_name,
        })

        # Wait for matching response
        while True:
            msg = await self.ws.receive_json()
            if msg["type"] == "response" and msg["requestId"] == request_id:
                return deserialize_value(msg["responseBody"])
```

**Recommendation**: Start with polling (simpler, works over standard GraphQL). Add WebSocket support later for lower latency.

______________________________________________________________________

## 5. Decision: Custom Daemon or Modified OSS Daemon?

### Option A: Fork and Modify the Daemon (Recommended)

Create a `dagster-agent-daemon` that extends the OSS daemon with agent-aware evaluation logic.

**Pros:**

- Minimal changes — mostly replacing gRPC calls with queue-based requests.
- Reuses all existing daemon infrastructure (heartbeat management, thread coordination).
- Can be packaged as a separate library (`dagster-agent-daemon`).

**Cons:**

- Maintenance burden of keeping fork in sync with upstream.
- Some daemon internals are tightly coupled to direct gRPC access.

### Option B: Keep Standard Daemon, Add Agent Proxy Layer

Create a sidecar proxy that intercepts the daemon's gRPC calls and routes them through agents.

**Pros:**

- No daemon modifications needed.
- Proxy can be deployed independently.

**Cons:**

- Complex — need to proxy gRPC protocol.
- Adds latency and failure points.
- Doesn't work well with k8s where code servers are in different pods.

### Option C: Hybrid — Daemon Directly Connects for Evaluation, Agent Handles Runs

The daemon evaluates schedules/sensors directly (needs its own gRPC server access), but run launching goes through the agent queue.

**Pros:**

- Minimal daemon changes — only modify run launching path.
- Evaluation is fast (no round-trip through queue).

**Cons:**

- Daemon still needs network access to code servers.
- Doesn't fully decouple daemon from agents.
- Code server endpoints need to be discoverable by the daemon.

### Recommendation: Option A with Option C Elements

Start with **Option C** (hybrid) for Phase 1:

- Daemon evaluates schedules/sensors directly (standard behavior).
- Run launching goes through agent queue.
- This is the minimal change and works immediately.

Evolve to **full Option A** in Phase 2:

- All code server interactions go through agent queue.
- Full decoupling of daemon from code location servers.

______________________________________________________________________

## 6. Implementation Plan for Daemon Changes

### Phase 1 (Hybrid — Minimal Changes)

```python
# In dagster_agent/daemon/run_coordinator.py

class AgentQueuedRunCoordinatorDaemon(QueuedRunCoordinatorDaemon):
    """Dequeue runs and enqueue them to the agent queue instead of launching directly."""

    async def dequeue_runs(self, instance, ...):
        runs = await super()._get_runs_to_dequeue(instance)

        for run in runs:
            location_name = self._get_location_name(run)

            # Enqueue LAUNCH_RUN request to agent queue
            await self._enqueue_agent_request(
                request_api="LAUNCH_RUN",
                request_body=serialize_value(LaunchRunArgs(dagster_run=run)),
                location_name=location_name,
            )

            # Update run status (don't launch directly)
            instance.add_run_tags(run.run_id, {"dagster/launched_by": "agent-queue"})
```

### Phase 2 (Full Agent Routing)

```python
# In dagster_agent/daemon/scheduler.py

class AgentSchedulerDaemon(SchedulerDaemon):
    """Evaluate schedules through the agent queue."""

    async def evaluate_schedule(self, schedule, workspace_context):
        # Instead of direct gRPC call:
        # result = client.external_schedule_execution(args)

        # Enqueue request and wait for response:
        result = await self._agent_client.request(
            request_api="GET_EXTERNAL_SCHEDULE_EXECUTION_DATA",
            request_body=serialize_value(ScheduleExecutionArgs(...)),
            location_name=schedule.location_name,
        )

        if result.should_execute:
            # Enqueue run launch
            await self._enqueue_run(result.run)
```

______________________________________________________________________

## 7. Summary

| Question | Answer |
| ----------------------------------- | --------------------------------------------------------------------------------------------- |
| Do we need a custom daemon? | **Yes**, but modifications can be incremental. |
| Can the standard daemon work as-is? | Only if code servers are locally accessible (defeats multi-agent purpose). |
| What's the minimal change? | Route run launching through agent queue; keep evaluation direct. |
| What's the full change? | All code server interactions go through agent queue. |
| Recommended approach? | Start with hybrid (Phase 1), evolve to full routing (Phase 2). |
| Should we fork dagster-daemon? | Package changes as a separate `dagster-agent-daemon` library that extends OSS daemon classes. |
