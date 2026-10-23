"""REST API endpoints for the Dagster agent system.

Provides routes under ``/api/agent/*`` that agents use for:
- Querying workspace entries (code locations to manage)
- Popping work requests from the queue
- Submitting heartbeats
- Uploading code location data and snapshots
- Enqueuing requests (for UI/daemon)

All endpoints require authentication (via API key or session).
Code location scoping is enforced per-request.
"""

from __future__ import annotations

import json
import logging

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

logger = logging.getLogger("dagster-webserver.api.agent")


def build_agent_routes() -> list[Route]:
    """Build the list of agent API routes."""
    return [
        Route(
            "/api/agent/workspace-entries",
            _get_workspace_entries,
            methods=["GET"],
        ),
        Route(
            "/api/agent/enqueue-request",
            _enqueue_request,
            methods=["POST"],
        ),
        Route(
            "/api/agent/pop-requests",
            _pop_requests,
            methods=["POST"],
        ),
        Route(
            "/api/agent/request-status/{request_id}",
            _get_request_status,
            methods=["GET"],
        ),
        Route(
            "/api/agent/complete-request/{request_id}",
            _complete_request,
            methods=["POST"],
        ),
        Route(
            "/api/agent/fail-request/{request_id}",
            _fail_request,
            methods=["POST"],
        ),
        Route(
            "/api/agent/heartbeat",
            _submit_heartbeat,
            methods=["POST"],
        ),
        Route(
            "/api/agent/upload-data",
            _upload_code_location_data,
            methods=["POST"],
        ),
        Route(
            "/api/agent/check-snapshot",
            _check_snapshot,
            methods=["GET"],
        ),
        Route(
            "/api/agent/upload-snapshot",
            _upload_snapshot,
            methods=["POST"],
        ),
        # Code location CRUD (admin)
        Route(
            "/api/agent/code-locations",
            _list_code_locations,
            methods=["GET"],
        ),
        Route(
            "/api/agent/code-locations",
            _create_code_location,
            methods=["POST"],
        ),
        Route(
            "/api/agent/code-locations/{location_name}",
            _update_code_location,
            methods=["PUT"],
        ),
        Route(
            "/api/agent/code-locations/{location_name}",
            _delete_code_location,
            methods=["DELETE"],
        ),
    ]


# ── Helpers ─────────────────────────────────────────────────────────────


async def _get_session(request: Request):
    """Get an async DB session from the app state."""
    from dagster_webserver.database import get_session_factory

    factory = get_session_factory()
    return factory()


def _get_agent_id(request: Request) -> str | None:
    """Extract the authenticated agent/user identity."""
    user = getattr(request.state, "user", None)
    if user:
        return user.username
    return None


def _check_location_access(request: Request, location_name: str | None) -> bool:
    """Check that the authenticated user has access to the given location.

    Service accounts (agents) are allowed — they authenticate via API key
    and their scope is determined by labels/allowed_locations in config.
    Regular users need at least LAUNCHER role for location-scoped operations.
    """
    # If no location specified, allow (global operations like heartbeat)
    if not location_name:
        return True

    user = getattr(request.state, "user", None)
    if not user:
        return False

    # Service accounts (agents) are allowed — their scope is controlled
    # by labels and allowed_locations in the agent config.
    if user.is_service_account:
        return True

    # Regular users need at least LAUNCHER-level access for location operations.
    # ADMIN and EDITOR have all permissions; LAUNCHER can launch/terminate.
    _LOCATION_ROLES = {"launcher", "editor", "admin"}
    if user.role in _LOCATION_ROLES:
        return True

    # Custom roles — check if they grant any relevant permission
    if user.custom_permissions:
        return any(user.custom_permissions.values())

    return False


# ── Agent Endpoints ─────────────────────────────────────────────────────


async def _get_workspace_entries(request: Request) -> JSONResponse:
    """Return code locations for this agent."""
    from dagster_webserver.database.agent_queries import get_workspace_entries

    agent_labels = request.query_params.get("labels")
    labels_dict = json.loads(agent_labels) if agent_labels else None

    location_names_param = request.query_params.getlist("location_name")
    location_names = location_names_param or None

    async with await _get_session(request) as session:
        locations = await get_workspace_entries(
            session,
            agent_labels=labels_dict,
            location_names=location_names,
        )

    return JSONResponse(
        {
            "locations": [
                {
                    "location_name": loc.location_name,
                    "deploy_metadata": loc.deploy_metadata,
                    "labels": loc.labels,
                    "status": loc.status,
                    "last_loaded_at": str(loc.last_loaded_at)
                    if loc.last_loaded_at
                    else None,
                    "load_error": loc.load_error,
                }
                for loc in locations
            ]
        }
    )


async def _enqueue_request(request: Request) -> JSONResponse:
    """Add a work request to the queue (for UI/daemon)."""
    from dagster_webserver.database.agent_queries import enqueue_request

    body = await request.json()
    request_api = body.get("request_api")
    request_body = body.get("request_body")

    if not request_api or not request_body:
        return JSONResponse(
            {"error": "request_api and request_body are required"},
            status_code=400,
        )

    location_name = body.get("location_name")
    labels = body.get("labels")
    ttl_seconds = body.get("ttl_seconds", 300)

    # Check location access
    if not _check_location_access(request, location_name):
        return JSONResponse({"error": "Access denied for location"}, status_code=403)

    async with await _get_session(request) as session:
        req = await enqueue_request(
            session,
            request_api=request_api,
            request_body=json.dumps(request_body)
            if isinstance(request_body, dict)
            else request_body,
            location_name=location_name,
            labels=labels,
            ttl_seconds=ttl_seconds,
        )

    return JSONResponse({"request_id": req.id}, status_code=201)


async def _pop_requests(request: Request) -> JSONResponse:
    """Agent pops pending work items from the queue."""
    from dagster_webserver.database.agent_queries import pop_requests

    agent_id = _get_agent_id(request)
    if not agent_id:
        return JSONResponse({"error": "Authentication required"}, status_code=401)

    body = await request.json() if await request.body() else {}
    limit = body.get("limit", 10)
    labels = body.get("labels")

    async with await _get_session(request) as session:
        requests = await pop_requests(
            session,
            agent_id=agent_id,
            agent_labels=labels,
            limit=limit,
        )

    return JSONResponse(
        {
            "requests": [
                {
                    "request_id": r.id,
                    "request_api": r.request_api,
                    "request_body": r.request_body,
                    "location_name": r.location_name,
                    "labels": r.labels,
                }
                for r in requests
            ]
        }
    )


async def _get_request_status(request: Request) -> JSONResponse:
    """Poll for request status (for request-reply pattern)."""
    from dagster_webserver.database.agent_queries import get_request_status

    request_id = request.path_params["request_id"]
    agent_id = _get_agent_id(request)
    if not agent_id:
        return JSONResponse({"error": "Authentication required"}, status_code=401)

    async with await _get_session(request) as session:
        req = await get_request_status(session, request_id)

    if not req:
        return JSONResponse({"error": "Request not found"}, status_code=404)

    # Only the assigned agent or an admin can view the response
    if req.assigned_to_agent and req.assigned_to_agent != agent_id:
        user = getattr(request.state, "user", None)
        is_admin = user and user.role in ("admin", "editor")
        if not is_admin:
            return JSONResponse(
                {"error": "Request assigned to a different agent"},
                status_code=403,
            )

    return JSONResponse(
        {
            "request_id": req.id,
            "status": req.status,
            "response_body": req.response_body,
            "created_at": str(req.created_at) if req.created_at else None,
            "popped_at": str(req.popped_at) if req.popped_at else None,
        }
    )


async def _complete_request(request: Request) -> JSONResponse:
    """Agent marks a request as completed with a response body."""
    from dagster_webserver.database.agent_queries import complete_request

    request_id = request.path_params["request_id"]
    agent_id = _get_agent_id(request)
    if not agent_id:
        return JSONResponse({"error": "Authentication required"}, status_code=401)

    body = await request.json()
    response_body = body.get("response_body")
    # Serialize to string if it's a dict
    if isinstance(response_body, (dict, list)):
        response_body = json.dumps(response_body)

    async with await _get_session(request) as session:
        success = await complete_request(
            session,
            request_id=request_id,
            agent_id=agent_id,
            response_body=response_body,
        )

    if not success:
        return JSONResponse(
            {"error": "Request not found or not assigned to this agent"},
            status_code=404,
        )

    return JSONResponse({"status": "completed", "request_id": request_id})


async def _fail_request(request: Request) -> JSONResponse:
    """Agent marks a request as failed with an error message."""
    from dagster_webserver.database.agent_queries import fail_request

    request_id = request.path_params["request_id"]
    agent_id = _get_agent_id(request)
    if not agent_id:
        return JSONResponse({"error": "Authentication required"}, status_code=401)

    body = await request.json()
    error_message = body.get("error", "Unknown error")

    async with await _get_session(request) as session:
        success = await fail_request(
            session,
            request_id=request_id,
            agent_id=agent_id,
            error=error_message,
        )

    if not success:
        return JSONResponse(
            {"error": "Request not found or not assigned to this agent"},
            status_code=404,
        )

    return JSONResponse({"status": "failed", "request_id": request_id})


async def _submit_heartbeat(request: Request) -> JSONResponse:
    """Agent reports health."""
    from dagster_webserver.database.agent_queries import submit_heartbeat

    agent_id = _get_agent_id(request)
    if not agent_id:
        return JSONResponse({"error": "Authentication required"}, status_code=401)

    body = await request.json()

    async with await _get_session(request) as session:
        await submit_heartbeat(
            session,
            agent_id=agent_id,
            agent_labels=body.get("labels"),
            agent_type=body.get("agent_type"),
            metadata=body.get("metadata"),
            errors=body.get("errors"),
            code_server_heartbeats=body.get("code_server_heartbeats"),
        )

    return JSONResponse({"status": "ok"})


async def _upload_code_location_data(request: Request) -> JSONResponse:
    """Agent uploads loaded manifests for a code location."""
    from datetime import datetime, timezone

    agent_id = _get_agent_id(request)
    if not agent_id:
        return JSONResponse({"error": "Authentication required"}, status_code=401)

    body = await request.json()
    location_name = body.get("location_name")
    manifest = body.get("manifest")
    error = body.get("error")

    if not location_name:
        return JSONResponse({"error": "location_name is required"}, status_code=400)

    if not _check_location_access(request, location_name):
        return JSONResponse({"error": "Access denied"}, status_code=403)

    async with await _get_session(request) as session:
        from sqlalchemy import select

        from dagster_webserver.database.models import CodeLocationDefinition

        result = await session.execute(
            select(CodeLocationDefinition).where(
                CodeLocationDefinition.location_name == location_name
            )
        )
        loc = result.scalar_one_or_none()

        if loc:
            if error:
                loc.load_error = error
            else:
                loc.loaded_manifest = manifest
                loc.last_loaded_at = datetime.now(timezone.utc)
                loc.load_error = None
            await session.commit()

    return JSONResponse({"status": "ok"})


async def _check_snapshot(request: Request) -> JSONResponse:
    """Check if a snapshot already exists (by content hash)."""
    from dagster_webserver.database.agent_queries import check_snapshot_exists

    location_name = request.query_params.get("location_name")
    snapshot_type = request.query_params.get("snapshot_type")
    sha1_hash = request.query_params.get("sha1_hash")

    if not all([location_name, snapshot_type, sha1_hash]):
        return JSONResponse(
            {"error": "location_name, snapshot_type, and sha1_hash are required"},
            status_code=400,
        )

    async with await _get_session(request) as session:
        existing = await check_snapshot_exists(
            session, location_name, snapshot_type, sha1_hash
        )

    if existing:
        return JSONResponse({"exists": True, "size_bytes": existing.size_bytes})
    return JSONResponse({"exists": False})


async def _upload_snapshot(request: Request) -> JSONResponse:
    """Upload a serialized snapshot."""
    from dagster_webserver.database.agent_queries import store_snapshot

    agent_id = _get_agent_id(request)
    if not agent_id:
        return JSONResponse({"error": "Authentication required"}, status_code=401)

    body = await request.json()
    location_name = body.get("location_name")
    snapshot_type = body.get("snapshot_type")
    sha1_hash = body.get("sha1_hash")
    size_bytes = body.get("size_bytes", 0)
    data_b64 = body.get("data")  # Base64-encoded

    if not all([location_name, snapshot_type, sha1_hash]):
        return JSONResponse(
            {"error": "location_name, snapshot_type, and sha1_hash are required"},
            status_code=400,
        )

    import base64

    data = base64.b64decode(data_b64) if data_b64 else None

    async with await _get_session(request) as session:
        snapshot = await store_snapshot(
            session,
            location_name=location_name,
            snapshot_type=snapshot_type,
            sha1_hash=sha1_hash,
            size_bytes=size_bytes,
            data=data,
        )

    return JSONResponse({"id": snapshot.id}, status_code=201)


# ── Code Location CRUD (Admin) ──────────────────────────────────────────


async def _list_code_locations(request: Request) -> JSONResponse:
    """List all code locations with status."""
    from dagster_webserver.database.agent_queries import list_code_locations

    status = request.query_params.get("status")

    async with await _get_session(request) as session:
        locations = await list_code_locations(session, status=status)

    return JSONResponse(
        {
            "locations": [
                {
                    "location_name": loc.location_name,
                    "deploy_metadata": loc.deploy_metadata,
                    "labels": loc.labels,
                    "status": loc.status,
                    "last_loaded_at": str(loc.last_loaded_at)
                    if loc.last_loaded_at
                    else None,
                    "load_error": loc.load_error,
                    "created_at": str(loc.created_at) if loc.created_at else None,
                }
                for loc in locations
            ]
        }
    )


async def _create_code_location(request: Request) -> JSONResponse:
    """Create a new code location definition."""
    from dagster_webserver.database.agent_queries import upsert_code_location

    body = await request.json()
    location_name = body.get("location_name")
    deploy_metadata = body.get("deploy_metadata", {})
    labels = body.get("labels")

    if not location_name:
        return JSONResponse({"error": "location_name is required"}, status_code=400)

    async with await _get_session(request) as session:
        loc = await upsert_code_location(
            session,
            location_name=location_name,
            deploy_metadata=deploy_metadata,
            labels=labels,
        )

    return JSONResponse(
        {
            "location_name": loc.location_name,
            "status": loc.status,
            "created_at": str(loc.created_at) if loc.created_at else None,
        },
        status_code=201,
    )


async def _update_code_location(request: Request) -> JSONResponse:
    """Update a code location's deploy metadata or labels."""

    location_name = request.path_params["location_name"]
    body = await request.json()

    async with await _get_session(request) as session:
        from sqlalchemy import select

        from dagster_webserver.database.models import CodeLocationDefinition

        result = await session.execute(
            select(CodeLocationDefinition).where(
                CodeLocationDefinition.location_name == location_name
            )
        )
        loc = result.scalar_one_or_none()

        if not loc:
            return JSONResponse({"error": "Code location not found"}, status_code=404)

        # Update fields
        if "deploy_metadata" in body:
            loc.deploy_metadata = body["deploy_metadata"]
        if "labels" in body:
            loc.labels = body["labels"]
        if "status" in body:
            from dagster_webserver.database.models import CODE_LOCATION_STATUSES

            if body["status"] not in CODE_LOCATION_STATUSES:
                return JSONResponse(
                    {
                        "error": f"Invalid status. Must be one of: {', '.join(CODE_LOCATION_STATUSES)}"
                    },
                    status_code=400,
                )
            loc.status = body["status"]

        await session.commit()
        await session.refresh(loc)

    return JSONResponse(
        {
            "location_name": loc.location_name,
            "deploy_metadata": loc.deploy_metadata,
            "labels": loc.labels,
            "status": loc.status,
        }
    )


async def _delete_code_location(request: Request) -> JSONResponse:
    """Remove (soft-delete) a code location."""
    from dagster_webserver.database.agent_queries import delete_code_location

    location_name = request.path_params["location_name"]

    async with await _get_session(request) as session:
        deleted = await delete_code_location(session, location_name)

    if not deleted:
        return JSONResponse({"error": "Code location not found"}, status_code=404)

    return JSONResponse({"status": "deleted"})
