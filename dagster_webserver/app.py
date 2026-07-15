from __future__ import annotations

from typing import TYPE_CHECKING

from dagster import _check as check
from dagster._core.execution.compute_logs import warn_if_compute_logs_disabled
from dagster._core.telemetry import log_workspace_stats
from dagster._core.workspace.context import IWorkspaceProcessContext
from starlette.applications import Starlette

from dagster_webserver.webserver import DagsterWebserver

if TYPE_CHECKING:
    from dagster_webserver.auth.provider import BaseAuthProvider


def create_app_from_workspace_process_context(
    workspace_process_context: IWorkspaceProcessContext,
    path_prefix: str = "",
    live_data_poll_rate: int | None = None,
    auth_provider: BaseAuthProvider | None = None,
    **kwargs,
) -> Starlette:
    check.inst_param(
        workspace_process_context, "workspace_process_context", IWorkspaceProcessContext
    )
    check.str_param(path_prefix, "path_prefix")

    instance = workspace_process_context.instance

    if path_prefix:
        if not path_prefix.startswith("/"):
            raise Exception(
                f'The path prefix should begin with a leading "/": got {path_prefix}'
            )
        if path_prefix.endswith("/"):
            raise Exception(
                f'The path prefix should not include a trailing "/": got {path_prefix}'
            )

    warn_if_compute_logs_disabled()

    log_workspace_stats(instance, workspace_process_context)

    # Wrap process context with auth-aware context when auth is enabled
    if auth_provider is not None:
        from dagster._core.workspace.context import WorkspaceProcessContext

        from dagster_webserver.auth.context import (
            AuthenticatedWorkspaceProcessContext,
        )

        if isinstance(workspace_process_context, WorkspaceProcessContext):
            workspace_process_context = AuthenticatedWorkspaceProcessContext(
                inner=workspace_process_context,
                auth_provider=auth_provider,
            )

    asgi_app = DagsterWebserver(
        workspace_process_context,
        path_prefix,
        live_data_poll_rate,
        auth_provider=auth_provider,
    ).create_asgi_app(**kwargs)

    # Store auth provider on app state so route handlers can access it
    if auth_provider is not None:
        asgi_app.state.auth_provider = auth_provider
        _add_session_middleware(asgi_app, auth_provider)

    return asgi_app


def _add_session_middleware(app: Starlette, auth_provider: BaseAuthProvider) -> None:
    """Add Starlette session middleware for cookie-based sessions."""
    try:
        from starlette.middleware.sessions import SessionMiddleware
    except ImportError:
        return  # starlette[full] not installed — sessions won't work

    secret = getattr(auth_provider.config, "_session_secret", None)
    if not secret:
        import secrets

        secret = secrets.token_hex(32)

    app.add_middleware(
        SessionMiddleware,
        secret_key=secret,
        max_age=auth_provider.config.session_max_age,
        same_site="lax",
    )
