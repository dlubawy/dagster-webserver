"""Authenticated workspace context classes.

Provides request and process context subclasses that inject user identity
and resolve per-user permissions instead of the global ``read_only`` flag.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from dagster._core.workspace.context import (
    IWorkspaceProcessContext,
    WorkspaceProcessContext,
    WorkspaceRequestContext,
)
from dagster._core.workspace.permissions import PermissionResult

from dagster_webserver.auth.provider import BaseAuthProvider
from dagster_webserver.auth.users import AuthUser

if TYPE_CHECKING:
    pass


class AuthenticatedWorkspaceRequestContext(WorkspaceRequestContext):
    """WorkspaceRequestContext that resolves permissions per-user.

    When a user is present, permissions are derived from the user's role
    via the auth provider.  When no user is present, falls back to the
    standard ``read_only`` behavior.
    """

    def __init__(
        self,
        *,
        user: AuthUser | None = None,
        auth_provider: BaseAuthProvider | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._user = user
        self._auth_provider = auth_provider

    @property
    def permissions(self) -> Mapping[str, PermissionResult]:
        if self._user and self._auth_provider:
            return self._auth_provider.get_user_permissions(self._user)
        # Fall back to standard read_only behavior
        return super().permissions

    def permissions_for_location(
        self, *, location_name: str
    ) -> Mapping[str, PermissionResult]:
        if self._user and self._auth_provider:
            # For now, location-scoped permissions use the same role-based map
            # filtered to location-scoped permissions.  Future work: per-location
            # role assignments.
            from dagster._core.workspace.permissions import LOCATION_SCOPED_PERMISSIONS

            all_perms = self._auth_provider.get_user_permissions(self._user)
            return {
                perm: result
                for perm, result in all_perms.items()
                if perm in LOCATION_SCOPED_PERMISSIONS
            }
        return super().permissions_for_location(location_name=location_name)

    @property
    def user(self) -> AuthUser | None:
        """The authenticated user, or ``None``."""
        return self._user

    def get_viewer_tags(self) -> dict[str, str]:
        tags = super().get_viewer_tags()
        if self._user:
            tags["dagster.io/user"] = self._user.username
            tags["dagster.io/role"] = self._user.role
        return tags

    def get_reporting_user_tags(self) -> dict[str, str]:
        tags = super().get_reporting_user_tags()
        if self._user:
            tags["dagster.io/user"] = self._user.username
            tags["dagster.io/role"] = self._user.role
        return tags


class AuthenticatedWorkspaceProcessContext(
    IWorkspaceProcessContext[AuthenticatedWorkspaceRequestContext]
):
    """Process context that creates authenticated request contexts.

    Wraps a standard ``WorkspaceProcessContext`` and intercepts
    ``create_request_context`` to inject the auth provider and resolve
    the user from ``request.state.user``.
    """

    def __init__(
        self,
        inner: WorkspaceProcessContext,
        auth_provider: BaseAuthProvider | None = None,
    ) -> None:
        self._inner = inner
        self._auth_provider = auth_provider

    @property
    def instance(self):
        return self._inner.instance

    @property
    def version(self) -> str:
        return self._inner.version

    def create_request_context(
        self, source: Any | None = None
    ) -> AuthenticatedWorkspaceRequestContext:
        # Extract user from request state if available
        user: AuthUser | None = None
        if source is not None:
            user = getattr(getattr(source, "state", None), "user", None)

        # Create the standard context kwargs
        workspace = self._inner.get_current_workspace()
        return AuthenticatedWorkspaceRequestContext(
            instance=self._inner.instance,
            current_workspace=workspace,
            process_context=self,
            version=self._inner.version,
            source=source,
            read_only=self._inner.read_only,
            user=user,
            auth_provider=self._auth_provider,
        )

    def reload_code_location(self, name: str) -> None:
        self._inner.reload_code_location(name)

    def shutdown_code_location(self, name: str) -> None:
        self._inner.shutdown_code_location(name)

    def reload_workspace(self) -> None:
        self._inner.reload_workspace()

    def refresh_workspace(self) -> None:
        self._inner.refresh_workspace()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> None:
        pass
