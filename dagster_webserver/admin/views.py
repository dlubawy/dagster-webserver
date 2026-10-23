"""Admin portal view classes.

Provides:
- ``BaseAdminView`` — abstract base class for admin CRUD views
- ``UserView`` — user management (CRUD)
- ``RoleView`` — role management (CRUD)
- ``@action`` / ``@row_action`` decorators for batch and per-row operations
"""

from __future__ import annotations

import inspect
import json
import re
import secrets
import string
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from dagster._core.workspace.permissions import PermissionResult
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response
from starlette.status import HTTP_302_FOUND

from dagster_webserver.admin.permissions import (
    can_edit_code_locations,
    can_edit_oidc,
    can_edit_roles,
    can_edit_users,
    can_view_code_locations,
    can_view_oidc,
    can_view_roles,
    can_view_users,
    has_any_admin_permission,
)

if TYPE_CHECKING:
    from dagster_webserver.auth.db_backend import DatabaseUserBackend
    from dagster_webserver.auth.users import AuthUser


# ---------------------------------------------------------------------------
# Action decorators (mirroring starlette-admin pattern)
# ---------------------------------------------------------------------------


def action(
    name: str,
    text: str,
    confirmation: str | None = None,
    submit_btn_class: str = "btn-primary",
    submit_btn_text: str = "Yes, Proceed",
    icon_class: str | None = None,
) -> Callable:
    """Decorator to register a batch action on a ``BaseAdminView``."""

    def wrap(f: Callable[..., Awaitable[str]]) -> Callable[..., Awaitable[str]]:
        f._action = {  # type: ignore[attr-defined]
            "name": name,
            "text": text,
            "confirmation": confirmation,
            "submit_btn_text": submit_btn_text,
            "submit_btn_class": submit_btn_class,
            "icon_class": icon_class,
        }
        return f

    return wrap


def row_action(
    name: str,
    text: str,
    confirmation: str | None = None,
    icon_class: str | None = None,
    exclude_from_list: bool = False,
    exclude_from_detail: bool = False,
) -> Callable:
    """Decorator to register a per-row action on a ``BaseAdminView``."""

    def wrap(f: Callable[..., Awaitable[str]]) -> Callable[..., Awaitable[str]]:
        f._row_action = {  # type: ignore[attr-defined]
            "name": name,
            "text": text,
            "confirmation": confirmation,
            "icon_class": icon_class,
            "exclude_from_list": exclude_from_list,
            "exclude_from_detail": exclude_from_detail,
        }
        return f

    return wrap


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_admin_perms(request: Request) -> dict[str, PermissionResult]:
    """Read resolved admin permissions from request state."""
    return getattr(request.state, "admin_permissions", {})


def _resolve_authuser_permissions(user: AuthUser) -> dict[str, PermissionResult]:
    """Build a PermissionResult map from an AuthUser for admin-permission checks."""
    from dagster_webserver.auth.roles import (
        Role,
        get_custom_permissions,
        get_role_permissions,
    )

    if user.role == "custom" and user.custom_permissions:
        return get_custom_permissions(user.custom_permissions)
    try:
        role = Role(user.role)
        return get_role_permissions(role)
    except ValueError:
        return get_role_permissions(Role.VIEWER)


# ---------------------------------------------------------------------------
# Base view
# ---------------------------------------------------------------------------


class BaseAdminView(ABC):
    """Abstract base class for admin portal CRUD views."""

    identity: str = ""
    label: str = ""
    plural_label: str = ""
    icon: str = ""

    list_columns: list[str] = []
    detail_fields: list[str] = []
    create_fields: list[str] = []
    edit_fields: list[str] = []

    def __init__(self, backend: DatabaseUserBackend) -> None:
        self._backend = backend
        self._actions: dict[str, dict[str, str]] = {}
        self._row_actions: dict[str, dict[str, str]] = {}
        self._init_actions()

    # -- Permission hooks (override in subclasses) --

    def is_accessible(self, request: Request) -> bool:
        return True

    def can_create(self, request: Request) -> bool:
        return True

    def can_edit(self, request: Request) -> bool:
        return True

    def can_delete(self, request: Request) -> bool:
        return True

    # -- Abstract CRUD --

    @abstractmethod
    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
    ) -> list[Any]: ...

    @abstractmethod
    async def count(
        self,
        request: Request,
        where: dict[str, Any] | None = None,
    ) -> int: ...

    @abstractmethod
    async def find_by_pk(self, request: Request, pk: str) -> Any: ...

    @abstractmethod
    async def create(self, request: Request, data: dict[str, Any]) -> Any: ...

    @abstractmethod
    async def edit(self, request: Request, pk: str, data: dict[str, Any]) -> Any: ...

    @abstractmethod
    async def delete(self, request: Request, pks: list[str]) -> int: ...

    # -- Serialization --

    async def serialize(
        self, obj: Any, request: Request, action: str = "list"
    ) -> dict[str, Any]:
        """Convert an object to a display-friendly dict."""
        raise NotImplementedError

    async def get_pk(self, obj: Any) -> str:
        raise NotImplementedError

    # -- Action registration --

    def _init_actions(self) -> None:
        for _name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if hasattr(method, "_action"):
                self._actions[method._action["name"]] = method._action
            if hasattr(method, "_row_action"):
                self._row_actions[method._row_action["name"]] = method._row_action

    async def is_action_allowed(self, request: Request, name: str) -> bool:
        if name == "delete":
            return self.can_delete(request)
        return True

    async def is_row_action_allowed(self, request: Request, name: str) -> bool:
        if name == "view":
            return self.is_accessible(request)
        if name == "edit":
            return self.can_edit(request)
        if name == "delete":
            return self.can_delete(request)
        if name == "reset_password":
            return self.can_edit(request)
        return True

    async def get_all_actions(self, request: Request) -> list[dict[str, Any]]:
        actions = []
        for name, meta in self._actions.items():
            if await self.is_action_allowed(request, name):
                actions.append(meta)
        return actions

    async def get_all_row_actions(self, request: Request) -> list[dict[str, Any]]:
        actions = []
        for name, meta in self._row_actions.items():
            if await self.is_row_action_allowed(request, name):
                actions.append(meta)
        return actions

    async def handle_action(
        self, request: Request, pks: list[str], name: str
    ) -> str | Response:
        handler = getattr(self, f"{name}_action", None)
        if handler is None:
            return "Invalid action"
        if not await self.is_action_allowed(request, name):
            return "Forbidden"
        result = await handler(request, pks)
        return result

    async def handle_row_action(
        self, request: Request, pk: str, name: str
    ) -> str | Response:
        # Look for a dedicated row action first, then fall back to batch action
        handler = getattr(self, f"{name}_row_action", None)
        if handler is None:
            handler = getattr(self, f"{name}_action", None)
        if handler is None:
            return "Invalid row action"
        if not await self.is_row_action_allowed(request, name):
            return "Forbidden"
        result = await handler(request, pk)
        return result


# ---------------------------------------------------------------------------
# UserView
# ---------------------------------------------------------------------------


class UserView(BaseAdminView):
    identity = "users"
    label = "User"
    plural_label = "Users"
    icon = "users"

    list_columns = [
        "username",
        "display_name",
        "email",
        "is_active",
        "created_at",
    ]
    detail_fields = [
        "username",
        "display_name",
        "email",
        "is_active",
        "created_at",
        "updated_at",
    ]
    create_fields = ["username", "password", "email", "display_name"]
    edit_fields = [
        "username",
        "password",
        "email",
        "display_name",
        "is_active",
    ]

    def is_accessible(self, request: Request) -> bool:
        return can_view_users(_get_admin_perms(request))

    def can_create(self, request: Request) -> bool:
        return can_edit_users(_get_admin_perms(request))

    def can_edit(self, request: Request) -> bool:
        return can_edit_users(_get_admin_perms(request))

    def can_delete(self, request: Request) -> bool:
        return can_edit_users(_get_admin_perms(request))

    # -- CRUD --

    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
    ) -> list[AuthUser]:
        users = await self._backend.list_users()
        # Exclude service accounts — they have their own dedicated view
        users = [u for u in users if not getattr(u, "is_service_account", False)]
        return users[skip : skip + limit]

    async def count(self, request: Request, where: dict[str, Any] | None = None) -> int:
        users = await self._backend.list_users()
        users = [u for u in users if not getattr(u, "is_service_account", False)]
        return len(users)

    async def find_by_pk(self, request: Request, pk: str) -> AuthUser | None:
        return await self._backend.get_user(pk)

    @staticmethod
    def _parse_role_assignments(data: dict[str, Any]) -> list[dict[str, str | None]]:
        """Parse role_assignments JSON from form data."""
        raw = data.get("role_assignments", "[]")
        if isinstance(raw, str):
            try:
                assignments = json.loads(raw)
            except json.JSONDecodeError:
                return []
        elif isinstance(raw, list):
            assignments = raw
        else:
            return []
        # Validate structure
        result = []
        for a in assignments:
            if not isinstance(a, dict) or "role_name" not in a:
                continue
            result.append(
                {
                    "role_name": a["role_name"],
                    "code_location_name": a.get("code_location_name") or None,
                }
            )
        return result

    async def create(self, request: Request, data: dict[str, Any]) -> AuthUser:
        username = data["username"].strip()
        existing = await self._backend.get_user(username)
        if existing is not None:
            raise ValueError(f"User '{username}' already exists")

        # Determine global role from assignments (default to viewer)
        role_assignments = self._parse_role_assignments(data)
        global_role = "viewer"
        for a in role_assignments:
            if a["code_location_name"] is None:
                global_role = a["role_name"]
                break

        user = await self._backend.create_user(
            username=username,
            password=data["password"],
            role=global_role,
            email=data.get("email"),
            display_name=data.get("display_name"),
        )

        # Set all role assignments (including location-scoped ones)
        if len(role_assignments) > 1 or any(
            a["code_location_name"] is not None for a in role_assignments
        ):
            await self._backend.set_user_role_assignments(username, role_assignments)

        return user

    async def edit(self, request: Request, pk: str, data: dict[str, Any]) -> AuthUser:
        current_user = getattr(request.state, "user", None)

        # Self-protection: cannot self-demote
        role_assignments = self._parse_role_assignments(data)
        if current_user and pk == current_user.username:
            global_assignment = next(
                (a for a in role_assignments if a["code_location_name"] is None),
                None,
            )
            if global_assignment:
                new_role_obj = await self._backend.get_role(
                    global_assignment["role_name"]
                )
                if new_role_obj:
                    new_perms = _resolve_authuser_permissions_for_role(new_role_obj)
                    if not has_any_admin_permission(new_perms):
                        raise ValueError(
                            "Cannot change your role to one with no admin permissions"
                        )

        # Coerce is_active from checkbox string to bool
        is_active_raw = data.get("is_active")
        is_active = None
        if "is_active" in data:
            is_active = is_active_raw in ("true", "True", "on", True, "1")

        # Update user fields (no role param — handled via assignments)
        user = await self._backend.update_user(
            pk,
            password=data.get("password") or None,
            email=data.get("email") or None,
            display_name=data.get("display_name") or None,
            is_active=is_active,
        )

        # Update role assignments
        if role_assignments:
            await self._backend.set_user_role_assignments(pk, role_assignments)
            # Re-fetch to get updated role
            user = await self._backend.get_user(pk)

        return user

    async def delete(self, request: Request, pks: list[str]) -> int:
        current_user = getattr(request.state, "user", None)

        all_users = await self._backend.list_users()

        for username in pks:
            # Cannot delete self
            if current_user and username == current_user.username:
                raise ValueError("Cannot delete your own account")

            # Check this won't leave zero admins
            admin_count = 0
            for u in all_users:
                if u.username == username:
                    continue
                perms = _resolve_authuser_permissions(u)
                if has_any_admin_permission(perms):
                    admin_count += 1

            if admin_count == 0:
                raise ValueError("Cannot delete the last user with admin permissions")

            await self._backend.delete_user(username)
        return len(pks)

    # -- Serialization --

    async def serialize(
        self, obj: AuthUser, request: Request, action: str = "list"
    ) -> dict[str, Any]:
        return {
            "username": obj.username,
            "display_name": obj.display_name or "",
            "email": obj.email or "",
            "role": obj.role,
            "is_active": True,  # list_users only returns active users
            "oidc_sub": obj.oidc_sub,
            "created_at": "",
            "updated_at": "",
        }

    async def get_pk(self, obj: AuthUser) -> str:
        return obj.username

    async def is_row_action_allowed(self, request: Request, name: str) -> bool:
        if name == "reset_password":
            # Reset password is only allowed for non-OIDC users
            # We need to check the actual user to see if they have an OIDC sub
            return self.can_edit(request)
        return await super().is_row_action_allowed(request, name)

    # -- Row actions --

    @row_action(
        name="view", text="View", icon_class="fas fa-eye", exclude_from_detail=True
    )
    async def view_action(self, request: Request, pk: str) -> Response:
        route_name = request.app.state.ROUTE_NAME
        url = str(
            request.url_for(route_name + ":admin-detail", identity=self.identity, pk=pk)
        )
        return RedirectResponse(url, status_code=HTTP_302_FOUND)

    @row_action(name="edit", text="Edit", icon_class="fas fa-edit")
    async def edit_action(self, request: Request, pk: str) -> Response:
        route_name = request.app.state.ROUTE_NAME
        url = str(
            request.url_for(route_name + ":admin-edit", identity=self.identity, pk=pk)
        )
        return RedirectResponse(url, status_code=HTTP_302_FOUND)

    @row_action(
        name="delete",
        text="Delete",
        confirmation="Are you sure you want to delete this user?",
        icon_class="fas fa-trash",
    )
    async def delete_row_action(self, request: Request, pk: str) -> str:
        await self.delete(request, [pk])
        return "User was successfully deleted"

    @row_action(
        name="reset_password",
        text="Reset Password",
        confirmation="Generate a new random password for this user?",
        icon_class="fas fa-key",
    )
    async def reset_password_action(self, request: Request, pk: str) -> str:
        chars = string.ascii_letters + string.digits
        new_password = "".join(secrets.choice(chars) for _ in range(16))
        await self._backend.update_user(pk, password=new_password)
        return f"New password: {new_password}"

    # -- Batch actions --

    @action(
        name="delete",
        text="Delete selected",
        confirmation="Are you sure you want to delete the selected users?",
        submit_btn_class="btn-danger",
        submit_btn_text="Yes, delete",
    )
    async def delete_action(self, request: Request, pks: list[str]) -> str:
        count = await self.delete(request, pks)
        return f"{count} user(s) were successfully deleted"


# ---------------------------------------------------------------------------
# RoleView
# ---------------------------------------------------------------------------


class RoleView(BaseAdminView):
    identity = "roles"
    label = "Role"
    plural_label = "Roles"
    icon = "shield"

    list_columns = ["name", "is_builtin", "user_count", "created_at"]
    detail_fields = ["name", "is_builtin", "permissions", "created_at", "updated_at"]
    create_fields = ["name", "permissions"]
    edit_fields = ["permissions"]

    def is_accessible(self, request: Request) -> bool:
        return can_view_roles(_get_admin_perms(request))

    def can_create(self, request: Request) -> bool:
        return can_edit_roles(_get_admin_perms(request))

    def can_edit(self, request: Request) -> bool:
        return can_edit_roles(_get_admin_perms(request))

    def can_delete(self, request: Request) -> bool:
        return can_edit_roles(_get_admin_perms(request))

    # -- CRUD --

    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
    ) -> list[Any]:
        roles = await self._backend.list_roles()
        return roles[skip : skip + limit]

    async def count(self, request: Request, where: dict[str, Any] | None = None) -> int:
        roles = await self._backend.list_roles()
        return len(roles)

    async def find_by_pk(self, request: Request, pk: str) -> Any:
        return await self._backend.get_role(pk)

    async def create(self, request: Request, data: dict[str, Any]) -> Any:
        return await self._backend.create_role(
            name=data["name"],
            permissions=data.get("permissions", {}),
        )

    async def edit(self, request: Request, pk: str, data: dict[str, Any]) -> Any:
        role = await self._backend.get_role(pk)
        if role and role.is_builtin:
            raise ValueError(f"Built-in role '{pk}' cannot be modified")
        return await self._backend.update_role(pk, permissions=data.get("permissions"))

    async def delete(self, request: Request, pks: list[str]) -> int:
        for name in pks:
            role = await self._backend.get_role(name)
            if role and role.is_builtin:
                raise ValueError(f"Built-in role '{name}' cannot be deleted")
            await self._backend.delete_role(name)
        return len(pks)

    # -- Serialization --

    async def serialize(
        self, obj: Any, request: Request, action: str = "list"
    ) -> dict[str, Any]:
        raw_perms = getattr(obj, "permissions", None)
        if isinstance(raw_perms, dict):
            permissions = dict(raw_perms)
        elif isinstance(raw_perms, str):
            permissions = json.loads(raw_perms) if raw_perms else {}
        else:
            permissions = {}
        # user_count: check __dict__ directly to avoid triggering a lazy-load
        # on a detached ORM object (get_role returns outside session).
        user_count = 0
        users_rel = obj.__dict__.get("users")
        if users_rel is not None:
            try:
                user_count = len(users_rel)
            except Exception:
                pass
        return {
            "name": obj.name,
            "is_builtin": obj.is_builtin,
            "permissions": permissions,
            "user_count": user_count,
            "created_at": (obj.created_at.isoformat() if obj.created_at else ""),
            "updated_at": (obj.updated_at.isoformat() if obj.updated_at else ""),
        }

    async def get_pk(self, obj: Any) -> str:
        return obj.name

    # -- Row actions --

    @row_action(
        name="view", text="View", icon_class="fas fa-eye", exclude_from_detail=True
    )
    async def view_action(self, request: Request, pk: str) -> Response:
        route_name = request.app.state.ROUTE_NAME
        url = str(
            request.url_for(route_name + ":admin-detail", identity=self.identity, pk=pk)
        )
        return RedirectResponse(url, status_code=HTTP_302_FOUND)

    @row_action(name="edit", text="Edit", icon_class="fas fa-edit")
    async def edit_action(self, request: Request, pk: str) -> Response:
        route_name = request.app.state.ROUTE_NAME
        url = str(
            request.url_for(route_name + ":admin-edit", identity=self.identity, pk=pk)
        )
        return RedirectResponse(url, status_code=HTTP_302_FOUND)

    @row_action(
        name="delete",
        text="Delete",
        confirmation="Are you sure you want to delete this role?",
        icon_class="fas fa-trash",
    )
    async def delete_row_action(self, request: Request, pk: str) -> str:
        await self.delete(request, [pk])
        return "Role was successfully deleted"

    # -- Batch actions --

    @action(
        name="delete",
        text="Delete selected",
        confirmation="Are you sure you want to delete the selected roles?",
        submit_btn_class="btn-danger",
        submit_btn_text="Yes, delete",
    )
    async def delete_action(self, request: Request, pks: list[str]) -> str:
        count = await self.delete(request, pks)
        return f"{count} role(s) were successfully deleted"


# ---------------------------------------------------------------------------
# OIDCProviderView
# ---------------------------------------------------------------------------


def _mask_secret(secret: str) -> str:
    """Mask a client secret for display, showing only last 4 characters."""
    if not secret or len(secret) < 4:
        return "••••••••"
    return "••••••••" + secret[-4:]


class OIDCProviderView(BaseAdminView):
    identity = "oidc"
    label = "OIDC Provider"
    plural_label = "OIDC Providers"
    icon = "shield-alt"

    list_columns = [
        "display_name",
        "issuer_url",
        "user_count",
        "enabled",
        "display_order",
        "created_at",
    ]
    detail_fields = [
        "name",
        "display_name",
        "issuer_url",
        "client_id",
        "client_secret",
        "scopes",
        "enabled",
        "display_order",
        "user_count",
    ]
    create_fields = [
        "name",
        "display_name",
        "issuer_url",
        "client_id",
        "client_secret",
        "scopes",
        "display_order",
    ]
    edit_fields = [
        "display_name",
        "issuer_url",
        "client_id",
        "client_secret",
        "scopes",
        "enabled",
        "display_order",
    ]

    def is_accessible(self, request: Request) -> bool:
        return can_view_oidc(_get_admin_perms(request))

    def can_create(self, request: Request) -> bool:
        return can_edit_oidc(_get_admin_perms(request))

    def can_edit(self, request: Request) -> bool:
        return can_edit_oidc(_get_admin_perms(request))

    def can_delete(self, request: Request) -> bool:
        return can_edit_oidc(_get_admin_perms(request))

    # -- CRUD --

    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
    ) -> list[Any]:
        providers = await self._backend.list_oidc_providers()
        return providers[skip : skip + limit]

    async def count(self, request: Request, where: dict[str, Any] | None = None) -> int:
        providers = await self._backend.list_oidc_providers()
        return len(providers)

    async def find_by_pk(self, request: Request, pk: str) -> Any:
        return await self._backend.get_oidc_provider(pk)

    async def create(self, request: Request, data: dict[str, Any]) -> Any:
        name = data["name"].strip()
        existing = await self._backend.get_oidc_provider(name)
        if existing is not None:
            raise ValueError(f"OIDC provider '{name}' already exists")
        return await self._backend.create_oidc_provider(
            name=name,
            display_name=data["display_name"].strip(),
            issuer_url=data["issuer_url"].strip(),
            client_id=data["client_id"].strip(),
            client_secret=data["client_secret"].strip(),
            scopes=data.get("scopes", "openid email profile").strip(),
            display_order=int(data.get("display_order", 0)),
        )

    async def edit(self, request: Request, pk: str, data: dict[str, Any]) -> Any:
        kwargs: dict[str, Any] = {}
        if "display_name" in data and data["display_name"]:
            kwargs["display_name"] = data["display_name"].strip()
        if "issuer_url" in data and data["issuer_url"]:
            kwargs["issuer_url"] = data["issuer_url"].strip()
        if "client_id" in data and data["client_id"]:
            kwargs["client_id"] = data["client_id"].strip()
        # Only update client_secret if a non-empty value is provided
        if "client_secret" in data and data["client_secret"]:
            kwargs["client_secret"] = data["client_secret"].strip()
        if "scopes" in data:
            kwargs["scopes"] = data["scopes"].strip()
        if "enabled" in data:
            kwargs["enabled"] = data["enabled"] in ("true", "True", "on", True, "1")
        if "display_order" in data:
            kwargs["display_order"] = int(data["display_order"])
        return await self._backend.update_oidc_provider(pk, **kwargs)

    async def delete(self, request: Request, pks: list[str]) -> int:
        for name in pks:
            await self._backend.delete_oidc_provider(name)
        return len(pks)

    # -- Serialization --

    async def serialize(
        self, obj: Any, request: Request, action: str = "list"
    ) -> dict[str, Any]:
        # user_count: check __dict__ directly to avoid triggering a lazy-load
        # on a detached ORM object.
        user_count = 0
        users_rel = obj.__dict__.get("users")
        if users_rel is not None:
            try:
                user_count = len(users_rel)
            except Exception:
                pass

        result = {
            "name": obj.name,
            "display_name": obj.display_name,
            "issuer_url": obj.issuer_url,
            "client_id": obj.client_id,
            "client_secret": _mask_secret(obj.client_secret),
            "scopes": obj.scopes,
            "enabled": obj.enabled,
            "display_order": obj.display_order,
            "user_count": user_count,
            "created_at": (obj.created_at.isoformat() if obj.created_at else ""),
            "updated_at": (obj.updated_at.isoformat() if obj.updated_at else ""),
        }
        # In edit mode, don't mask the secret so the form can show a placeholder
        if action == "edit":
            result["client_secret"] = ""  # Empty = keep current value
        return result

    async def get_pk(self, obj: Any) -> str:
        return obj.name

    # -- Row actions --

    @row_action(
        name="view", text="View", icon_class="fas fa-eye", exclude_from_detail=True
    )
    async def view_action(self, request: Request, pk: str) -> Response:
        route_name = request.app.state.ROUTE_NAME
        url = str(
            request.url_for(route_name + ":admin-detail", identity=self.identity, pk=pk)
        )
        return RedirectResponse(url, status_code=HTTP_302_FOUND)

    @row_action(name="edit", text="Edit", icon_class="fas fa-edit")
    async def edit_action(self, request: Request, pk: str) -> Response:
        route_name = request.app.state.ROUTE_NAME
        url = str(
            request.url_for(route_name + ":admin-edit", identity=self.identity, pk=pk)
        )
        return RedirectResponse(url, status_code=HTTP_302_FOUND)

    @row_action(
        name="delete",
        text="Delete",
        confirmation="Are you sure you want to delete this OIDC provider?",
        icon_class="fas fa-trash",
    )
    async def delete_row_action(self, request: Request, pk: str) -> str:
        await self.delete(request, [pk])
        return "OIDC provider was successfully deleted"

    # -- Batch actions --

    @action(
        name="delete",
        text="Delete selected",
        confirmation="Are you sure you want to delete the selected OIDC providers?",
        submit_btn_class="btn-danger",
        submit_btn_text="Yes, delete",
    )
    async def delete_action(self, request: Request, pks: list[str]) -> str:
        count = await self.delete(request, pks)
        return f"{count} OIDC provider(s) were successfully deleted"


# ---------------------------------------------------------------------------
# Helpers for self-protection
# ---------------------------------------------------------------------------


def _resolve_authuser_permissions_for_role(
    role_obj: Any,
) -> dict[str, PermissionResult]:
    """Build a PermissionResult map for a database Role ORM object.

    Used by self-protection checks in UserView.edit().
    """
    from dagster_webserver.auth.roles import (
        Role as RoleEnum,
    )
    from dagster_webserver.auth.roles import (
        _admin_permissions_for_role,
        get_custom_permissions,
    )

    if role_obj.is_builtin:
        try:
            role_enum = RoleEnum(role_obj.name)
            return _admin_permissions_for_role(role_enum)
        except ValueError:
            return {}
    else:
        # Custom role — permissions stored as JSON
        raw = getattr(role_obj, "permissions", None)
        if isinstance(raw, dict):
            raw_perms = dict(raw)
        elif isinstance(raw, str):
            raw_perms = json.loads(raw) if raw else {}
        else:
            raw_perms = {}
        return get_custom_permissions(raw_perms)


# ---------------------------------------------------------------------------
# ServiceAccountView
# ---------------------------------------------------------------------------


class ServiceAccountView(BaseAdminView):
    identity = "service-accounts"
    label = "Service Account"
    plural_label = "Service Accounts"
    icon = "robot"

    list_columns = ["username", "display_name", "created_at"]
    detail_fields = ["username", "display_name", "created_at"]
    create_fields = ["display_name"]
    edit_fields: list[
        str
    ] = []  # No standard fields to edit; edit page shows role management

    def is_accessible(self, request: Request) -> bool:
        return can_view_users(_get_admin_perms(request))

    def can_create(self, request: Request) -> bool:
        return can_edit_users(_get_admin_perms(request))

    def can_edit(self, request: Request) -> bool:
        return can_edit_users(_get_admin_perms(request))

    def can_delete(self, request: Request) -> bool:
        return can_edit_users(_get_admin_perms(request))

    # -- CRUD --

    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
    ) -> list[AuthUser]:
        accounts = await self._backend.list_service_accounts()
        return accounts[skip : skip + limit]

    async def count(self, request: Request, where: dict[str, Any] | None = None) -> int:
        accounts = await self._backend.list_service_accounts()
        return len(accounts)

    async def find_by_pk(self, request: Request, pk: str) -> AuthUser | None:
        user = await self._backend.get_user(pk)
        if user is None or not getattr(user, "is_service_account", False):
            return None
        return user

    @staticmethod
    def _generate_username(display_name: str) -> str:
        """Generate a service account username from the display name.

        Format: svc_<slugified_display_name>
        e.g. 'My CI Bot' → 'svc_my_ci_bot'
        """
        slug = display_name.strip().lower()
        # Replace non-alphanumeric chars with underscores
        slug = re.sub(r"[^a-z0-9]+", "_", slug).strip("_")
        if not slug:
            raise ValueError("Display name must contain at least one letter or number")
        return f"svc_{slug}"

    async def create(self, request: Request, data: dict[str, Any]) -> Any:
        display_name = data["display_name"].strip()
        if not display_name:
            raise ValueError("Display name is required")
        username = self._generate_username(display_name)
        existing = await self._backend.get_user(username)
        if existing is not None:
            raise ValueError(
                f"Service account '{username}' already exists. "
                f"Use a different display name."
            )

        # Determine global role from assignments (default to viewer)
        role_assignments = UserView._parse_role_assignments(data)
        global_role = "viewer"
        for a in role_assignments:
            if a["code_location_name"] is None:
                global_role = a["role_name"]
                break

        user = await self._backend.create_service_account(
            username=username,
            role_name=global_role,
            display_name=display_name,
        )

        # Set all role assignments (including location-scoped ones)
        if len(role_assignments) > 1 or any(
            a["code_location_name"] is not None for a in role_assignments
        ):
            await self._backend.set_user_role_assignments(username, role_assignments)

        return user

    async def edit(self, request: Request, pk: str, data: dict[str, Any]) -> Any:
        # Service accounts have no editable standard fields — only role assignments.
        role_assignments = UserView._parse_role_assignments(data)
        if role_assignments:
            await self._backend.set_user_role_assignments(pk, role_assignments)
        return await self._backend.get_user(pk)

    async def delete(self, request: Request, pks: list[str]) -> int:
        for username in pks:
            await self._backend.delete_user(username)
        return len(pks)

    # -- Serialization --

    async def serialize(
        self, obj: AuthUser, request: Request, action: str = "list"
    ) -> dict[str, Any]:
        return {
            "username": obj.username,
            "display_name": obj.display_name or "",
            "created_at": "",
        }

    async def get_pk(self, obj: AuthUser) -> str:
        return obj.username

    # -- Row actions --

    @row_action(
        name="view", text="View", icon_class="fas fa-eye", exclude_from_detail=True
    )
    async def view_action(self, request: Request, pk: str) -> Response:
        route_name = request.app.state.ROUTE_NAME
        url = str(
            request.url_for(route_name + ":admin-detail", identity=self.identity, pk=pk)
        )
        return RedirectResponse(url, status_code=HTTP_302_FOUND)

    @row_action(name="edit", text="Edit", icon_class="fas fa-edit")
    async def edit_action(self, request: Request, pk: str) -> Response:
        route_name = request.app.state.ROUTE_NAME
        url = str(
            request.url_for(route_name + ":admin-edit", identity=self.identity, pk=pk)
        )
        return RedirectResponse(url, status_code=HTTP_302_FOUND)

    @row_action(
        name="delete",
        text="Delete",
        confirmation="Are you sure? This will also revoke all API keys for this account.",
        icon_class="fas fa-trash",
    )
    async def delete_row_action(self, request: Request, pk: str) -> str:
        await self.delete(request, [pk])
        return f"Service account '{pk}' was deleted"

    # -- Batch actions --

    @action(
        name="delete",
        text="Delete selected",
        confirmation="Are you sure you want to delete the selected service accounts?",
        submit_btn_class="btn-danger",
        submit_btn_text="Yes, delete",
    )
    async def delete_action(self, request: Request, pks: list[str]) -> str:
        count = await self.delete(request, pks)
        return f"{count} service account(s) were deleted"


# ---------------------------------------------------------------------------
# ApiKeyView
# ---------------------------------------------------------------------------


class ApiKeyView(BaseAdminView):
    identity = "api-keys"
    label = "API Key"
    plural_label = "API Keys"
    icon = "key"

    list_columns = ["name", "username", "is_active", "created_at", "expires_at"]
    detail_fields = [
        "name",
        "username",
        "is_active",
        "created_at",
        "expires_at",
        "last_used_at",
    ]
    create_fields = ["username", "name", "expires_in_days"]
    edit_fields: list[str] = []  # Keys are immutable

    def is_accessible(self, request: Request) -> bool:
        return can_view_users(_get_admin_perms(request))

    def can_create(self, request: Request) -> bool:
        return can_edit_users(_get_admin_perms(request))

    def can_edit(self, request: Request) -> bool:
        return False

    def can_delete(self, request: Request) -> bool:
        return can_edit_users(_get_admin_perms(request))

    # -- CRUD --

    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
    ) -> list[Any]:
        """List all API keys across all users."""
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload

        from dagster_webserver.database import ApiKey as ApiKeyModel

        await self._backend._ensure_ready()
        from dagster_webserver.database import get_session_factory

        async with get_session_factory()() as session:
            result = await session.execute(
                select(ApiKeyModel)
                .options(selectinload(ApiKeyModel.user))
                .order_by(ApiKeyModel.created_at.desc())
            )
            keys = list(result.scalars().all())
        return keys[skip : skip + limit]

    async def count(self, request: Request, where: dict[str, Any] | None = None) -> int:
        items = await self.find_all(request, limit=10000)
        return len(items)

    async def find_by_pk(self, request: Request, pk: str) -> Any | None:
        """Find API key by integer ID."""
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload

        from dagster_webserver.database import ApiKey as ApiKeyModel
        from dagster_webserver.database import get_session_factory

        await self._backend._ensure_ready()
        try:
            key_id = int(pk)
        except ValueError:
            return None

        async with get_session_factory()() as session:
            result = await session.execute(
                select(ApiKeyModel)
                .options(selectinload(ApiKeyModel.user))
                .where(ApiKeyModel.id == key_id)
            )
            return result.scalar_one_or_none()

    async def create(self, request: Request, data: dict[str, Any]) -> Any:
        """Create an API key and show the plaintext key once."""
        username = data["username"].strip()
        user = await self._backend.get_user(username)
        if user is None:
            raise ValueError(f"User '{username}' not found")

        name = data["name"].strip() or f"key-{datetime.now().strftime('%Y%m%d')}"
        expires_in_days = None
        if data.get("expires_in_days", "").strip():
            try:
                expires_in_days = int(data["expires_in_days"])
            except ValueError:
                raise ValueError("'expires_in_days' must be a whole number")

        api_key_obj, plaintext_key = await self._backend.create_api_key(
            username=username,
            name=name,
            expires_in_days=expires_in_days,
        )

        # Return an HTML response that shows the key once
        from starlette.responses import HTMLResponse

        return HTMLResponse(
            f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>API Key Created</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css"/>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f6f8; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }}
.card {{ background: #fff; border-radius: 12px; padding: 40px; max-width: 560px; width: 100%; box-shadow: 0 4px 24px rgba(3,6,21,.1); text-align: center; }}
.card h2 {{ margin: 0 0 8px; color: #171c2c; }}
.card p {{ color: #6f7a91; font-size: 14px; margin: 0 0 24px; }}
.key-box {{ background: #171c2c; color: #7cffb2; font-family: 'SF Mono', Monaco, monospace; font-size: 13px; padding: 16px; border-radius: 8px; word-break: break-all; margin-bottom: 16px; }}
.warning {{ background: #fef0ed; border: 1px solid #f5c2b8; color: #c72a1c; padding: 12px; border-radius: 6px; font-size: 13px; margin-bottom: 24px; }}
.btn {{ display: inline-block; padding: 10px 24px; border-radius: 6px; font-size: 14px; font-weight: 500; text-decoration: none; }}
.btn-primary {{ background: #4f438d; color: #fff; }}
.btn-primary:hover {{ background: #4037b5; }}
</style></head>
<body>
<div class="card">
  <i class="fas fa-key" style="font-size: 32px; color: #4f438d; margin-bottom: 16px;"></i>
  <h2>API Key Created</h2>
  <p>For user <strong>{username}</strong> — "{name}"</p>
  <div class="key-box">{plaintext_key}</div>
  <div class="warning"><i class="fas fa-exclamation-triangle"></i> Copy this key now. It will <strong>never be shown again</strong>.</div>
  <a href="/admin/api-keys/list" class="btn btn-primary">Done — Back to API Keys</a>
</div>
</body></html>"""
        )

    async def edit(self, request: Request, pk: str, data: dict[str, Any]) -> Any:
        raise ValueError("API keys cannot be edited. Revoke and create a new one.")

    async def delete(self, request: Request, pks: list[str]) -> int:
        count = 0
        for pk in pks:
            try:
                key_id = int(pk)
            except ValueError:
                continue
            if await self._backend.revoke_api_key(key_id):
                count += 1
        return count

    # -- Serialization --

    async def serialize(
        self, obj: Any, request: Request, action: str = "list"
    ) -> dict[str, Any]:
        username = obj.user.username if obj.user else "?"
        return {
            "_pk": str(obj.id),
            "name": obj.name,
            "username": username,
            "is_active": obj.is_active,
            "created_at": (
                obj.created_at.strftime("%Y-%m-%d %H:%M") if obj.created_at else ""
            ),
            "expires_at": (
                obj.expires_at.strftime("%Y-%m-%d") if obj.expires_at else "Never"
            ),
            "last_used_at": (
                obj.last_used_at.strftime("%Y-%m-%d %H:%M")
                if obj.last_used_at
                else "Never"
            ),
        }

    async def get_pk(self, obj: Any) -> str:
        return str(obj.id)

    # -- Row actions --

    @row_action(
        name="view", text="View", icon_class="fas fa-eye", exclude_from_detail=True
    )
    async def view_action(self, request: Request, pk: str) -> Response:
        route_name = request.app.state.ROUTE_NAME
        url = str(
            request.url_for(route_name + ":admin-detail", identity=self.identity, pk=pk)
        )
        return RedirectResponse(url, status_code=HTTP_302_FOUND)

    @row_action(
        name="delete",
        text="Revoke",
        confirmation="Are you sure you want to revoke this API key? It will stop working immediately.",
        icon_class="fas fa-ban",
    )
    async def delete_row_action(self, request: Request, pk: str) -> str:
        await self.delete(request, [pk])
        return "API key was revoked"

    # -- Batch actions --

    @action(
        name="delete",
        text="Revoke selected",
        confirmation="Are you sure you want to revoke the selected API keys?",
        submit_btn_class="btn-danger",
        submit_btn_text="Yes, revoke",
    )
    async def delete_action(self, request: Request, pks: list[str]) -> str:
        count = await self.delete(request, pks)
        return f"{count} API key(s) were revoked"


# ---------------------------------------------------------------------------
# CodeLocationView
# ---------------------------------------------------------------------------


class CodeLocationView(BaseAdminView):
    identity = "code-locations"
    label = "Code Location"
    plural_label = "Code Locations"
    icon = "code"

    list_columns = ["location_name", "image", "status", "labels", "created_at"]
    detail_fields = [
        "location_name",
        "status",
        "deploy_metadata",
        "labels",
        "created_at",
        "updated_at",
    ]
    create_fields = [
        "location_name",
        "image",
        "python_file",
        "module_name",
        "working_directory",
        "executable_path",
        "attribute",
    ]
    edit_fields = [
        "image",
        "python_file",
        "module_name",
        "working_directory",
        "executable_path",
        "attribute",
        "status",
    ]

    def is_accessible(self, request: Request) -> bool:
        return can_view_code_locations(_get_admin_perms(request))

    def can_create(self, request: Request) -> bool:
        return can_edit_code_locations(_get_admin_perms(request))

    def can_edit(self, request: Request) -> bool:
        return can_edit_code_locations(_get_admin_perms(request))

    def can_delete(self, request: Request) -> bool:
        return can_edit_code_locations(_get_admin_perms(request))

    # -- CRUD --

    async def find_all(
        self,
        request: Request,
        skip: int = 0,
        limit: int = 100,
        where: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
    ) -> list[Any]:
        locations = await self._backend.list_code_locations()
        return locations[skip : skip + limit]

    async def count(self, request: Request, where: dict[str, Any] | None = None) -> int:
        locations = await self._backend.list_code_locations()
        return len(locations)

    async def find_by_pk(self, request: Request, pk: str) -> Any | None:
        return await self._backend.get_code_location(pk)

    async def create(self, request: Request, data: dict[str, Any]) -> Any:
        location_name = data["location_name"].strip()
        if not location_name:
            raise ValueError("Location name is required")

        deploy_metadata: dict[str, Any] = {}
        for field in (
            "image",
            "python_file",
            "module_name",
            "working_directory",
            "executable_path",
            "attribute",
        ):
            val = data.get(field, "").strip()
            if val:
                deploy_metadata[field] = val

        if not deploy_metadata.get("python_file") and not deploy_metadata.get(
            "module_name"
        ):
            # Container launchers (k8s/docker) run `dagster api grpc` inside
            # the image, so a code pointer is still required even when an
            # image is set.
            raise ValueError("Either 'python_file' or 'module_name' is required")

        return await self._backend.create_code_location(
            location_name=location_name,
            deploy_metadata=deploy_metadata,
        )

    async def edit(self, request: Request, pk: str, data: dict[str, Any]) -> Any:
        from dagster_webserver.database import CODE_LOCATION_STATUSES

        deploy_metadata: dict[str, Any] = {}
        for field in (
            "image",
            "python_file",
            "module_name",
            "working_directory",
            "executable_path",
            "attribute",
        ):
            val = data.get(field, "").strip()
            if val:
                deploy_metadata[field] = val

        status = data.get("status") or None
        if status is not None and status not in CODE_LOCATION_STATUSES:
            raise ValueError(
                f"Invalid status '{status}'. Must be one of: "
                + ", ".join(CODE_LOCATION_STATUSES)
            )

        return await self._backend.update_code_location(
            location_name=pk,
            deploy_metadata=deploy_metadata if deploy_metadata else None,
            status=status,
        )

    async def delete(self, request: Request, pks: list[str]) -> int:
        count = 0
        for name in pks:
            if await self._backend.delete_code_location(name):
                count += 1
        return count

    # -- Serialization --

    async def serialize(
        self, obj: Any, request: Request, action: str = "list"
    ) -> dict[str, Any]:
        labels_str = ""
        if obj.labels:
            labels_str = ", ".join(f"{k}={v}" for k, v in obj.labels.items())

        result = {
            "location_name": obj.location_name,
            "status": obj.status,
            "labels": labels_str or "—",
            "created_at": (
                obj.created_at.strftime("%Y-%m-%d %H:%M") if obj.created_at else ""
            ),
            "updated_at": (
                obj.updated_at.strftime("%Y-%m-%d %H:%M") if obj.updated_at else ""
            ),
        }

        dm = obj.deploy_metadata or {}
        # Edit/detail forms prefill from these values, so keep them empty
        # (not the list placeholder) when no image is set.
        result["image"] = dm.get("image", "") if action in ("detail", "edit") else (dm.get("image") or "—")

        if action in ("detail", "edit"):
            result["deploy_metadata"] = json.dumps(dm, indent=2)
            result["python_file"] = dm.get("python_file", "")
            result["module_name"] = dm.get("module_name", "")
            result["working_directory"] = dm.get("working_directory", "")
            result["executable_path"] = dm.get("executable_path", "")
            result["attribute"] = dm.get("attribute", "")

        return result

    async def get_pk(self, obj: Any) -> str:
        return obj.location_name

    # -- Row actions --

    @row_action(
        name="view", text="View", icon_class="fas fa-eye", exclude_from_detail=True
    )
    async def view_action(self, request: Request, pk: str) -> Response:
        route_name = request.app.state.ROUTE_NAME
        url = str(
            request.url_for(route_name + ":admin-detail", identity=self.identity, pk=pk)
        )
        return RedirectResponse(url, status_code=HTTP_302_FOUND)

    @row_action(name="edit", text="Edit", icon_class="fas fa-edit")
    async def edit_action(self, request: Request, pk: str) -> Response:
        route_name = request.app.state.ROUTE_NAME
        url = str(
            request.url_for(route_name + ":admin-edit", identity=self.identity, pk=pk)
        )
        return RedirectResponse(url, status_code=HTTP_302_FOUND)

    @row_action(
        name="delete",
        text="Delete",
        confirmation="Are you sure you want to remove this code location? Agents will stop managing it.",
        icon_class="fas fa-trash",
    )
    async def delete_row_action(self, request: Request, pk: str) -> str:
        await self.delete(request, [pk])
        return f"Code location '{pk}' was removed"

    # -- Batch actions --

    @action(
        name="delete",
        text="Delete selected",
        confirmation="Are you sure you want to remove the selected code locations?",
        submit_btn_class="btn-danger",
        submit_btn_text="Yes, delete",
    )
    async def delete_action(self, request: Request, pks: list[str]) -> str:
        count = await self.delete(request, pks)
        return f"{count} code location(s) were removed"


# ---------------------------------------------------------------------------
# Scoped role assignment helper (used by UserView detail page)
# ---------------------------------------------------------------------------


async def get_user_scoped_roles(
    backend: DatabaseUserBackend, username: str
) -> list[dict[str, Any]]:
    """Get all role assignments for a user in a display-friendly format."""
    assignments = await backend.get_user_role_assignments(username)
    result = []
    for a in assignments:
        result.append(
            {
                "role_name": a.role.name if a.role else "?",
                "code_location_name": a.code_location_name or "(global)",
                "is_global": a.code_location_name is None,
            }
        )
    return result
