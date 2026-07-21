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
import secrets
import string
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from dagster._core.workspace.permissions import PermissionResult
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response
from starlette.status import HTTP_302_FOUND

from dagster_webserver.admin.permissions import (
    can_edit_oidc,
    can_edit_roles,
    can_edit_users,
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
        "role",
        "is_active",
        "created_at",
    ]
    detail_fields = [
        "username",
        "display_name",
        "email",
        "role",
        "is_active",
        "created_at",
        "updated_at",
    ]
    create_fields = ["username", "password", "role", "email", "display_name"]
    edit_fields = [
        "username",
        "password",
        "role",
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
        return users[skip : skip + limit]

    async def count(self, request: Request, where: dict[str, Any] | None = None) -> int:
        users = await self._backend.list_users()
        return len(users)

    async def find_by_pk(self, request: Request, pk: str) -> AuthUser | None:
        return await self._backend.get_user(pk)

    async def create(self, request: Request, data: dict[str, Any]) -> AuthUser:
        username = data["username"].strip()
        existing = await self._backend.get_user(username)
        if existing is not None:
            raise ValueError(f"User '{username}' already exists")
        return await self._backend.create_user(
            username=username,
            password=data["password"],
            role=data.get("role", "viewer"),
            email=data.get("email"),
            display_name=data.get("display_name"),
        )

    async def edit(self, request: Request, pk: str, data: dict[str, Any]) -> AuthUser:
        current_user = getattr(request.state, "user", None)

        # Self-protection: cannot self-demote
        if current_user and pk == current_user.username:
            new_role = data.get("role")
            if new_role:
                new_role_obj = await self._backend.get_role(new_role)
                if new_role_obj:
                    # Build permission map for the new role
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

        return await self._backend.update_user(
            pk,
            password=data.get("password") or None,
            role=data.get("role"),
            email=data.get("email") or None,
            display_name=data.get("display_name") or None,
            is_active=is_active,
        )

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
