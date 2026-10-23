"""Database-backed ``UserBackend`` implementation.

Stores users and custom roles in a relational database via SQLAlchemy.
Imports models and session from ``dagster_webserver.database`` to keep
database infrastructure cleanly separated from auth logic.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import selectinload

from dagster_webserver.auth.users import (
    AuthUser,
    UserBackend,
    _hash_password,
    _verify_password,
)
from dagster_webserver.database import (
    ApiKey,
    OIDCProvider,
    Role,
    User,
    UserRoleAssignment,
    get_engine,
    get_session_factory,
    init_engine,
)
from dagster_webserver.database.models import Base

logger = logging.getLogger("dagster-webserver.auth")

# Reusable eager-load option: loads role_assignments and their roles in one query.
_USER_ROLE_ASSIGNMENTS_LOAD = selectinload(User.role_assignments).selectinload(
    UserRoleAssignment.role
)


class DatabaseUserBackend(UserBackend):
    """User backend backed by a relational database.

    Parameters
    ----------
    database_url:
        SQLAlchemy connection string (e.g.
        ``sqlite+aiosqlite:///auth.db`` or
        ``postgresql+asyncpg://user:pass@host/db``).
    create_tables:
        If ``True``, create tables and seed built-in roles on startup.
    default_role:
        Role name to use when a user has no explicit ``role_id``
        assignment (falls back to ``"viewer"``).
    """

    def __init__(
        self,
        database_url: str,
        *,
        create_tables: bool = True,
        default_role: str = "viewer",
    ) -> None:
        self._database_url = database_url
        self._default_role_name = default_role
        self._default_role: Role | None = None
        self._create_tables = create_tables

        init_engine(database_url)

    async def _ensure_tables(self) -> None:
        """Create tables and seed built-in roles if needed.

        Called lazily on first async operation so we're inside an event loop.
        """
        if not self._create_tables:
            return

        engine = get_engine()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

            # Seed built-in roles if the table is empty
            from dagster_webserver.auth.roles import ROLE_PERMISSIONS
            from dagster_webserver.auth.roles import Role as RoleEnum

            result = await conn.execute(
                text("SELECT COUNT(*) FROM roles WHERE is_builtin = 1")
            )
            count = result.scalar()

            if count == 0:
                for role in RoleEnum:
                    perm_map = {
                        perm.name: enabled
                        for perm, enabled in ROLE_PERMISSIONS[role].items()
                    }
                    await conn.execute(
                        text(
                            "INSERT INTO roles (name, permissions, is_builtin) "
                            "VALUES (:name, :permissions, :is_builtin)"
                        ),
                        {
                            "name": role.value,
                            "permissions": json.dumps(perm_map),
                            "is_builtin": True,
                        },
                    )

        self._create_tables = False  # only run once

    async def _ensure_ready(self) -> None:
        """Ensure tables exist and resolve the default role."""
        await self._ensure_tables()
        await self._resolve_default_role()

    # ── UserBackend ABC ──────────────────────────────────────────

    async def authenticate(self, username: str, password: str) -> AuthUser | None:
        """Verify credentials and return the user, or ``None`` if invalid."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.username == username)
            )
            user = result.scalar_one_or_none()

            if not user or not user.is_active:
                return None

            if not _verify_password(password, user.password_hash):
                return None

            return self._to_auth_user(user)

    async def get_user(self, username: str) -> AuthUser | None:
        """Look up user by username (e.g. from session)."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.username == username)
            )
            user = result.scalar_one_or_none()

            if user and user.is_active:
                return self._to_auth_user(user)
            return None

    # ── User CRUD ────────────────────────────────────────────────

    async def create_user(
        self,
        username: str,
        password: str,
        role: str | None = "viewer",
        email: str | None = None,
        display_name: str | None = None,
    ) -> AuthUser:
        """Create a new user with a global role assignment.

        ``role`` is a role name string (e.g. ``"viewer"``, ``"admin"``, or a
        custom role name). Defaults to ``"viewer"`` if not specified.

        Raises
        ------
        ValueError
            If the role name is unknown.
        IntegrityError
            If a user with this username already exists.
        """
        await self._ensure_ready()

        role_obj = await self._lookup_role(role) if role else None

        user = User(
            username=username,
            password_hash=_hash_password(password),
            email=email,
            display_name=display_name,
            is_active=True,
        )

        async with get_session_factory()() as session:
            session.add(user)
            await session.flush()  # Get user.id
            # Create global role assignment if a specific role was provided.
            # If role is None, no global assignment is created — the default
            # role will be used at resolution time in _to_auth_user.
            if role_obj:
                assignment = UserRoleAssignment(
                    user_id=user.id,
                    role_id=role_obj.id,
                    code_location_name=None,  # Global scope
                )
                session.add(assignment)
            await session.commit()
            # Re-fetch with eager loading
            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.id == user.id)
            )
            user = result.scalar_one()
            return self._to_auth_user(user)

    async def update_user(
        self,
        username: str,
        *,
        password: str | None = None,
        role: str | None = None,
        email: str | None = None,
        display_name: str | None = None,
        is_active: bool | None = None,
    ) -> AuthUser:
        """Update one or more fields.  Returns updated ``AuthUser``.

        If ``role`` is provided, updates the user's global role assignment.
        """
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.username == username)
            )
            user = result.scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")

            if password is not None:
                if user.oidc_sub is not None:
                    raise ValueError(
                        f"User '{username}' is linked to an OIDC provider. "
                        "Password cannot be set for OIDC users."
                    )
                user.password_hash = _hash_password(password)

            if role is not None:
                role_obj = await self._lookup_role(role, session=session)
                # Update or create the global role assignment
                global_assignment = next(
                    (a for a in user.role_assignments if a.code_location_name is None),
                    None,
                )
                if global_assignment:
                    global_assignment.role_id = role_obj.id
                    global_assignment.role = role_obj  # Keep in-memory state consistent
                else:
                    new_assign = UserRoleAssignment(
                        user_id=user.id,
                        role_id=role_obj.id,
                        code_location_name=None,
                    )
                    new_assign.role = role_obj
                    session.add(new_assign)
                    user.role_assignments.append(new_assign)

            if email is not None:
                user.email = email
            if display_name is not None:
                user.display_name = display_name
            if is_active is not None:
                user.is_active = is_active

            await session.commit()
            return self._to_auth_user(user)

    async def delete_user(self, username: str) -> None:
        """Hard-delete a user.  Raises ``ValueError`` if not found."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User).where(User.username == username)
            )
            user = result.scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")
            await session.delete(user)
            await session.commit()

    async def list_users(self) -> list[AuthUser]:
        """Return all active users."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.is_active.is_(True))
                .order_by(User.username)
            )
            users = result.scalars().all()
            return [self._to_auth_user(u) for u in users]

    # ── API Key Authentication ───────────────────────────────────

    async def get_user_by_api_key(self, api_key: str) -> AuthUser | None:
        """Look up a user by their API key (SHA-256 hashed lookup)."""
        await self._ensure_ready()

        key_hash = hashlib.sha256(api_key.encode()).hexdigest()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(ApiKey).where(
                    ApiKey.key_hash == key_hash,
                    ApiKey.is_active.is_(True),
                )
            )
            api_key_obj = result.scalar_one_or_none()

            if not api_key_obj:
                return None

            # Check expiration
            if api_key_obj.expires_at and api_key_obj.expires_at < datetime.now(
                timezone.utc
            ):
                return None

            # Load the user with role assignments
            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.id == api_key_obj.user_id)
            )
            user = result.scalar_one_or_none()

            if not user or not user.is_active:
                return None

            # Update last_used_at
            api_key_obj.last_used_at = datetime.now(timezone.utc)
            await session.commit()

            return self._to_auth_user(user)

    async def create_api_key(
        self,
        username: str,
        name: str,
        expires_in_days: int | None = None,
    ) -> tuple[ApiKey, str]:
        """Create a new API key for a user.

        Returns the ORM object and the plaintext key (shown once).
        """
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User).where(User.username == username)
            )
            user = result.scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")

            import secrets

            plaintext_key = secrets.token_urlsafe(32)
            key_hash = hashlib.sha256(plaintext_key.encode()).hexdigest()

            expires_at = None
            if expires_in_days:
                expires_at = datetime.now(timezone.utc) + timedelta(
                    days=expires_in_days
                )

            api_key_obj = ApiKey(
                user_id=user.id,
                key_hash=key_hash,
                name=name,
                is_active=True,
                expires_at=expires_at,
            )
            session.add(api_key_obj)
            await session.commit()
            await session.refresh(api_key_obj)

            return api_key_obj, plaintext_key

    async def list_api_keys(self, username: str) -> list[ApiKey]:
        """List all API keys for a user."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User).where(User.username == username)
            )
            user = result.scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")

            result = await session.execute(
                select(ApiKey)
                .where(ApiKey.user_id == user.id)
                .order_by(ApiKey.created_at.desc())
            )
            return list(result.scalars().all())

    async def revoke_api_key(self, key_id: int) -> bool:
        """Deactivate an API key."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(select(ApiKey).where(ApiKey.id == key_id))
            api_key_obj = result.scalar_one_or_none()
            if api_key_obj is None:
                return False
            api_key_obj.is_active = False
            await session.commit()
            return True

    # ── Service Account Management ───────────────────────────────

    async def create_service_account(
        self,
        username: str,
        role_name: str = "viewer",
        display_name: str | None = None,
    ) -> AuthUser:
        """Create a service account user.

        Service accounts have ``is_service_account=True`` and empty
        ``password_hash`` — they can only authenticate via API key.
        A global role assignment is created in ``user_role_assignments``.
        """
        await self._ensure_ready()

        role_obj = await self._lookup_role(role_name)

        user = User(
            username=username,
            password_hash="",  # No password for service accounts
            display_name=display_name or username,
            is_active=True,
            is_service_account=True,
        )

        async with get_session_factory()() as session:
            session.add(user)
            await session.flush()  # Get user.id

            # Create global role assignment
            assignment = UserRoleAssignment(
                user_id=user.id,
                role_id=role_obj.id,
                code_location_name=None,  # Global
            )
            session.add(assignment)
            await session.commit()

            # Re-fetch with eager loading
            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.id == user.id)
            )
            user = result.scalar_one()
            return self._to_auth_user(user)

    # ── Per-Location Role Assignments ────────────────────────────

    async def assign_role_to_location(
        self,
        username: str,
        role_name: str,
        code_location_name: str | None = None,
    ) -> UserRoleAssignment:
        """Assign a role to a user, optionally scoped to a code location."""
        await self._ensure_ready()

        role_obj = await self._lookup_role(role_name)

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User).where(User.username == username)
            )
            user = result.scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")

            # Check if assignment already exists
            result = await session.execute(
                select(UserRoleAssignment).where(
                    UserRoleAssignment.user_id == user.id,
                    UserRoleAssignment.role_id == role_obj.id,
                    UserRoleAssignment.code_location_name == code_location_name,
                )
            )
            existing = result.scalar_one_or_none()
            if existing:
                return existing

            assignment = UserRoleAssignment(
                user_id=user.id,
                role_id=role_obj.id,
                code_location_name=code_location_name,
            )
            session.add(assignment)
            await session.commit()
            await session.refresh(assignment)
            return assignment

    async def remove_role_from_location(
        self,
        username: str,
        role_name: str,
        code_location_name: str | None = None,
    ) -> bool:
        """Remove a role assignment from a user."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User).where(User.username == username)
            )
            user = result.scalar_one_or_none()
            if user is None:
                return False

            result = await session.execute(select(Role).where(Role.name == role_name))
            role_obj = result.scalar_one_or_none()
            if role_obj is None:
                return False

            result = await session.execute(
                select(UserRoleAssignment).where(
                    UserRoleAssignment.user_id == user.id,
                    UserRoleAssignment.role_id == role_obj.id,
                    UserRoleAssignment.code_location_name == code_location_name,
                )
            )
            assignment = result.scalar_one_or_none()
            if assignment is None:
                return False

            await session.delete(assignment)
            await session.commit()
            return True

    async def set_user_role_assignments(
        self,
        username: str,
        assignments: list[dict[str, str | None]],
    ) -> list[UserRoleAssignment]:
        """Replace all role assignments for a user.

        Args:
            username: The user to update.
            assignments: List of dicts with keys 'role_name' and 'code_location_name'
                         (None for global). Scopes must be unique — one role per scope.

        Raises:
            ValueError: If user not found, role not found, or duplicate scopes.
        """
        await self._ensure_ready()

        # Validate unique scopes
        seen_scopes: set[str | None] = set()
        for a in assignments:
            scope = a.get("code_location_name") or None
            if scope in seen_scopes:
                display = scope if scope else "(global)"
                raise ValueError(
                    f"Duplicate scope '{display}' — only one role per scope is allowed"
                )
            seen_scopes.add(scope)

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User).where(User.username == username)
            )
            user = result.scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")

            # Delete all existing assignments
            existing_result = await session.execute(
                select(UserRoleAssignment).where(UserRoleAssignment.user_id == user.id)
            )
            for existing in existing_result.scalars().all():
                await session.delete(existing)

            # Insert new assignments
            new_assignments: list[UserRoleAssignment] = []
            for a in assignments:
                role_name = a["role_name"]
                scope = a.get("code_location_name") or None

                role_result = await session.execute(
                    select(Role).where(Role.name == role_name)
                )
                role_obj = role_result.scalar_one_or_none()
                if role_obj is None:
                    raise ValueError(f"Role '{role_name}' not found")

                assignment = UserRoleAssignment(
                    user_id=user.id,
                    role_id=role_obj.id,
                    code_location_name=scope,
                )
                session.add(assignment)
                new_assignments.append(assignment)

            await session.commit()
            for a in new_assignments:
                await session.refresh(a)
            return new_assignments

    async def get_user_roles_for_location(
        self, username: str, location_name: str | None = None
    ) -> list[Role]:
        """Get all roles applicable to a user for a given location.

        Returns roles where ``code_location_name IS NULL`` (global)
        or matches the given location name.
        """
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User).where(User.username == username)
            )
            user = result.scalar_one_or_none()
            if user is None:
                return []

            stmt = (
                select(Role)
                .join(UserRoleAssignment, UserRoleAssignment.role_id == Role.id)
                .where(
                    UserRoleAssignment.user_id == user.id,
                    (
                        (UserRoleAssignment.code_location_name.is_(None))
                        | (UserRoleAssignment.code_location_name == location_name)
                    )
                    if location_name
                    else UserRoleAssignment.code_location_name.is_(None),
                )
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_user_role_assignments(
        self, username: str
    ) -> list[UserRoleAssignment]:
        """Get all role assignments for a user (for admin UI)."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User).where(User.username == username)
            )
            user = result.scalar_one_or_none()
            if user is None:
                return []

            from sqlalchemy.orm import selectinload

            result = await session.execute(
                select(UserRoleAssignment)
                .options(selectinload(UserRoleAssignment.role))
                .where(UserRoleAssignment.user_id == user.id)
                .order_by(UserRoleAssignment.code_location_name)
            )
            return list(result.scalars().all())

    # ── Role CRUD ────────────────────────────────────────────────

    async def create_role(
        self,
        name: str,
        permissions: dict[str, bool],
    ) -> Role:
        """Create a new custom role with the given permission map.

        Raises ``IntegrityError`` if a role with this name already exists.
        """
        await self._ensure_ready()

        role = Role(
            name=name,
            permissions=permissions,
            is_builtin=False,
        )
        async with get_session_factory()() as session:
            session.add(role)
            await session.commit()
            await session.refresh(role)
            return role

    async def update_role(
        self,
        name: str,
        *,
        permissions: dict[str, bool] | None = None,
    ) -> Role:
        """Update a custom role's permissions.

        Raises ``ValueError`` if the role is built-in or not found.
        """
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(select(Role).where(Role.name == name))
            role = result.scalar_one_or_none()
            if role is None:
                raise ValueError(f"Role '{name}' not found")
            if role.is_builtin:
                raise ValueError(f"Built-in role '{name}' cannot be modified")
            if permissions is not None:
                role.permissions = permissions
            await session.commit()
            await session.refresh(role)
            return role

    async def delete_role(self, name: str) -> None:
        """Delete a custom role.

        Users still assigned to the role have their role_id nulled out.
        Raises ``ValueError`` if the role is built-in.
        """
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(select(Role).where(Role.name == name))
            role = result.scalar_one_or_none()
            if role is None:
                raise ValueError(f"Role '{name}' not found")
            if role.is_builtin:
                raise ValueError(f"Built-in role '{name}' cannot be deleted")
            # Check if any role assignments reference this role
            from sqlalchemy import func as sa_func

            count_result = await session.execute(
                select(sa_func.count())
                .select_from(UserRoleAssignment)
                .where(UserRoleAssignment.role_id == role.id)
            )
            if count_result.scalar() > 0:
                raise ValueError(
                    f"Cannot delete role '{name}' — it is still assigned to users"
                )
            await session.delete(role)
            await session.commit()

    async def list_roles(self) -> list[Role]:
        """Return all roles (built-in and custom)."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(Role).order_by(Role.is_builtin.desc(), Role.name)
            )
            return list(result.scalars().all())

    async def get_role(self, name: str) -> Role | None:
        """Look up a role by name."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(select(Role).where(Role.name == name))
            return result.scalar_one_or_none()

    # ── Internal helpers ─────────────────────────────────────────

    @staticmethod
    def _get_global_role_from_assignments(user: User) -> Role | None:
        """Find the user's global role from their role assignments.

        Returns the Role object for the assignment with code_location_name=None,
        or None if no global assignment exists.
        """
        for a in user.role_assignments:
            if a.code_location_name is None and a.role is not None:
                return a.role
        return None

    def _to_auth_user(self, user: User) -> AuthUser:
        """Convert an ORM ``User`` to an ``AuthUser``.

        The user's global role is derived from their role assignments
        (the one with code_location_name=None). If no global assignment
        exists, falls back to the configured default role.
        """
        role = self._get_global_role_from_assignments(user)
        if role is None:
            role = self._default_role
            if role is None:
                logger.warning(
                    "No role for user '%s' and default role '%s' not found "
                    "in database. Using viewer permissions.",
                    user.username,
                    self._default_role_name,
                )
                return AuthUser(
                    username=user.username,
                    role="viewer",
                    custom_permissions=None,
                    email=user.email,
                    display_name=user.display_name,
                    oidc_sub=user.oidc_sub,
                    is_service_account=user.is_service_account,
                )

        if role.is_builtin:
            return AuthUser(
                username=user.username,
                role=role.name,
                custom_permissions=None,
                email=user.email,
                display_name=user.display_name,
                oidc_sub=user.oidc_sub,
                is_service_account=user.is_service_account,
            )
        else:
            raw = role.permissions
            if isinstance(raw, dict):
                perms = dict(raw)
            elif isinstance(raw, str):
                perms = json.loads(raw) if raw else {}
            else:
                perms = {}
            return AuthUser(
                username=user.username,
                role="custom",
                custom_permissions=perms,
                email=user.email,
                display_name=user.display_name,
                oidc_sub=user.oidc_sub,
                is_service_account=user.is_service_account,
            )

    async def _lookup_role(self, name: str, *, session: Any = None) -> Role:
        """Look up a role by name, raising ``ValueError`` if not found."""
        if session is not None:
            result = await session.execute(select(Role).where(Role.name == name))
        else:
            async with get_session_factory()() as s:
                result = await s.execute(select(Role).where(Role.name == name))
        role = result.scalar_one_or_none()
        if role is None:
            raise ValueError(f"Role '{name}' not found")
        return role

    async def _resolve_default_role(self) -> None:
        """Look up the default role by name and cache it.

        Queries the database directly to avoid recursing through
        ``get_role()`` (which itself calls ``_ensure_ready()``).
        """
        if self._default_role is not None:
            return
        async with get_session_factory()() as session:
            result = await session.execute(
                select(Role).where(Role.name == self._default_role_name)
            )
            self._default_role = result.scalar_one_or_none()

    # ── OIDC Provider CRUD ───────────────────────────────────────

    async def list_oidc_providers(
        self, *, enabled_only: bool = False
    ) -> list[OIDCProvider]:
        """Return OIDC providers ordered by display_order.

        If *enabled_only* is ``True``, filters to enabled providers.
        """
        from sqlalchemy.orm import selectinload

        await self._ensure_ready()

        async with get_session_factory()() as session:
            stmt = (
                select(OIDCProvider)
                .options(selectinload(OIDCProvider.users))
                .order_by(OIDCProvider.display_order, OIDCProvider.name)
            )
            if enabled_only:
                stmt = stmt.where(OIDCProvider.enabled.is_(True))
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_oidc_provider(self, name: str) -> OIDCProvider | None:
        """Look up an OIDC provider by name."""
        from sqlalchemy.orm import selectinload

        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(OIDCProvider)
                .options(selectinload(OIDCProvider.users))
                .where(OIDCProvider.name == name)
            )
            return result.scalar_one_or_none()

    async def get_oidc_provider_by_id(self, provider_id: int) -> OIDCProvider | None:
        """Look up an OIDC provider by ID."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(OIDCProvider).where(OIDCProvider.id == provider_id)
            )
            return result.scalar_one_or_none()

    async def create_oidc_provider(
        self,
        name: str,
        display_name: str,
        issuer_url: str,
        client_id: str,
        client_secret: str,
        *,
        scopes: str = "openid email profile",
        display_order: int = 0,
    ) -> OIDCProvider:
        """Create a new OIDC provider.

        Raises ``IntegrityError`` if a provider with this name already exists.
        """
        await self._ensure_ready()

        provider = OIDCProvider(
            name=name,
            display_name=display_name,
            issuer_url=issuer_url,
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes,
            enabled=True,
            display_order=display_order,
        )
        async with get_session_factory()() as session:
            session.add(provider)
            await session.commit()
            await session.refresh(provider)
            return provider

    async def update_oidc_provider(
        self,
        name: str,
        *,
        display_name: str | None = None,
        issuer_url: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        scopes: str | None = None,
        enabled: bool | None = None,
        display_order: int | None = None,
    ) -> OIDCProvider:
        """Update an OIDC provider.

        Raises ``ValueError`` if not found.
        """
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(OIDCProvider).where(OIDCProvider.name == name)
            )
            provider = result.scalar_one_or_none()
            if provider is None:
                raise ValueError(f"OIDC provider '{name}' not found")

            if display_name is not None:
                provider.display_name = display_name
            if issuer_url is not None:
                provider.issuer_url = issuer_url
            if client_id is not None:
                provider.client_id = client_id
            if client_secret is not None:
                provider.client_secret = client_secret
            if scopes is not None:
                provider.scopes = scopes
            if enabled is not None:
                provider.enabled = enabled
            if display_order is not None:
                provider.display_order = display_order

            await session.commit()
            await session.refresh(provider)
            return provider

    async def delete_oidc_provider(self, name: str) -> None:
        """Delete an OIDC provider.

        Nulls out ``oidc_provider_id`` on any linked users.
        Raises ``ValueError`` if not found.
        """
        from sqlalchemy.orm import selectinload

        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(OIDCProvider)
                .options(selectinload(OIDCProvider.users))
                .where(OIDCProvider.name == name)
            )
            provider = result.unique().scalar_one_or_none()
            if provider is None:
                raise ValueError(f"OIDC provider '{name}' not found")
            # Null out oidc_provider_id on any linked users
            for user in provider.users:
                user.oidc_provider_id = None
                user.oidc_sub = None
            await session.delete(provider)
            await session.commit()

    # ── OIDC User Lookup & Creation ──────────────────────────────

    async def get_user_by_oidc(
        self, provider_id: int, oidc_sub: str
    ) -> AuthUser | None:
        """Look up a user by OIDC provider ID and subject identifier."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(
                    User.oidc_provider_id == provider_id,
                    User.oidc_sub == oidc_sub,
                )
            )
            user = result.scalar_one_or_none()
            if user and user.is_active:
                return self._to_auth_user(user)
            return None

    async def get_user_by_email(self, email: str) -> AuthUser | None:
        """Look up a user by email address."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.email == email)
            )
            user = result.scalar_one_or_none()
            if user and user.is_active:
                return self._to_auth_user(user)
            return None

    async def create_oidc_user(
        self,
        username: str,
        provider_id: int,
        oidc_sub: str,
        *,
        email: str | None = None,
        display_name: str | None = None,
        role: str = "viewer",
    ) -> AuthUser:
        """Create a new user linked to an OIDC provider.

        ``password_hash`` is set to empty string — OIDC users authenticate
        only via OIDC.

        Raises ``ValueError`` if the role is unknown.
        """
        await self._ensure_ready()

        role_obj = await self._lookup_role(role)

        user = User(
            username=username,
            password_hash="",
            email=email,
            display_name=display_name,
            is_active=True,
            oidc_provider_id=provider_id,
            oidc_sub=oidc_sub,
        )

        async with get_session_factory()() as session:
            session.add(user)
            await session.flush()
            # Create global role assignment (default viewer)
            session.add(
                UserRoleAssignment(
                    user_id=user.id,
                    role_id=role_obj.id,
                    code_location_name=None,
                )
            )
            await session.commit()

            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.id == user.id)
            )
            user = result.scalar_one()
            return self._to_auth_user(user)

    async def link_oidc_to_user(
        self, username: str, provider_id: int, oidc_sub: str
    ) -> AuthUser:
        """Link an OIDC identity to an existing user.

        Raises ``ValueError`` if the user is not found.
        """
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.username == username)
            )
            user = result.scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")
            user.oidc_provider_id = provider_id
            user.oidc_sub = oidc_sub
            await session.commit()

            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.id == user.id)
            )
            user = result.scalar_one()
            return self._to_auth_user(user)

    # ── Service Account Listing ─────────────────────────────────

    async def list_service_accounts(self) -> list[AuthUser]:
        """Return all active service accounts."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User)
                .options(_USER_ROLE_ASSIGNMENTS_LOAD)
                .where(User.is_active.is_(True), User.is_service_account.is_(True))
                .order_by(User.username)
            )
            users = result.scalars().all()
            return [self._to_auth_user(u) for u in users]

    # ── Code Location Management ────────────────────────────────

    async def list_code_locations(self, status: str | None = None) -> list[Any]:
        """List code location definitions."""
        await self._ensure_ready()

        from dagster_webserver.database.models import CodeLocationDefinition

        async with get_session_factory()() as session:
            stmt = select(CodeLocationDefinition).order_by(
                CodeLocationDefinition.location_name
            )
            if status:
                stmt = stmt.where(CodeLocationDefinition.status == status)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_code_location(self, location_name: str) -> Any | None:
        """Get a single code location by name."""
        await self._ensure_ready()

        from dagster_webserver.database.models import CodeLocationDefinition

        async with get_session_factory()() as session:
            result = await session.execute(
                select(CodeLocationDefinition).where(
                    CodeLocationDefinition.location_name == location_name
                )
            )
            return result.scalar_one_or_none()

    async def create_code_location(
        self,
        location_name: str,
        deploy_metadata: dict[str, Any],
        labels: dict[str, str] | None = None,
    ) -> Any:
        """Create a new code location definition."""
        await self._ensure_ready()

        from dagster_webserver.database.models import CodeLocationDefinition

        async with get_session_factory()() as session:
            existing = await session.execute(
                select(CodeLocationDefinition).where(
                    CodeLocationDefinition.location_name == location_name
                )
            )
            if existing.scalar_one_or_none():
                raise ValueError(f"Code location '{location_name}' already exists")

            loc = CodeLocationDefinition(
                location_name=location_name,
                deploy_metadata=deploy_metadata,
                labels=labels,
                status="active",
            )
            session.add(loc)
            await session.commit()
            await session.refresh(loc)
            return loc

    async def update_code_location(
        self,
        location_name: str,
        deploy_metadata: dict[str, Any] | None = None,
        labels: dict[str, str] | None = None,
        status: str | None = None,
    ) -> Any:
        """Update an existing code location."""
        await self._ensure_ready()

        from dagster_webserver.database.models import CodeLocationDefinition

        async with get_session_factory()() as session:
            result = await session.execute(
                select(CodeLocationDefinition).where(
                    CodeLocationDefinition.location_name == location_name
                )
            )
            loc = result.scalar_one_or_none()
            if loc is None:
                raise ValueError(f"Code location '{location_name}' not found")

            if deploy_metadata is not None:
                loc.deploy_metadata = deploy_metadata
            if labels is not None:
                loc.labels = labels
            if status is not None:
                loc.status = status
            await session.commit()
            await session.refresh(loc)
            return loc

    async def delete_code_location(self, location_name: str) -> bool:
        """Hard-delete a code location definition.

        Agents reconcile against the workspace-entries API (which only
        returns ``status == 'active'`` rows), so removing the row causes
        them to tear down the location's server on their next sync. Use
        the ``status`` column for lifecycle signalling (pause/resume).
        """
        await self._ensure_ready()

        from dagster_webserver.database.models import CodeLocationDefinition

        async with get_session_factory()() as session:
            result = await session.execute(
                select(CodeLocationDefinition).where(
                    CodeLocationDefinition.location_name == location_name
                )
            )
            loc = result.scalar_one_or_none()
            if loc is None:
                return False
            await session.delete(loc)
            await session.commit()
            return True
