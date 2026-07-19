"""Database-backed ``UserBackend`` implementation.

Stores users and custom roles in a relational database via SQLAlchemy.
Imports models and session from ``dagster_webserver.database`` to keep
database infrastructure cleanly separated from auth logic.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import joinedload

from dagster_webserver.auth.users import (
    AuthUser,
    UserBackend,
    _hash_password,
    _verify_password,
)
from dagster_webserver.database import (
    Role,
    User,
    get_engine,
    get_session_factory,
    init_engine,
)
from dagster_webserver.database.models import Base

logger = logging.getLogger("dagster-webserver.auth")


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
                .options(joinedload(User.role))
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
                .options(joinedload(User.role))
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
        """Create a new user.

        ``role`` is a role name string (e.g. ``"viewer"``, ``"admin"``, or a
        custom role name).  Pass ``None`` to leave ``role_id`` unset — the
        default role will be used at resolution time.

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
            role_id=role_obj.id if role_obj else None,
            email=email,
            display_name=display_name,
            is_active=True,
        )

        async with get_session_factory()() as session:
            session.add(user)
            await session.commit()
            await session.refresh(user)
            if user.role_id is not None:
                result = await session.execute(
                    select(Role).where(Role.id == user.role_id)
                )
                user.role = result.scalar_one_or_none()
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
        """Update one or more fields.  Returns updated ``AuthUser``."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(User)
                .options(joinedload(User.role))
                .where(User.username == username)
            )
            user = result.scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")

            if password is not None:
                user.password_hash = _hash_password(password)

            if role is not None:
                role_obj = await self._lookup_role(role, session=session)
                user.role_id = role_obj.id
                await session.refresh(user, ["role"])

            if email is not None:
                user.email = email
            if display_name is not None:
                user.display_name = display_name
            if is_active is not None:
                user.is_active = is_active

            await session.commit()
            await session.refresh(user)
            if user.role_id is not None:
                r = await session.execute(select(Role).where(Role.id == user.role_id))
                user.role = r.scalar_one_or_none()
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
                .options(joinedload(User.role))
                .where(User.is_active.is_(True))
                .order_by(User.username)
            )
            users = result.scalars().all()
            return [self._to_auth_user(u) for u in users]

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
            result = await session.execute(
                select(Role).options(joinedload(Role.users)).where(Role.name == name)
            )
            role = result.unique().scalar_one_or_none()
            if role is None:
                raise ValueError(f"Role '{name}' not found")
            if role.is_builtin:
                raise ValueError(f"Built-in role '{name}' cannot be deleted")
            # Null out role_id on any users still assigned to this role
            for user in role.users:
                user.role_id = None
            await session.delete(role)
            await session.commit()

    async def list_roles(self) -> list[Role]:
        """Return all roles (built-in and custom)."""
        from sqlalchemy.orm import selectinload

        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(
                select(Role)
                .options(selectinload(Role.users))
                .order_by(Role.is_builtin.desc(), Role.name)
            )
            return list(result.scalars().all())

    async def get_role(self, name: str) -> Role | None:
        """Look up a role by name."""
        await self._ensure_ready()

        async with get_session_factory()() as session:
            result = await session.execute(select(Role).where(Role.name == name))
            return result.scalar_one_or_none()

    # ── Internal helpers ─────────────────────────────────────────

    def _to_auth_user(self, user: User) -> AuthUser:
        """Convert an ORM ``User`` to an ``AuthUser``.

        If ``user.role`` is ``None``, falls back to the configured default
        role.
        """
        role = user.role
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
                )

        if role.is_builtin:
            return AuthUser(
                username=user.username,
                role=role.name,
                custom_permissions=None,
                email=user.email,
                display_name=user.display_name,
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
