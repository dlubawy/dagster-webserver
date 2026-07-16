# Database-Backed `UserBackend` — Research & Design

> **Date:** 2026-07-15
> **Scope:** Research for a new `UserBackend` implementation that stores users,
> roles, and custom permissions in a relational database via SQLAlchemy.

______________________________________________________________________

## 1. Problem Statement

The existing `FileUserBackend` and `InMemoryUserBackend` require either:

- Editing a YAML/JSON file and restarting the webserver (file-based), or
- Hard-coding users at Python runtime (in-memory).

Neither supports **runtime user management** — administrators cannot create,
update, or delete users through the UI without touching config files or
restarting the process.

A database-backed `UserBackend` solves this by:

- Persisting users, password hashes, and role assignments in a relational
  database (SQLite for dev, PostgreSQL for production).
- Allowing user management via CLI commands backed by database object methods.
- Supporting custom roles with granular permission maps stored as JSON.

______________________________________________________________________

## 2. Reference: Existing `UserBackend` Contract

The abstract `UserBackend` ABC (`dagster_webserver/auth/users.py`) defines two
async methods:

```python
class UserBackend(ABC):
    @abstractmethod
    async def authenticate(self, username: str, password: str) -> AuthUser | None:
        """Verify credentials and return the user, or None if invalid."""

    @abstractmethod
    async def get_user(self, username: str) -> AuthUser | None:
        """Look up user by username (e.g. from session)."""
```

A database-backed implementation must satisfy this contract **and** provide
additional object methods (`create_user`, `update_user`, `delete_user`,
`list_users`) used by CLI commands such as `init-admin`.

### `AuthUser` Dataclass

```python
@dataclass(frozen=True)
class AuthUser:
    username: str
    role: str                      # "viewer" | "editor" | "admin" | "custom"
    custom_permissions: dict[str, bool] | None = None
    email: str | None = None
    display_name: str | None = None
```

### Role & Permission Model (`dagster_webserver/auth/roles.py`)

Five built-in roles map to `Permissions` enum values:

| Role | Permissions |
| ---------------- | ------------------------------------------------- |
| `catalog_viewer` | None (all disabled) |
| `viewer` | None (all disabled) |
| `launcher` | Launch/re-execute/terminate pipelines + backfills |
| `editor` | All permissions enabled |
| `admin` | All permissions enabled |

Custom roles use `get_custom_permissions(perm_map: dict[str, bool])` to build
a `PermissionResult` map from an arbitrary permission dictionary.

______________________________________________________________________

## 3. Database Schema Design

### 3.1 Naming Convention

**Singular ORM model → plural table name.** Each ORM class uses a singular
name (e.g. `User`, `Role`) but maps to a **plural** SQL table name (e.g.
`users`, `roles`). This matches the convention used by Flask-SQLAlchemy,
Django, and most web frameworks.

| ORM class | `__tablename__` |
| --------- | --------------- |
| `Role` | `roles` |
| `User` | `users` |

### 3.2 Tables

#### `roles`

Custom roles are **first-class entities** stored in their own table. Built-in
roles (`catalog_viewer`, `viewer`, `launcher`, `editor`, `admin`) are seeded
as rows during the initial migration. Custom roles are additional rows with
their own permission maps. This avoids duplicating permission JSON on every
user assignment — each unique role is stored once.

| Column | Type | Constraints |
| ------------- | ---------------- | ----------------------------------- |
| `id` | INTEGER / BIGINT | PK, AUTOINCREMENT |
| `name` | VARCHAR(64) | UNIQUE, NOT NULL |
| `permissions` | JSON | NOT NULL (map of permission → bool) |
| `is_builtin` | BOOLEAN | DEFAULT FALSE, NOT NULL |
| `created_at` | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP |
| `updated_at` | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP |

Built-in roles are seeded with `is_builtin = TRUE` and their permission maps
pre-populated from `dagster_webserver/auth/roles.py`.

#### `users`

| Column | Type | Constraints |
| --------------- | ---------------- | ----------------------------- |
| `id` | INTEGER / BIGINT | PK, AUTOINCREMENT |
| `username` | VARCHAR(128) | UNIQUE, NOT NULL |
| `password_hash` | VARCHAR(1024) | NOT NULL (argon2 PHC string) |
| `role_id` | INTEGER / BIGINT | FK → `roles.id`, **NULLABLE** |
| `email` | VARCHAR(256) | NULL |
| `display_name` | VARCHAR(256) | NULL |
| `is_active` | BOOLEAN | DEFAULT TRUE |
| `created_at` | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP |
| `updated_at` | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP |

Users reference their role via a foreign key. This means changing a role's
permissions automatically affects all users assigned to it.

### 3.3 SQLAlchemy ORM Models

Models live in `dagster_webserver/database/models.py` and are imported by
the auth module where needed. This keeps database infrastructure (models,
engine, session factories, migrations) cleanly separated from auth logic
(users, roles, providers, middleware).

```python
# dagster_webserver/database/models.py
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, JSON, ForeignKey, func,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)


class Base(DeclarativeBase):
    """Base for all database tables.  Kept separate from Dagster's own
    metadata to avoid collisions with Dagster's run/event/schedule storage."""


class Role(Base):
    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    permissions: Mapped[dict[str, bool]] = mapped_column(JSON, nullable=False)
    is_builtin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    users: Mapped[list["User"]] = relationship("User", back_populates="role")


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(1024), nullable=False)
    role_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("roles.id"), nullable=True
    )
    email: Mapped[str | None] = mapped_column(String(256), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    role: Mapped["Role"] = relationship("Role", back_populates="users")
```

### 3.4 Role Resolution Flow

When `authenticate()` or `get_user()` is called, the user's role is resolved
through the foreign key relationship with a configurable fallback:

1. Load `User` row by username.
1. If `user.role_id` is **not null**, follow the FK to the `Role` row.
1. If `user.role_id` is **null**, use the **default role** configured on the
   `DatabaseUserBackend` (defaults to `"viewer"`). This is resolved by looking
   up the built-in role row by name.
1. Read `role.permissions` (JSON → `dict[str, bool]`).
1. Build `AuthUser` with `role=role.name` and `custom_permissions=role.permissions`
   (if `role.is_builtin` is `False`, the `role` field is set to `"custom"` to
   signal the permission resolver to use the explicit map).

Making `role_id` nullable allows users to be created without an explicit role
assignment. The default role acts as a system-wide fallback, so administrators
control the baseline permission level for unassigned users. This is configurable
at the backend level (e.g. via `--default-role` CLI flag) and will be exposed
as a runtime option later.

### 3.5 Module Layout

Database code lives in its own package, independent of auth logic:

```
dagster_webserver/
├── auth/
│   ├── __init__.py
│   ├── users.py           # AuthUser, UserBackend ABC, InMemoryUserBackend, FileUserBackend
│   ├── provider.py        # SessionAuthProvider, ApiKeyAuthProvider
│   ├── middleware.py      # AuthMiddleware
│   ├── routes.py          # /login, /logout, /api/me
│   ├── roles.py           # Role enum, ROLE_PERMISSIONS, get_role_permissions()
│   └── context.py         # AuthenticatedWorkspaceRequestContext
└── database/
    ├── __init__.py        # exports: Base, engine, AsyncSession
    ├── models.py          # Base, Role, User ORM models
    ├── engine.py          # create_engine, async_sessionmaker helpers
    └── alembic/
        ├── env.py
        ├── script.py.mako
        └── versions/
```

The auth module imports models from `dagster_webserver.database.models`:

```python
# dagster_webserver/auth/db_backend.py
from dagster_webserver.database.models import User, Role
from dagster_webserver.database import AsyncSession
```

This separation means:

- Database infrastructure (engine, session, migrations) can evolve independently.
- Auth logic (providers, middleware, routes) has no direct SQLAlchemy coupling.
- The `UserBackend` ABC remains database-agnostic — any backend implementation
  can swap in a different persistence layer without touching auth code.

______________________________________________________________________

## 4. Technology Choices

### 4.1 SQLAlchemy 2.0 (Async)

**Already available** as a transitive dependency of Dagster. Version 2.0.51
is installed in the dev environment.

- **Why async:** The `UserBackend.authenticate()` and `get_user()` methods are
  `async def`. Using SQLAlchemy's async engine (`create_async_engine`)
  avoids blocking the event loop during I/O.
- **Dialects supported:**
  - `sqlite+aiosqlite:///` — for local/dev (requires `aiosqlite`)
  - `postgresql+asyncpg:///` — for production (requires `asyncpg`)

### 4.2 Alembic for Migrations

**Already available** as a transitive dependency of Dagster. Version 1.18.5
is installed.

- Auth tables get their **own** Alembic configuration (separate `env.py` and
  `versions/` directory) so migrations are independent of Dagster's schema.
- The `env.py` imports `Base.metadata` from `dagster_webserver.database` to
  ensure Alembic sees the correct model metadata.
- Migration directory: `dagster_webserver/database/alembic/`
- Initial migration creates the `roles` and `users` tables and seeds the
  five built-in roles.

### 4.3 Session Management

Two approaches are viable:

| Approach | Pros | Cons |
| ------------------------------------------ | ---------------------------------------- | ------------------------------ |
| **AsyncSession per-request** (recommended) | Clean lifecycle, no thread-safety issues | Slightly more boilerplate |
| **SyncSession in executor** | Reuses sync SQLAlchemy | Blocks event loop; unnecessary |

We recommend `AsyncSession` with a scoped session factory, defined in
`dagster_webserver/database/engine.py` and exported from the package:

```python
# dagster_webserver/database/engine.py
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

engine = create_async_engine("sqlite+aiosqlite:///auth.db")
AsyncSession = async_sessionmaker(engine, expire_on_commit=False)
```

```python
# dagster_webserver/database/__init__.py
from .engine import engine, AsyncSession
from .models import Base, Role, User

__all__ = ["Base", "Role", "User", "engine", "AsyncSession"]
```

______________________________________________________________________

## 5. Implementation Design

### 5.1 Class Signature

```python
class DatabaseUserBackend(UserBackend):
    def __init__(
        self,
        database_url: str,
        *,
        create_tables: bool = True,
    ) -> None:
        ...
```

**Parameters:**

- `database_url` — SQLAlchemy connection string (e.g.
  `sqlite+aiosqlite:///auth.db` or `postgresql+asyncpg://user:pass@host/db`).
- `create_tables` — If `True`, run Alembic upgrade to `head` on startup.
  Defaults to `True` for convenience; set to `False` in production where
  migrations are managed externally.

### 5.2 Core Methods

The `DatabaseUserBackend` lives in `dagster_webserver/auth/db_backend.py` and
imports models + session from the database package:

```python
from dagster_webserver.database import AsyncSession
from dagster_webserver.database.models import User, Role

async def authenticate(self, username: str, password: str) -> AuthUser | None:
    """Look up user by username, verify password hash, return AuthUser."""
    async with AsyncSession() as session:
        user = await session.execute(
            select(User).options(joinedload(User.role)).where(User.username == username)
        )
        user = user.scalar_one_or_none()
        if not user or not user.is_active:
            return None
        if _verify_password(password, user.password_hash):
            return self._to_auth_user(user)
        return None

async def get_user(self, username: str) -> AuthUser | None:
    """Look up user by username (for session restoration)."""
    async with AsyncSession() as session:
        user = await session.execute(
            select(User).options(joinedload(User.role)).where(User.username == username)
        )
        user = user.scalar_one_or_none()
        if user and user.is_active:
            return self._to_auth_user(user)
        return None
```

The `joinedload(User.role)` eager-loads the related `Role` in the same query
to avoid the N+1 problem.

### 5.3 Additional Object Methods

These are **not** part of the `UserBackend` ABC but are needed by CLI
commands (e.g. `init-admin`) and future management tooling:

```python
async def create_user(
    self,
    username: str,
    password: str,
    role: str = "viewer",
    email: str | None = None,
    display_name: str | None = None,
) -> AuthUser:
    """Create a new user.

    `role` is a role name string (e.g. "viewer", "editor", or a custom role
    name).  The method looks up the corresponding `Role` row and assigns the
    FK.  Raises IntegrityError if username exists or role name is unknown.
    """

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
    """Update one or more fields.  Returns updated AuthUser."""

async def delete_user(self, username: str) -> None:
    """Soft-delete (set is_active=False) or hard-delete a user."""

async def list_users(self) -> list[AuthUser]:
    """Return all active users."""

# -- Role management methods --

async def create_role(
    self,
    name: str,
    permissions: dict[str, bool],
) -> Role:
    """Create a new custom role with the given permission map.

    Raises IntegrityError if a role with this name already exists.
    """

async def update_role(
    self,
    name: str,
    *,
    permissions: dict[str, bool] | None = None,
) -> Role:
    """Update a custom role's permissions.  Built-in roles cannot be modified."""

async def delete_role(self, name: str) -> None:
    """Delete a custom role.  Built-in roles cannot be deleted.

    Raises an error if any users are still assigned to this role.
    """

async def list_roles(self) -> list[Role]:
    """Return all roles (built-in and custom)."""

async def get_role(self, name: str) -> Role | None:
    """Look up a role by name."""
```

### 5.4 `_to_auth_user` Helper

```python
def _to_auth_user(self, user: User) -> AuthUser:
    # If user has no explicit role, fall back to the configured default role
    role = user.role
    if role is None:
        role = self._default_role  # Role ORM object looked up at init time
    return AuthUser(
        username=user.username,
        role="custom" if not role.is_builtin else role.name,
        custom_permissions=role.permissions if not role.is_builtin else None,
        email=user.email,
        display_name=user.display_name,
    )
```

For built-in roles, the `role` field carries the role name (e.g. `"viewer"`)
and `custom_permissions` is `None` — the permission resolver uses the
built-in role map from `roles.py`. For custom roles, `role` is set to
`"custom"` and `custom_permissions` carries the explicit permission map
stored in the `roles` table. When `user.role_id` is `None`, the default
role is used as a fallback.

______________________________________________________________________

## 6. Integration Points

### 6.1 CLI Flag

Add to `dagster_webserver/cli.py`:

```python
@click.option(
    "--auth-database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    help="SQLAlchemy URL for the auth database "
         "(e.g. sqlite+aiosqlite:///auth.db or "
         "postgresql+asyncpg://user:pass@host/db).",
)
```

### 6.2 Provider Wiring

In `dagster_webserver/auth/provider.py`, extend `SessionAuthProvider` to
accept a `DatabaseUserBackend`:

```python
provider = SessionAuthProvider(
    user_backend=DatabaseUserBackend(database_url),
    session_secret=secret,
    default_role="viewer",
)
```

### 6.3 CLI Commands

A convenience CLI command to bootstrap the first admin user:

```bash
dagster-webserver auth init-admin --username admin --password changeme
```

This looks up the built-in `admin` role by name, hashes the password with
argon2id, and inserts the user row with the correct `role_id` FK.

Additional CLI commands for role management:

```bash
# Create a custom role
dagster-webserver auth create-role --name analyst --permissions '{"read_workspace": true, "launch_pipeline_execution": false}'

# List all roles
dagster-webserver auth list-roles

# Update a custom role's permissions
dagster-webserver auth update-role --name analyst --permissions '{...}'

# Delete a custom role
dagster-webserver auth delete-role --name analyst
```

______________________________________________________________________

## 7. Dependencies

| Package | Purpose | Group |
| ----------- | ----------------------- | --------- |
| `aiosqlite` | Async SQLite driver | `auth` |
| `asyncpg` | Async PostgreSQL driver | `auth-db` |

**Rationale for splitting:**

- `aiosqlite` is lightweight and works for all local/dev scenarios. Add to
  the existing `auth` optional dependency group.
- `asyncpg` is the production-grade PostgreSQL driver with C extensions.
  Place in a new `auth-db` optional dependency group so it is only
  installed when needed.

Updated `pyproject.toml`:

```toml
[project.optional-dependencies]
auth = ["pyyaml", "argon2-cffi", "aiosqlite"]
auth-db = ["asyncpg"]
```

______________________________________________________________________

## 8. Security Considerations

### 8.1 Password Hashing

- Continue using **argon2-cffi** (argon2id) for all password hashing.
- The `DatabaseUserBackend.create_user()` method hashes passwords before
  storing them.
- Existing `FileUserBackend` migration: provide a CLI command to import
  users from a YAML/JSON file into the database.

### 8.2 SQL Injection

- SQLAlchemy ORM parameterized queries prevent SQL injection.
- Raw SQL (if any) must use bound parameters exclusively.

### 8.3 Session Secret

- The session signing secret (`--session-secret`) is separate from the
  database and remains stored in environment variables or secrets managers.

### 8.4 Rate Limiting

- The database backend should support integration with rate-limiting
  middleware (e.g. `slowapi`) to prevent brute-force attacks on the
  `/login` endpoint.
- This is orthogonal to the backend itself but should be documented.

### 8.5 Audit Logging

- Add `login_attempts` and `last_login_at` columns to track authentication
  activity.
- Log all user creation, update, and deletion operations to the
  application log for audit trails.

______________________________________________________________________

## 9. Migration Strategy

### 9.1 From FileUserBackend to DatabaseUserBackend

Provide a migration command:

```bash
dagster-webserver auth migrate-users --from-file users.yaml --to-db sqlite+aiosqlite:///auth.db
```

This reads users from the YAML/JSON file, hashes passwords with argon2id
(if not already hashed), and inserts them into the database.

### 9.2 Alembic Migrations

Migrations live under `dagster_webserver/database/alembic/versions/`.

Initial migration file (`versions/001_create_roles_and_users.py`):

```python
"""Create roles and users tables, seed built-in roles.

Revision ID: 001
Revises:
Create Date: 2026-07-15
"""

from alembic import op
import sqlalchemy as sa

def upgrade() -> None:
    op.create_table(
        "roles",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(64), unique=True, nullable=False),
        sa.Column("permissions", sa.JSON, nullable=False),
        sa.Column("is_builtin", sa.Boolean, default=False, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_roles_name", "roles", ["name"], unique=True)

    # Seed built-in roles from dagster_webserver.auth.roles
    conn = op.get_bind()
    from dagster_webserver.auth.roles import Role, ROLE_PERMISSIONS
    for role in Role:
        perm_map = {perm.name: enabled for perm, enabled in ROLE_PERMISSIONS[role].items()}
        conn.execute(
            sa.insert(sa.table(
                "roles",
                sa.column("name", sa.String),
                sa.column("permissions", sa.JSON),
                sa.column("is_builtin", sa.Boolean),
            )).values(
                name=role.value,
                permissions=perm_map,
                is_builtin=True,
            )
        )

    op.create_table(
        "users",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("username", sa.String(128), unique=True, nullable=False),
        sa.Column("password_hash", sa.String(1024), nullable=False),
        sa.Column("role_id", sa.Integer, sa.ForeignKey("roles.id"), nullable=True),
        sa.Column("email", sa.String(256), nullable=True),
        sa.Column("display_name", sa.String(256), nullable=True),
        sa.Column("is_active", sa.Boolean, default=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_users_username", "users", ["username"], unique=True)
    op.create_index("ix_users_role_id", "users", ["role_id"])

def downgrade() -> None:
    op.drop_index("ix_users_role_id")
    op.drop_index("ix_users_username")
    op.drop_table("users")
    op.drop_index("ix_roles_name")
    op.drop_table("roles")
```

______________________________________________________________________

## 10. Testing Strategy

### 10.1 Unit Tests

- `DatabaseUserBackend.create_user()` — creates user, resolves role by name,
  hashes password.
- `DatabaseUserBackend.authenticate()` — valid/invalid credentials, joined
  role lookup.
- `DatabaseUserBackend.get_user()` — active/inactive users.
- `DatabaseUserBackend.update_user()` — partial updates, role reassignment.
- `DatabaseUserBackend.delete_user()` — soft vs hard delete.
- `DatabaseUserBackend.list_users()` — active users.
- `DatabaseUserBackend.create_role()` — creates custom role with permissions.
- `DatabaseUserBackend.update_role()` — updates custom role permissions.
- `DatabaseUserBackend.delete_role()` — rejects built-in roles, checks for
  assigned users.
- `DatabaseUserBackend.list_roles()` — returns all roles.
- `_to_auth_user()` — resolves built-in vs custom role correctly.

### 10.2 Integration Tests

- Full login flow with `DatabaseUserBackend` (create → authenticate →
  session → `/api/me`).
- Alembic migration from empty to `head` (including built-in role seeding).
- Migration from `FileUserBackend` YAML to database (role resolution).
- Custom role assignment end-to-end (create role → assign user → verify
  permissions via GraphQL).

### 10.3 Test Database

- Use `sqlite+aiosqlite:///:memory:` for tests (no disk I/O, fast).
- Create tables before each test, drop after each test.

______________________________________________________________________

## 11. Design Decisions

1. **Multi-tenancy** — Not supported. The models are single-tenant and
   shared across all workspace contexts. A future extension may serve
   multiple workspace contexts at different endpoints (similar to
   Dagster+), but the `users` and `roles` tables remain shared. No
   `tenant_id` column is needed.

1. **One role per user** — Each user is assigned exactly one role that
   governs all their permissions. This will eventually expand to support
   team-based role assignment or scoped roles/permissions by code location,
   but the current design keeps it simple: a single `role_id` FK on the
   `users` table.

1. **Password reset** — Not implemented. Users must be managed through CLI
   commands or future admin tooling. No email infrastructure or token-based
   reset flow is designed at this time.

1. **OIDC identity resolution** — The `UserBackend` ABC is already designed
   to support separate implementations for different identity providers.
   An OIDC-based backend can be added as a new `UserBackend` subclass
   without modifying existing auth logic. The `DatabaseUserBackend` does
   not block this path — it coexists alongside other backends via the
   same provider/middleware wiring. Implementation details for OIDC are
   out of scope for this research.

______________________________________________________________________

## 12. Summary

| Aspect | Decision |
| ------------------- | -------------------------------------------------------- |
| ORM | SQLAlchemy 2.0 async (`AsyncSession`) |
| Migrations | Alembic (separate from Dagster's own migrations) |
| Default dialect | SQLite via `aiosqlite` |
| Production dialects | PostgreSQL (`asyncpg`) |
| Password hashing | argon2-cffi (argon2id) — already in place |
| Schema | `roles` + `users` tables, FK relationship, role seeding |
| MetaData | Separate `Base` in `dagster_webserver.database` |
| User management | Object methods on `DatabaseUserBackend`, invoked via CLI |
| Migration from file | CLI command to import YAML/JSON into database |
