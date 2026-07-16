# Implementation Plan: Database-Backed `UserBackend`

> **Research reference:** `./.agent/research/06-database-userbackend-research.md`
>
> **Goal:** Add a `DatabaseUserBackend` that stores users and custom roles in a
> relational database (SQLite for dev, PostgreSQL for production), with database
> infrastructure isolated in `dagster_webserver.database`.

______________________________________________________________________

## Phase 1 — `dagster_webserver.database` Package

**Objective:** Create the database package with models, engine, and Alembic
scaffolding. No auth code touches this package.

### Task 1.1 — Package skeleton

- [ ] Create `dagster_webserver/database/__init__.py`
  - Exports: `Base`, `Role`, `User`, `engine`, `AsyncSession`
- [ ] Create `dagster_webserver/database/models.py`
  - `Base(DeclarativeBase)` — separate `MetaData` from Dagster's storage
  - `Role(Base)` — `__tablename__ = "roles"`, columns: `id`, `name` (unique),
    `permissions` (JSON), `is_builtin` (bool), `created_at`, `updated_at`,
    relationship `users` → `User`
  - `User(Base)` — `__tablename__ = "users"`, columns: `id`, `username` (unique),
    `password_hash`, `role_id` (FK → `roles.id`, **nullable**), `email`, `display_name`,
    `is_active`, `created_at`, `updated_at`, relationship `role` → `Role`
- [ ] Create `dagster_webserver/database/engine.py`
  - `create_async_engine(database_url)` helper
  - `async_sessionmaker` factory
  - Module-level `engine` and `AsyncSession` created from a passed URL
  - `init_engine(database_url: str)` function that creates/replaces engine + session

### Task 1.2 — Alembic scaffolding

- [ ] Create `dagster_webserver/database/alembic/` directory
- [ ] Create `dagster_webserver/database/alembic/env.py`
  - Imports `Base.metadata` from `dagster_webserver.database.models`
  - Configures `target_metadata = Base.metadata`
  - Supports both online (connection) and offline modes
- [ ] Create `dagster_webserver/database/alembic/script.py.mako`
  - Use Alembic's default template (no custom Dagster formatting needed —
    auth migrations are independent and keep standard conventions)
- [ ] Create `dagster_webserver/database/alembic/alembic.ini`
  - `script_location` points to the alembic directory
  - `sqlalchemy.url` set to placeholder (overridden at runtime)
- [ ] Create initial migration `dagster_webserver/database/alembic/versions/001_create_roles_and_users.py`
  - Creates `roles` table
  - Seeds five built-in roles from `dagster_webserver.auth.roles.ROLE_PERMISSIONS`
  - Creates `users` table with FK to `roles.id`
  - Creates indexes: `ix_roles_name`, `ix_users_username`, `ix_users_role_id`
  - Downgrade reverses all operations

### Task 1.3 — Dependencies

- [ ] Add `aiosqlite` to `[project.optional-dependencies] auth` in `pyproject.toml`
- [ ] Add `auth-db = ["asyncpg"]` optional dependency group in `pyproject.toml`
- [ ] Run `uv lock` to update lockfile

### Task 1.4 — Tests

- [ ] Create `dagster_webserver_tests/test_database.py`
- [ ] Test: `Base.metadata` contains `roles` and `users` tables
- [ ] Test: `Role` model has correct columns and relationships
- [ ] Test: `User` model has correct columns and relationships
- [ ] Test: `init_engine()` creates engine and session from URL
- [ ] Test: Alembic migration runs from empty to head (creates tables, seeds roles)
- [ ] Test: Alembic downgrade removes tables

______________________________________________________________________

## Phase 2 — `DatabaseUserBackend` Implementation

**Objective:** Implement `DatabaseUserBackend` in `dagster_webserver/auth/db_backend.py`
that satisfies the `UserBackend` ABC and provides CRUD + role management methods.

### Task 2.1 — `dagster_webserver/auth/db_backend.py`

- [ ] Create `DatabaseUserBackend(UserBackend)` class
- [ ] `__init__(self, database_url: str, *, create_tables: bool = True, default_role: str = "viewer")`
  - Calls `init_engine(database_url)` on the database package
  - If `create_tables`, runs Alembic upgrade to head
  - Looks up the built-in `Role` row matching `default_role` name and stores
    as `self._default_role` (used when `user.role_id` is `None`)
- [ ] `async authenticate(username, password)` — `UserBackend` ABC method
  - Query `User` by username with `joinedload(User.role)`
  - Verify password hash via `_verify_password()` from `users.py`
  - Return `AuthUser` or `None`
- [ ] `async get_user(username)` — `UserBackend` ABC method
  - Query `User` by username with `joinedload(User.role)`
  - Check `is_active`
  - Return `AuthUser` or `None`
- [ ] `_to_auth_user(user: User) -> AuthUser` — private helper
  - If `user.role` is `None`, fall back to `self._default_role`
  - For built-in roles: `role=role.name`, `custom_permissions=None`
  - For custom roles: `role="custom"`, `custom_permissions=role.permissions`
- [ ] `async create_user(username, password, role="viewer", email=None, display_name=None)`
  - Hash password with `_hash_password()` from `users.py`
  - If `role` is provided, look up `Role` by name string and set `role_id` FK
  - If `role` is `None`, insert `User` row with `role_id=NULL` (uses default at
    resolution time)
  - Return `AuthUser`
  - Raises `IntegrityError` if username exists; raises `ValueError` if role unknown
- [ ] `async update_user(username, *, password=None, role=None, email=None, display_name=None, is_active=None)`
  - Partial update of user fields
  - If `password` provided, re-hash
  - If `role` provided, look up by name and update `role_id`
  - Return updated `AuthUser`
- [ ] `async delete_user(username)`
  - Hard-delete the user row
  - Raises `ValueError` if user not found
- [ ] `async list_users()`
  - Return all active users as `list[AuthUser]`
- [ ] `async create_role(name, permissions)`
  - Insert `Role` row with `is_builtin=False`
  - Raises `IntegrityError` if name exists
- [ ] `async update_role(name, *, permissions=None)`
  - Update permissions on a custom role
  - Raises `ValueError` if role is built-in or not found
- [ ] `async delete_role(name)`
  - Delete a custom role
  - Raises `ValueError` if role is built-in
  - Raises `ValueError` if users are still assigned to the role
- [ ] `async list_roles()`
  - Return all roles as `list[Role]`
- [ ] `async get_role(name)`
  - Look up role by name, return `Role` or `None`

### Task 2.2 — Update `dagster_webserver/auth/__init__.py`

- [ ] Export `DatabaseUserBackend` from `__init__.py`
- [ ] Add to `__all__`

### Task 2.3 — Tests

- [ ] Create test class `TestDatabaseUserBackend` in `dagster_webserver_tests/test_auth.py`
  (or new file `test_db_backend.py`)
- [ ] Test: `create_user` hashes password, resolves role by name
- [ ] Test: `authenticate` with valid credentials returns `AuthUser`
- [ ] Test: `authenticate` for user with `role_id=NULL` falls back to default role
- [ ] Test: `authenticate` with invalid credentials returns `None`
- [ ] Test: `authenticate` for inactive user returns `None`
- [ ] Test: `get_user` returns `AuthUser` for active user
- [ ] Test: `get_user` returns `None` for inactive user
- [ ] Test: `update_user` partial updates (password, role, email)
- [ ] Test: `delete_user` removes user
- [ ] Test: `list_users` returns only active users
- [ ] Test: `create_role` creates custom role with permissions
- [ ] Test: `update_role` updates permissions, rejects built-in roles
- [ ] Test: `delete_role` deletes custom role, rejects built-in, rejects if users assigned
- [ ] Test: `list_roles` returns all roles
- [ ] Test: `get_role` lookup by name
- [ ] Test: `_to_auth_user` resolves built-in role correctly
- [ ] Test: `_to_auth_user` resolves custom role correctly
- [ ] Test: `_to_auth_user` falls back to default role when `user.role_id` is `None`
- [ ] Test: `create_user` with unknown role name raises `ValueError`
- [ ] Test: `create_user` with duplicate username raises `IntegrityError`
- [ ] **All tests use `sqlite+aiosqlite:///:memory:`** with tables created per-test

______________________________________________________________________

## Phase 3 — CLI Integration

**Objective:** Wire `DatabaseUserBackend` into the CLI so it can be selected via
`--auth-provider database` with `--auth-database-url`.

### Task 3.1 — New CLI options in `dagster_webserver/cli.py`

- [ ] Add `--auth-database-url` option
  - `envvar="DAGSTER_AUTH_DATABASE_URL"`
  - Help: "SQLAlchemy URL for the auth database"
- [ ] Add `--auth-provider database` to the existing choice list
  - Update `click.Choice(["session", "api-key", "database", "none"])`

### Task 3.2 — Provider wiring in `dagster_webserver/cli.py`

- [ ] In `_build_auth_provider()`, handle `auth_provider == "database"`
  - Requires `--auth-database-url`
  - Creates `DatabaseUserBackend(database_url)`
  - Wraps in `SessionAuthProvider` (only auth provider for database mode)
  - Falls back to random session secret if not provided (same warning as today)
  - If the dialect driver is not installed (e.g. `asyncpg` for PostgreSQL),
    fail fast with a clear error: "Driver not installed. Run: pip install
    dagster-webserver[auth-db]"

### Task 3.3 — `dagster-webserver db` CLI group

Database management commands live under a `db` subcommand group (not `auth`) to
keep database actions separate from auth configuration.

- [ ] Add `@cli.group("db")` subcommand group to `cli.py`
- [ ] `db init-admin` command
  - Options: `--username`, `--password`, `--database-url`
  - Creates engine, runs Alembic upgrade, inserts admin user with built-in `admin` role
- [ ] `db create-role` command
  - Options: `--name`, `--permissions` (JSON string), `--database-url`
  - Creates a custom role
- [ ] `db list-roles` command
  - Option: `--database-url`
  - Prints all roles (built-in + custom) with their permission summaries
- [ ] `db update-role` command
  - Options: `--name`, `--permissions` (JSON string), `--database-url`
  - Updates a custom role's permissions
- [ ] `db delete-role` command
  - Options: `--name`, `--database-url`
  - Deletes a custom role

### Task 3.4 — Tests

- [ ] Test: `--auth-provider database` with `--auth-database-url` creates `DatabaseUserBackend`
- [ ] Test: `--auth-provider database` without `--auth-database-url` raises error
- [ ] Test: `db init-admin` CLI creates admin user in database
- [ ] Test: `db create-role` CLI creates custom role
- [ ] Test: `db list-roles` CLI outputs roles
- [ ] Test: `db update-role` CLI updates role permissions
- [ ] Test: `db delete-role` CLI deletes custom role

______________________________________________________________________

## Phase 4 — End-to-End Integration Tests

**Objective:** Verify the full auth flow with `DatabaseUserBackend` works
end-to-end through the webserver.

### Task 4.1 — Integration test fixtures

- [ ] Add fixture: in-memory SQLite database with Alembic migrations applied
- [ ] Add fixture: `DatabaseUserBackend` connected to test database
- [ ] Add fixture: TestClient with `SessionAuthProvider(DatabaseUserBackend(...))`

### Task 4.2 — Integration tests

- [ ] Test: Full login flow (create user via backend → POST /login → session → GET /api/me)
- [ ] Test: GraphQL permissions reflect database role for built-in role
- [ ] Test: GraphQL permissions reflect database role for custom role
- [ ] Test: Logout clears session, subsequent requests are unauthenticated
- [ ] Test: Inactive user cannot authenticate
- [ ] Test: Role permission change affects existing users (update role → re-authenticate → verify new permissions)

### Task 4.3 — Existing test compatibility

- [ ] Verify all 42 existing auth tests still pass
- [ ] Verify all existing webserver tests still pass

______________________________________________________________________

## Phase 5 — Documentation

**Objective:** Update project documentation to cover the new database backend.

### Task 5.1 — `README.rst`

- [ ] Add section: "Database-Backed Authentication"
  - CLI usage: `--auth-provider database --auth-database-url sqlite+aiosqlite:///auth.db`
  - PostgreSQL example URL
  - `dagster-webserver db init-admin` command usage
  - Role management commands (`db create-role`, `db list-roles`, etc.)
- [ ] Add section: "Custom Roles"
  - How to create custom roles via CLI
  - How permissions map to Dagster `Permissions` enum values
  - Example permission JSON

### Task 5.2 — Research cross-reference

- [ ] Ensure `README.rst` links to or references the research doc for architecture details

______________________________________________________________________

## File Inventory

| File | Action | Description |
| --------------------------------------------------------------------------- | ---------- | ------------------------------------ |
| `dagster_webserver/database/__init__.py` | **Create** | Package init, exports |
| `dagster_webserver/database/models.py` | **Create** | `Base`, `Role`, `User` ORM models |
| `dagster_webserver/database/engine.py` | **Create** | Engine + session factory |
| `dagster_webserver/database/alembic/__init__.py` | **Create** | Empty init |
| `dagster_webserver/database/alembic/env.py` | **Create** | Alembic environment |
| `dagster_webserver/database/alembic/script.py.mako` | **Create** | Migration template |
| `dagster_webserver/database/alembic/alembic.ini` | **Create** | Alembic config |
| `dagster_webserver/database/alembic/versions/001_create_roles_and_users.py` | **Create** | Initial migration + seed |
| `dagster_webserver/auth/db_backend.py` | **Create** | `DatabaseUserBackend` |
| `dagster_webserver/auth/__init__.py` | **Modify** | Export `DatabaseUserBackend` |
| `dagster_webserver/cli.py` | **Modify** | New CLI options + auth subcommands |
| `pyproject.toml` | **Modify** | Add `aiosqlite`, `auth-db` group |
| `dagster_webserver_tests/test_db_backend.py` | **Create** | Unit tests for `DatabaseUserBackend` |
| `dagster_webserver_tests/test_database.py` | **Create** | Tests for models, engine, migrations |
| `README.rst` | **Modify** | Database auth documentation |

______________________________________________________________________

## Resolved Questions

1. **Auth provider for database mode** — `SessionAuthProvider` only. API key
   provisioning per-user is future work and out of scope.

1. **CLI command placement** — Database management commands (`init-admin`,
   `create-role`, etc.) live under `dagster-webserver db`, not `auth`.
   File-based user management remains a manual file edit.

1. **Missing dialect driver** — Fail fast with a clear error message pointing
   to `pip install dagster-webserver[auth-db]`.

1. **Alembic template** — Use Alembic's default `script.py.mako`. Auth
   migrations are independent and follow standard conventions.
