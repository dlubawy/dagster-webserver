"""Database infrastructure for dagster-webserver auth.

Exports
-------
Models
~~~~~~
- ``Base`` — DeclarativeBase for auth tables
- ``Role`` — ORM model for the ``roles`` table
- ``User`` — ORM model for the ``users`` table
- ``UserRoleAssignment`` — per-location role assignments
- ``ApiKey`` — user-owned API keys
- ``AgentHeartbeat`` — agent health records
- ``AgentRequest`` — work queue items
- ``CodeLocationDefinition`` — workspace entries
- ``CodeLocationSnapshot`` — loaded repository data

Engine
~~~~~~
- ``init_engine(database_url)`` — create/replace the async engine + session factory
- ``get_engine()`` — return the current engine
- ``get_session_factory()`` — return the current session factory
"""  # noqa: D205, D400

from dagster_webserver.database.engine import (
    get_engine,
    get_session_factory,
    init_engine,
)
from dagster_webserver.database.models import (
    CODE_LOCATION_STATUSES,
    AgentHeartbeat,
    AgentRequest,
    ApiKey,
    Base,
    CodeLocationDefinition,
    CodeLocationSnapshot,
    OIDCProvider,
    Role,
    User,
    UserRoleAssignment,
)

__all__ = [
    "CODE_LOCATION_STATUSES",
    "AgentHeartbeat",
    "AgentRequest",
    "ApiKey",
    "Base",
    "CodeLocationDefinition",
    "CodeLocationSnapshot",
    "OIDCProvider",
    "Role",
    "User",
    "UserRoleAssignment",
    "init_engine",
    "get_engine",
    "get_session_factory",
]
