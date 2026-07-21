"""Database infrastructure for dagster-webserver auth.

Exports
-------
Models
~~~~~~
- ``Base`` — DeclarativeBase for auth tables
- ``Role`` — ORM model for the ``roles`` table
- ``User`` — ORM model for the ``users`` table

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
from dagster_webserver.database.models import Base, OIDCProvider, Role, User

__all__ = [
    "Base",
    "OIDCProvider",
    "Role",
    "User",
    "init_engine",
    "get_engine",
    "get_session_factory",
]
