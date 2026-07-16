"""Async SQLAlchemy engine and session factory for the auth database.

Provides ``init_engine()`` which creates a new async engine and sessionmaker
from a database URL.  The returned objects are stored as module-level
attributes so they can be reused across requests.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import NoSuchModuleError as SAModuleNotFoundError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger("dagster-webserver.database")

# Module-level state — replaced each time init_engine() is called.
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_engine(
    database_url: str,
) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    """Create (or replace) the async engine and session factory.

    Args:
        database_url: SQLAlchemy connection string
            (e.g. ``sqlite+aiosqlite:///auth.db`` or
            ``postgresql+asyncpg://user:pass@host/db``).

    Returns:
        ``(engine, AsyncSession)`` tuple for direct use.

    Raises:
        ImportError: If the dialect driver is not installed.  The error
            message points the user at the correct optional dependency.
    """
    global _engine, _session_factory

    try:
        engine = create_async_engine(database_url, echo=False)
    except SAModuleNotFoundError as exc:
        _suggest_driver_install(database_url, exc)
        raise

    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    _engine = engine
    _session_factory = session_factory

    logger.info("Database engine initialised for %s", _mask_url(database_url))
    return engine, session_factory


def get_engine() -> AsyncEngine:
    """Return the current async engine.

    Raises:
        RuntimeError: If ``init_engine()`` has not been called yet.
    """
    if _engine is None:
        raise RuntimeError(
            "Database engine not initialised. Call init_engine() first "
            "or pass --auth-database-url to the CLI."
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the current session factory.

    Raises:
        RuntimeError: If ``init_engine()`` has not been called yet.
    """
    if _session_factory is None:
        raise RuntimeError(
            "Session factory not initialised. Call init_engine() first "
            "or pass --auth-database-url to the CLI."
        )
    return _session_factory


def _mask_url(url: str) -> str:
    """Mask credentials in a database URL for logging."""
    if "@" in url:
        scheme, rest = url.split("://", 1)
        host_db = rest.split("@", 1)[1]
        return f"{scheme}://***@{host_db}"
    return url


def _suggest_driver_install(url: str, exc: Exception) -> None:
    """Log a helpful hint about which optional dependency to install."""
    if "asyncpg" in url or "postgresql" in url:
        logger.error(
            "PostgreSQL driver not installed. Install with: "
            "pip install dagster-webserver[auth-db]"
        )
    elif "aiosqlite" in url or "sqlite" in url:
        logger.error(
            "SQLite async driver not installed. Install with: "
            "pip install dagster-webserver[auth]"
        )
    else:
        logger.error(
            "Database driver not installed for URL: %s\nOriginal error: %s",
            _mask_url(url),
            exc,
        )
