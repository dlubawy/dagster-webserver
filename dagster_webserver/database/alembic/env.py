"""Alembic environment configuration for the auth database.

Imports ``Base.metadata`` from the database package so Alembic sees the
correct model metadata.
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection

from dagster_webserver.database.models import Base

# Alembic Config object
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set target metadata for autogenerate support
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_with_connection(connection: Connection) -> None:
    """Run migrations with an already-open sync connection.

    This is used when the caller (e.g. ``DatabaseUserBackend._run_migrations``)
    provides its own connection to avoid spawning a second event loop.
    """
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    If a connection was passed via ``config.attributes["connection"]``, use it
    directly (sync path).  Otherwise fall back to creating an async engine.
    """
    # Check if a sync connection was provided by the caller
    connection = config.attributes.get("connection")
    if connection is not None:
        run_migrations_with_connection(connection)
    else:
        # No connection provided — create our own async engine
        import asyncio

        from sqlalchemy.ext.asyncio import async_engine_from_config

        async def do_run_migrations() -> None:
            connectable = async_engine_from_config(
                config.get_section(config.config_ini_section, {}),
                prefix="sqlalchemy.",
                poolclass=pool.NullPool,
            )
            async with connectable.connect() as conn:
                await conn.run_sync(lambda c: run_migrations_with_connection(c))
            await connectable.dispose()

        asyncio.run(do_run_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
