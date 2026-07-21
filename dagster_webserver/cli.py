import asyncio
import contextlib
import logging
import os
import sys
import textwrap
from collections.abc import AsyncIterator

import click
import dagster._check as check
import uvicorn
from dagster._annotations import deprecated
from dagster._cli.utils import (
    assert_no_remaining_opts,
    get_possibly_temporary_instance_for_cli,
)
from dagster._cli.workspace.cli_target import (
    WORKSPACE_TARGET_WARNING,
    WorkspaceOpts,
    workspace_opts_to_load_target,
)
from dagster._core.instance import InstanceRef
from dagster._core.telemetry import START_DAGSTER_WEBSERVER, log_action
from dagster._core.telemetry_upload import uploading_logging_thread
from dagster._core.workspace.context import (
    IWorkspaceProcessContext,
    WorkspaceProcessContext,
)
from dagster._serdes import deserialize_value
from dagster._utils import (
    DEFAULT_WORKSPACE_YAML_FILENAME,
    find_free_port,
    is_port_in_use,
)
from dagster._utils.interrupts import setup_interrupt_handlers
from dagster._utils.log import configure_loggers
from dagster_shared.cli import workspace_options
from dagster_shared.ipc import interrupt_on_ipc_shutdown_message

from dagster_webserver.app import create_app_from_workspace_process_context
from dagster_webserver.version import __version__

# Auth imports are lazy to avoid hard dependency when auth is not used
_AUTH_IMPORT_ERROR_MSG = (
    "Auth requires optional dependencies. Install them with:\n"
    "  pip install dagster-webserver[auth]"
)


def create_dagster_webserver_cli():
    return dagster_webserver


# If the user runs `dagit` from the command line, we update this to "dagit"
WEBSERVER_LOGGER_NAME = "dagster-webserver"

DEFAULT_WEBSERVER_HOST = "127.0.0.1"
DEFAULT_WEBSERVER_PORT = 3000

DEFAULT_DB_STATEMENT_TIMEOUT = 15000  # 15 sec
DEFAULT_POOL_RECYCLE = 3600  # 1 hr
DEFAULT_POOL_MAX_OVERFLOW = 20


@click.group(
    name="dagster-webserver",
    invoke_without_command=True,
    help=textwrap.dedent(
        f"""
        Run dagster-webserver. Loads a code location.

        {WORKSPACE_TARGET_WARNING}

        Examples:

        1. dagster-webserver start (works if ./{DEFAULT_WORKSPACE_YAML_FILENAME} exists)

        2. dagster-webserver start -w path/to/{DEFAULT_WORKSPACE_YAML_FILENAME}

        3. dagster-webserver start -f path/to/file.py

        4. dagster-webserver start -f path/to/file.py -d path/to/working_directory

        5. dagster-webserver start -m some_module

        6. dagster-webserver start -f path/to/file.py -a define_repo

        7. dagster-webserver start -m some_module -a define_repo

        8. dagster-webserver start -p 3333

        Options can also provide arguments via environment variables prefixed with DAGSTER_WEBSERVER.

        For example, DAGSTER_WEBSERVER_PORT=3333 dagster-webserver start
    """
    ),
)
@click.version_option(version=__version__, prog_name="dagster-webserver")
@click.pass_context
def dagster_webserver(ctx: click.Context, **_kwargs: object):
    """Top-level CLI group for dagster-webserver."""
    if ctx.invoked_subcommand is None:
        # No subcommand given — default to `start`
        ctx.invoke(start)


@dagster_webserver.command(
    name="start",
    help="Start the Dagster webserver.",
)
@click.option(
    "--host",
    "-h",
    type=click.STRING,
    default=DEFAULT_WEBSERVER_HOST,
    help="Host to run server on",
    show_default=True,
)
@click.option(
    "--port",
    "-p",
    type=click.INT,
    help=f"Port to run server on - defaults to {DEFAULT_WEBSERVER_PORT}",
    default=None,
    show_default=True,
)
@click.option(
    "--path-prefix",
    "-l",
    type=click.STRING,
    default="",
    help="The path prefix where server will be hosted (eg: /dagster-webserver)",
    show_default=True,
)
@click.option(
    "--db-statement-timeout",
    help=(
        "The timeout in milliseconds to set on database statements sent "
        "to the DagsterInstance. Not respected in all configurations."
    ),
    default=DEFAULT_DB_STATEMENT_TIMEOUT,
    type=click.INT,
    show_default=True,
)
@click.option(
    "--db-pool-recycle",
    help=(
        "The maximum age of a connection to use from the sqlalchemy pool without connection"
        " recycling. Set to -1 to disable. Not respected in all configurations."
    ),
    default=DEFAULT_POOL_RECYCLE,
    type=click.INT,
    show_default=True,
)
@click.option(
    "--db-pool-max-overflow",
    help=(
        "The maximum overflow size of the sqlalchemy pool. Set to -1 to disable."
        "Not respected in all configurations."
    ),
    default=DEFAULT_POOL_MAX_OVERFLOW,
    type=click.INT,
    show_default=True,
)
@click.option(
    "--read-only",
    help=(
        "Start server in read-only mode, where all mutations such as launching runs and "
        "turning schedules on/off are turned off."
    ),
    is_flag=True,
)
@click.option(
    "--suppress-warnings",
    help="Filter all warnings when hosting server.",
    is_flag=True,
)
@click.option(
    "--uvicorn-log-level",
    "--log-level",  # Back-compat
    help="Set the log level for the uvicorn web server.",
    show_default=True,
    default="warning",
    type=click.Choice(
        ["critical", "error", "warning", "info", "debug", "trace"], case_sensitive=False
    ),
)
@click.option(
    "--dagster-log-level",
    help="Set the log level for dagster log events.",
    show_default=True,
    default="info",
    type=click.Choice(
        ["critical", "error", "warning", "info", "debug"], case_sensitive=False
    ),
    envvar="DAGSTER_WEBSERVER_LOG_LEVEL",
)
@click.option(
    "--log-format",
    type=click.Choice(["colored", "json", "rich"], case_sensitive=False),
    show_default=True,
    required=False,
    default="colored",
    help="Format of the log output from the webserver",
)
@click.option(
    "--code-server-log-level",
    help="Set the log level for any code servers spun up by the webserver.",
    show_default=True,
    default="info",
    type=click.Choice(
        ["critical", "error", "warning", "info", "debug"], case_sensitive=False
    ),
)
@click.option(
    "--instance-ref",
    type=click.STRING,
    required=False,
    hidden=True,
)
@click.option(
    "--live-data-poll-rate",
    help="Rate at which the dagster UI polls for updated asset data (in milliseconds)",
    type=click.INT,
    required=False,
    default=2000,
    show_default=True,
)
@click.option(
    "--shutdown-pipe",
    type=click.INT,
    required=False,
    hidden=True,
    help="Internal use only. Pass a readable pipe file descriptor to the webserver process that will be monitored for a shutdown signal.",
)
# -- Auth options --
@click.option(
    "--auth-provider",
    type=click.Choice(
        ["session", "api-key", "database", "hybrid", "none"], case_sensitive=False
    ),
    default="none",
    help=(
        "Authentication provider to use. "
        "'session' uses cookie-based sessions with username/password. "
        "'api-key' uses Bearer token auth. "
        "'database' uses cookie-based sessions backed by a database. "
        "'hybrid' uses cookie-based sessions backed by a database with OIDC support. "
        "'none' disables auth (default)."
    ),
)
@click.option(
    "--auth-database-url",
    type=click.STRING,
    envvar="DAGSTER_AUTH_DATABASE_URL",
    default=None,
    help=(
        "SQLAlchemy URL for the auth database "
        "(e.g. sqlite+aiosqlite:///auth.db or "
        "postgresql+asyncpg://user:pass@host/db). "
        "Required when --auth-provider=database."
    ),
)
@click.option(
    "--users-file",
    type=click.Path(exists=False),
    default=None,
    help="Path to a YAML or JSON file defining users and their roles.",
)
@click.option(
    "--session-secret",
    type=click.STRING,
    envvar="DAGSTER_WEBSERVER_SESSION_SECRET",
    default=None,
    help="Secret key for signing session cookies. Required when --auth-provider=session.",
)
@click.option(
    "--default-role",
    type=click.Choice(
        ["catalog_viewer", "viewer", "launcher", "editor", "admin"],
        case_sensitive=False,
    ),
    default="viewer",
    help="Default role for users when no explicit role is assigned.",
)
@click.option(
    "--enable-admin-portal",
    is_flag=True,
    default=False,
    help=(
        "Enable the admin portal at /admin. Requires --auth-provider=database "
        "and --auth-database-url (or --admin-database-url)."
    ),
)
@click.option(
    "--admin-database-url",
    type=click.STRING,
    envvar="DAGSTER_ADMIN_DATABASE_URL",
    default=None,
    help=(
        "SQLAlchemy URL for the admin portal database. Defaults to --auth-database-url "
        "if not specified."
    ),
)
@workspace_options
def start(
    host: str,
    port: int,
    path_prefix: str,
    db_statement_timeout: int,
    db_pool_recycle: int,
    db_pool_max_overflow: int,
    read_only: bool,
    suppress_warnings: bool,
    uvicorn_log_level: str,
    dagster_log_level: str,
    log_format: str,
    code_server_log_level: str,
    instance_ref: str | None,
    live_data_poll_rate: int,
    shutdown_pipe: int | None,
    auth_provider: str,
    users_file: str | None,
    session_secret: str | None,
    auth_database_url: str | None,
    default_role: str,
    enable_admin_portal: bool,
    admin_database_url: str | None,
    **other_opts: object,
):
    """Start the Dagster webserver."""
    workspace_opts = WorkspaceOpts.extract_from_cli_options(other_opts)
    assert_no_remaining_opts(other_opts)

    if suppress_warnings:
        os.environ["PYTHONWARNINGS"] = "ignore"

    configure_loggers(formatter=log_format, log_level=dagster_log_level.upper())
    logger = logging.getLogger(WEBSERVER_LOGGER_NAME)

    if sys.argv[0].endswith("dagit"):
        logger.warning(
            "The `dagit` CLI command is deprecated and will be removed in dagster 2.0. Please use"
            " `dagster-webserver` instead."
        )

    # Set up windows interrupt signals to raise KeyboardInterrupt. Note that these handlers are
    # not used if we are using the shutdown pipe.
    setup_interrupt_handlers()

    with contextlib.ExitStack() as stack:
        if shutdown_pipe:
            stack.enter_context(interrupt_on_ipc_shutdown_message(shutdown_pipe))
        instance = stack.enter_context(
            get_possibly_temporary_instance_for_cli(
                cli_command="dagster-webserver",
                instance_ref=deserialize_value(instance_ref, InstanceRef)
                if instance_ref
                else None,
                logger=logger,
            )
        )
        # Allow the instance components to change behavior in the context of a long running server process
        instance.optimize_for_webserver(
            db_statement_timeout, db_pool_recycle, db_pool_max_overflow
        )

        # -- Build auth provider if requested --
        auth_provider_instance = _build_auth_provider(
            auth_provider=auth_provider,
            users_file=users_file,
            session_secret=session_secret,
            auth_database_url=auth_database_url,
            default_role=default_role,
        )

        # -- Build admin portal if requested --
        admin_portal_instance = _build_admin_portal(
            enable_admin_portal=enable_admin_portal,
            auth_provider_instance=auth_provider_instance,
            auth_database_url=auth_database_url,
            admin_database_url=admin_database_url,
        )

        with WorkspaceProcessContext(
            instance,
            version=__version__,
            read_only=read_only,
            workspace_load_target=workspace_opts_to_load_target(workspace_opts),
            code_server_log_level=code_server_log_level,
        ) as workspace_process_context:
            host_dagster_ui_with_workspace_process_context(
                workspace_process_context,
                host,
                port,
                path_prefix,
                uvicorn_log_level,
                live_data_poll_rate,
                auth_provider=auth_provider_instance,
                admin_portal=admin_portal_instance,
            )


@contextlib.asynccontextmanager
async def _lifespan(app) -> AsyncIterator:
    # workaround from https://github.com/encode/uvicorn/issues/1160 for termination
    try:
        yield
    except asyncio.exceptions.CancelledError:
        logging.getLogger(WEBSERVER_LOGGER_NAME).info(
            f"Server for {WEBSERVER_LOGGER_NAME} was shut down."
        )
        # Expected error when dagster-webserver is terminated by CTRL-C, suppress
        pass


def host_dagster_ui_with_workspace_process_context(
    workspace_process_context: IWorkspaceProcessContext,
    host: str | None,
    port: int | None,
    path_prefix: str,
    log_level: str,
    live_data_poll_rate: int | None = None,
    auth_provider: object | None = None,
    admin_portal: object | None = None,
):
    check.inst_param(
        workspace_process_context, "workspace_process_context", IWorkspaceProcessContext
    )
    host = check.opt_str_param(host, "host", "127.0.0.1")
    check.opt_int_param(port, "port")
    check.str_param(path_prefix, "path_prefix")
    check.opt_int_param(live_data_poll_rate, "live_data_poll_rate")

    logger = logging.getLogger(WEBSERVER_LOGGER_NAME)

    if auth_provider is not None:
        logger.info(
            "Authentication enabled (provider: %s)", type(auth_provider).__name__
        )
    if admin_portal is not None:
        logger.info("Admin portal enabled at /admin")

    app = create_app_from_workspace_process_context(
        workspace_process_context,
        path_prefix,
        live_data_poll_rate,
        auth_provider=auth_provider,
        admin_portal=admin_portal,
        lifespan=_lifespan,
    )

    if not port:
        if is_port_in_use(host, DEFAULT_WEBSERVER_PORT):
            port = find_free_port()
            logger.warning(
                f"Port {DEFAULT_WEBSERVER_PORT} is in use - using port {port} instead"
            )
        else:
            port = DEFAULT_WEBSERVER_PORT

    logger.info(
        f"Serving dagster-webserver on http://{host}:{port}{path_prefix} in process {os.getpid()}"
    )
    log_action(workspace_process_context.instance, START_DAGSTER_WEBSERVER)
    with uploading_logging_thread():
        uvicorn.run(
            app,
            host=host,
            port=port,
            log_level=log_level,
        )


# ---------------------------------------------------------------------------
# Auth helper functions
# ---------------------------------------------------------------------------


def _build_admin_portal(
    enable_admin_portal: bool,
    auth_provider_instance: object | None,
    auth_database_url: str | None,
    admin_database_url: str | None,
) -> object | None:
    """Build an AdminPortal instance from CLI options.

    Returns ``None`` when the portal is not enabled.
    """
    if not enable_admin_portal:
        return None

    # Admin portal requires database-backed auth
    if auth_provider_instance is None:
        raise click.UsageError(
            "Admin portal requires authentication. Use --auth-provider=database "
            "or --auth-provider=hybrid."
        )

    # Use admin_database_url if provided, otherwise fall back to auth_database_url
    db_url = admin_database_url or auth_database_url
    if not db_url:
        raise click.UsageError(
            "Admin portal requires a database URL. Use --auth-database-url "
            "or --admin-database-url."
        )

    try:
        from dagster_webserver.admin.portal import AdminPortal
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
    except ImportError as exc:
        raise click.UsageError(
            "Admin portal requires optional dependencies. Install with:\n"
            "  pip install dagster-webserver[auth]"
        ) from exc

    backend = DatabaseUserBackend(db_url, create_tables=True)
    return AdminPortal(backend)


def _build_auth_provider(
    auth_provider: str,
    users_file: str | None,
    session_secret: str | None,
    auth_database_url: str | None,
    default_role: str,
) -> object | None:
    """Build an auth provider instance from CLI options.

    Returns ``None`` when ``auth_provider == "none"``."``.
    """
    auth_provider = auth_provider.lower()
    if auth_provider == "none":
        return None

    from dagster_webserver.auth.provider import (
        ApiKeyAuthProvider,
        AuthConfig,
        SessionAuthProvider,
    )
    from dagster_webserver.auth.users import FileUserBackend, InMemoryUserBackend

    config = AuthConfig(
        default_role=default_role,
        allowed_routes=["login", "static", "root_static", "favicon_static"],
    )

    # Handle database-backed auth
    if auth_provider == "database":
        if not auth_database_url:
            raise click.BadParameter(
                "--auth-database-url is required when --auth-provider=database",
                param_hint="--auth-database-url",
            )
        try:
            from dagster_webserver.auth.db_backend import DatabaseUserBackend
        except ImportError as exc:
            raise click.UsageError(
                "Database auth requires optional dependencies. Install with:\n"
                "  pip install dagster-webserver[auth]"
            ) from exc

        if not session_secret:
            import secrets

            session_secret = secrets.token_hex(32)
            logging.getLogger(WEBSERVER_LOGGER_NAME).warning(
                "No --session-secret provided. A random secret was generated. "
                "Set DAGSTER_WEBSERVER_SESSION_SECRET or use --session-secret for persistence."
            )
        config._session_secret = session_secret  # type: ignore[attr-defined]
        user_backend = DatabaseUserBackend(
            auth_database_url,
            default_role=default_role,
        )
        return SessionAuthProvider(user_backend, config=config)

    # Handle hybrid (database + OIDC) auth
    if auth_provider == "hybrid":
        if not auth_database_url:
            raise click.BadParameter(
                "--auth-database-url is required when --auth-provider=hybrid",
                param_hint="--auth-database-url",
            )
        try:
            from dagster_webserver.auth.db_backend import DatabaseUserBackend
            from dagster_webserver.auth.provider import HybridSessionAuthProvider
        except ImportError as exc:
            raise click.UsageError(
                "Hybrid auth requires optional dependencies. Install with:\n"
                "  pip install dagster-webserver[auth-oidc]"
            ) from exc

        if not session_secret:
            import secrets

            session_secret = secrets.token_hex(32)
            logging.getLogger(WEBSERVER_LOGGER_NAME).warning(
                "No --session-secret provided. A random secret was generated. "
                "Set DAGSTER_WEBSERVER_SESSION_SECRET or use --session-secret for persistence."
            )
        config._session_secret = session_secret  # type: ignore[attr-defined]
        config.allowed_routes = list(config.allowed_routes) + [
            "oidc-authorize",
            "oidc-callback",
        ]
        user_backend = DatabaseUserBackend(
            auth_database_url,
            default_role=default_role,
        )
        return HybridSessionAuthProvider(user_backend, config=config)

    # Build user backend from --users-file
    if users_file:
        user_backend = FileUserBackend(users_file)
    else:
        # Default admin user for quick start
        user_backend = InMemoryUserBackend(
            {
                "admin": {"password": "admin", "role": "admin"},
            }
        )

    if auth_provider == "session":
        if not session_secret:
            import secrets

            session_secret = secrets.token_hex(32)
            logging.getLogger(WEBSERVER_LOGGER_NAME).warning(
                "No --session-secret provided. A random secret was generated. "
                "Set DAGSTER_WEBSERVER_SESSION_SECRET or use --session-secret for persistence."
            )
        config._session_secret = session_secret  # type: ignore[attr-defined]
        return SessionAuthProvider(user_backend, config=config)

    if auth_provider == "api-key":
        return ApiKeyAuthProvider(user_backend, config=config)

    raise click.BadParameter(f"Unknown auth provider: {auth_provider}")


# ---------------------------------------------------------------------------
# ``dagster-webserver db`` subcommand group
# ---------------------------------------------------------------------------


@dagster_webserver.group(name="db", help="Manage the auth database.")
def db_cli():
    """Database management commands."""
    pass


@db_cli.command(name="init-admin", help="Bootstrap the first admin user.")
@click.option("--username", required=True, help="Admin username.")
@click.option("--password", required=True, help="Admin password.")
@click.option(
    "--database-url",
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_init_admin(username: str, password: str, database_url: str) -> None:
    """Create the first admin user in the auth database."""
    try:
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
    except ImportError as exc:
        raise click.UsageError(
            "Database auth requires optional dependencies. Install with:\n"
            "  pip install dagster-webserver[auth]"
        ) from exc

    backend = DatabaseUserBackend(database_url, create_tables=True)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(backend.create_user(username, password, role="admin"))
    finally:
        loop.close()
    click.echo(f"Admin user '{username}' created successfully.")


@db_cli.command(name="create-role", help="Create a new custom role.")
@click.option("--name", required=True, help="Role name.")
@click.option(
    "--permissions",
    required=True,
    help="JSON object mapping permission names to booleans "
    '(e.g. \'{"read_workspace": true, "launch_pipeline_execution": false}\').',
)
@click.option(
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_create_role(name: str, permissions: str, database_url: str) -> None:
    """Create a custom role with the given permissions."""
    import json

    try:
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
    except ImportError as exc:
        raise click.UsageError(
            "Database auth requires optional dependencies. Install with:\n"
            "  pip install dagster-webserver[auth]"
        ) from exc

    try:
        perm_map = json.loads(permissions)
    except json.JSONDecodeError as exc:
        raise click.BadParameter("Invalid JSON", hint=permissions) from exc

    backend = DatabaseUserBackend(database_url, create_tables=True)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(backend.create_role(name, perm_map))
    finally:
        loop.close()
    click.echo(f"Custom role '{name}' created successfully.")


@db_cli.command(name="list-roles", help="List all roles (built-in and custom).")
@click.option(
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_list_roles(database_url: str) -> None:
    """List all roles."""
    try:
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
    except ImportError as exc:
        raise click.UsageError(
            "Database auth requires optional dependencies. Install with:\n"
            "  pip install dagster-webserver[auth]"
        ) from exc

    backend = DatabaseUserBackend(database_url, create_tables=True)
    loop = asyncio.new_event_loop()
    try:
        roles = loop.run_until_complete(backend.list_roles())
    finally:
        loop.close()

    for role in roles:
        tag = "[built-in]" if role.is_builtin else "[custom]"
        enabled = [k for k, v in role.permissions.items() if v]
        click.echo(f"  {role.name:<20} {tag:<12} {len(enabled)} permissions enabled")


@db_cli.command(name="update-role", help="Update a custom role's permissions.")
@click.option("--name", required=True, help="Role name.")
@click.option(
    "--permissions",
    required=True,
    help="JSON object mapping permission names to booleans.",
)
@click.option(
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_update_role(name: str, permissions: str, database_url: str) -> None:
    """Update a custom role's permissions."""
    import json

    try:
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
    except ImportError as exc:
        raise click.UsageError(
            "Database auth requires optional dependencies. Install with:\n"
            "  pip install dagster-webserver[auth]"
        ) from exc

    try:
        perm_map = json.loads(permissions)
    except json.JSONDecodeError as exc:
        raise click.BadParameter("Invalid JSON", hint=permissions) from exc

    backend = DatabaseUserBackend(database_url, create_tables=True)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(backend.update_role(name, permissions=perm_map))
    finally:
        loop.close()
    click.echo(f"Role '{name}' updated successfully.")


@db_cli.command(name="delete-role", help="Delete a custom role.")
@click.option("--name", required=True, help="Role name.")
@click.option(
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_delete_role(name: str, database_url: str) -> None:
    """Delete a custom role."""
    try:
        from dagster_webserver.auth.db_backend import DatabaseUserBackend
    except ImportError as exc:
        raise click.UsageError(
            "Database auth requires optional dependencies. Install with:\n"
            "  pip install dagster-webserver[auth]"
        ) from exc

    backend = DatabaseUserBackend(database_url, create_tables=True)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(backend.delete_role(name))
    finally:
        loop.close()
    click.echo(f"Role '{name}' deleted successfully.")


# ---------------------------------------------------------------------------
# Alembic migration commands
# ---------------------------------------------------------------------------


def _get_alembic_config(database_url: str):
    """Build an Alembic Config pointing at the auth database."""
    import os

    from alembic.config import Config

    # Locate the alembic directory shipped with the package
    alembic_dir = os.path.join(os.path.dirname(__file__), "database", "alembic")
    alembic_ini = os.path.join(alembic_dir, "alembic.ini")
    config = Config(alembic_ini)
    config.set_main_option("sqlalchemy.url", database_url)
    # Ensure script_location is absolute so Alembic finds env.py
    config.set_main_option("script_location", alembic_dir)
    return config


@db_cli.command(
    name="migrate",
    help="Run all pending database migrations (alias for upgrade head).",
)
@click.option(
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_migrate(database_url: str) -> None:
    """Upgrade the database to the latest revision."""
    config = _get_alembic_config(database_url)
    from alembic.command import upgrade

    upgrade(config, "head")
    click.echo("Database migrated to head successfully.")


@db_cli.command(name="upgrade", help="Upgrade the database to a specific revision.")
@click.option(
    "--revision",
    "-r",
    required=True,
    help="Target revision (e.g. 'head', '001', '-1').",
)
@click.option(
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_upgrade(database_url: str, revision: str) -> None:
    """Upgrade the database to a specific revision (e.g. 'head', '001', '-1')."""
    config = _get_alembic_config(database_url)
    from alembic.command import upgrade

    upgrade(config, revision)
    click.echo(f"Database upgraded to revision '{revision}'.")


@db_cli.command(name="downgrade", help="Downgrade the database to a specific revision.")
@click.option(
    "--revision",
    "-r",
    required=True,
    help="Target revision (e.g. '001', '-1', 'base').",
)
@click.option(
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_downgrade(database_url: str, revision: str) -> None:
    """Downgrade the database to a specific revision (e.g. '001', '-1', 'base')."""
    config = _get_alembic_config(database_url)
    from alembic.command import downgrade

    downgrade(config, revision)
    click.echo(f"Database downgraded to revision '{revision}'.")


@db_cli.command(name="current", help="Show the current revision for the database.")
@click.option(
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_current(database_url: str) -> None:
    """Show the current revision for the database."""
    config = _get_alembic_config(database_url)
    from alembic.command import current

    current(config, verbose=True)


@db_cli.command(name="history", help="List all available migration revisions.")
@click.option(
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_history(database_url: str) -> None:
    """List all available migration revisions."""
    config = _get_alembic_config(database_url)
    from alembic.command import history

    history(config, verbose=True)


@db_cli.command(
    name="stamp", help="Stamp the database with a revision without running migrations."
)
@click.option(
    "--revision",
    "-r",
    required=True,
    help="Target revision (e.g. 'head', '001', 'base').",
)
@click.option(
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_stamp(database_url: str, revision: str) -> None:
    """Stamp the database with a revision without running migrations (e.g. 'head', '001')."""
    config = _get_alembic_config(database_url)
    from alembic.command import stamp

    stamp(config, revision)
    click.echo(f"Database stamped with revision '{revision}'.")


@db_cli.command(
    name="check",
    help="Check if the database is at the latest revision.",
)
@click.option(
    "--database-url",
    envvar="DAGSTER_AUTH_DATABASE_URL",
    required=True,
    help="SQLAlchemy URL for the auth database.",
)
def db_check(database_url: str) -> None:
    """Check if the database schema is up to date."""
    import sys

    from alembic.command import check
    from alembic.util import AutogenerateDiffsDetected

    config = _get_alembic_config(database_url)
    try:
        check(config)
        click.echo("Database schema is up to date.")
    except AutogenerateDiffsDetected:
        click.echo(
            "Schema differences detected. Run 'alembic revision --autogenerate -m "
            "\"<message>\"' to generate a migration, or run 'dagster-webserver db migrate' "
            "to apply pending migrations.",
            err=True,
        )
        sys.exit(1)


cli = create_dagster_webserver_cli()


@deprecated(
    breaking_version="2.0",
    subject="DAGIT_* environment variables, WEBSERVER_LOGGER_NAME",
    emit_runtime_warning=False,
)
def main():
    # We only ever update this variable here. It is used to set the logger name as "dagit" if the
    # user invokes "dagit" on the command line.
    global WEBSERVER_LOGGER_NAME  # noqa: PLW0603

    # Click does not support passing multiple env var prefixes, so for backcompat we will convert any
    # DAGIT_* env vars to their DAGSTER_WEBSERVER_* equivalents here. Remove this in 2.0.
    for key, val in os.environ.items():
        if key.startswith("DAGIT_"):
            new_key = "DAGSTER_WEBSERVER_" + key[6:]
            if new_key not in os.environ:
                os.environ[new_key] = val

    if sys.argv[0].endswith("dagit"):
        WEBSERVER_LOGGER_NAME = "dagit"

    # click magic
    cli(auto_envvar_prefix="DAGSTER_WEBSERVER")
