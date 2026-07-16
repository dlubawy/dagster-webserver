=================
Dagster Webserver
=================

Web UI for Dagster.

Usage
~~~~~

Basic usage
-----------

.. code-block:: sh

  dagster-webserver -p 3333

Running with a workspace file:

.. code-block:: sh

  dagster-webserver -w path/to/workspace.yaml

Running dev UI
--------------

.. code-block:: sh

  NEXT_PUBLIC_BACKEND_ORIGIN="http://localhost:3333" yarn start


Authentication and RBAC
-----------------------

dagster-webserver supports optional user login with role-based access control.
Auth is **disabled by default** — enable it with the ``--auth-provider`` flag.

Enabling session-based auth
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: sh

  dagster-webserver --auth-provider session --session-secret my-secret-key

This starts the webserver with cookie-based sessions. A default ``admin`` user
(with password ``admin``) is created. On first visit the browser will be
redirected to a login page at ``/login``.

Specifying users from a file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a YAML file (e.g. ``users.yaml``):

.. code-block:: yaml

  users:
    admin:
      password: changeme
      role: admin
      email: admin@example.com
    editor:
      password: editor-pass
      role: editor
    viewer:
      password: viewer-pass
      role: viewer

Then start the webserver:

.. code-block:: sh

  dagster-webserver --auth-provider session --users-file users.yaml --session-secret my-secret-key

Roles
~~~~~

Five built-in roles are available, modelled after Dagster+ cloud:

+----------------+-------------------+-------------------+-------------------+-------------------+-------------------+
| Permission     | CATALOG\_VIEWER   | VIEWER            | LAUNCHER          | EDITOR            | ADMIN             |
+================+===================+===================+===================+===================+===================+
| Launch runs    | ✗                 | ✗                 | ✓                 | ✓                 | ✓                 |
+----------------+-------------------+-------------------+-------------------+-------------------+-------------------+
| Re-execute     | ✗                 | ✗                 | ✓                 | ✓                 | ✓                 |
+----------------+-------------------+-------------------+-------------------+-------------------+-------------------+
| Terminate runs | ✗                 | ✗                 | ✓                 | ✓                 | ✓                 |
+----------------+-------------------+-------------------+-------------------+-------------------+-------------------+
| Start schedule | ✗                 | ✗                 | ✗                 | ✓                 | ✓                 |
+----------------+-------------------+-------------------+-------------------+-------------------+-------------------+
| Edit sensors   | ✗                 | ✗                 | ✗                 | ✓                 | ✓                 |
+----------------+-------------------+-------------------+-------------------+-------------------+-------------------+
| Reload workspace| ✗                | ✗                 | ✗                 | ✗                 | ✓                 |
+----------------+-------------------+-------------------+-------------------+-------------------+-------------------+
| Wipe assets    | ✗                 | ✗                 | ✗                 | ✗                 | ✓                 |
+----------------+-------------------+-------------------+-------------------+-------------------+-------------------+

Custom permissions can be defined per user by setting ``role: custom`` and
providing an explicit ``custom_permissions`` map in the users file.

Database-backed auth
~~~~~~~~~~~~~~~~~~~~

For deployments that need runtime user management (create, update, delete
users without restarting), use the ``database`` auth provider:

.. code-block:: sh

  # SQLite (dev)
  dagster-webserver --auth-provider database \
    --auth-database-url sqlite+aiosqlite:///auth.db \
    --session-secret my-secret-key

  # PostgreSQL (production)
  dagster-webserver --auth-provider database \
    --auth-database-url postgresql+asyncpg://user:pass@host/db \
    --session-secret my-secret-key

The first time the server starts, it creates the ``roles`` and ``users``
tables and seeds the five built-in roles automatically.

Bootstrap the first admin user:

.. code-block:: sh

  dagster-webserver db init-admin \
    --username admin \
    --password changeme \
    --database-url sqlite+aiosqlite:///auth.db

Managing custom roles
^^^^^^^^^^^^^^^^^^^^^

Custom roles are first-class entities stored in the database.  Create one:

.. code-block:: sh

  dagster-webserver db create-role \
    --name analyst \
    --permissions '{"LAUNCH_PIPELINE_EXECUTION": true, "LAUNCH_PIPELINE_REEXECUTION": true}' \
    --database-url sqlite+aiosqlite:///auth.db

List all roles (built-in and custom):

.. code-block:: sh

  dagster-webserver db list-roles --database-url sqlite+aiosqlite:///auth.db

Update or delete custom roles:

.. code-block:: sh

  dagster-webserver db update-role --name analyst --permissions '{...}' --database-url sqlite+aiosqlite:///auth.db
  dagster-webserver db delete-role --name analyst --database-url sqlite+aiosqlite:///auth.db

Dependencies
^^^^^^^^^^^^

- SQLite (dev): ``pip install dagster-webserver[auth]``
- PostgreSQL (production): ``pip install dagster-webserver[auth-db]``

Default role fallback
^^^^^^^^^^^^^^^^^^^^^

If a user is created without an explicit role assignment (``role_id`` is
``NULL``), the ``--default-role`` flag determines their effective role.
This defaults to ``viewer``.

API key auth
~~~~~~~~~~~~

For programmatic access (CI/CD, scripts), use the ``api-key`` provider:

.. code-block:: sh

  dagster-webserver --auth-provider api-key --users-file users.yaml

Then authenticate requests with a ``Bearer`` token:

.. code-block:: sh

  curl -H "Authorization: Bearer <api-token>" http://localhost:3000/graphql

CLI options for auth
~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Option
     - Description
   * - ``--auth-provider {session,api-key,database,none}``
     - Authentication mode. ``none`` is the default (no auth).
   * - ``--auth-database-url URL``
     - SQLAlchemy connection string for the auth database
       (e.g. ``sqlite+aiosqlite:///auth.db`` or
       ``postgresql+asyncpg://user:pass@host/db``).
       Required when ``--auth-provider=database``.
   * - ``--users-file PATH``
     - Path to a YAML or JSON file defining users, passwords, and roles.
   * - ``--session-secret SECRET``
     - Secret key for signing session cookies. Also available via the
       ``DAGSTER_WEBSERVER_SESSION_SECRET`` environment variable.
   * - ``--default-role {catalog_viewer,viewer,launcher,editor,admin}``
     - Fallback role for users with no explicit role. Defaults to ``viewer``.

Sign out
~~~~~~~~

When auth is enabled, a **Sign out** button appears in the left navigation
sidebar (between *Collapse* and *Settings*). Clicking it clears the session
and redirects to the login page.
