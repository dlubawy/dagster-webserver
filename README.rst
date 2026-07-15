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
   * - ``--auth-provider {session,api-key,none}``
     - Authentication mode. ``none`` is the default (no auth).
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
