"""Workspace YAML loader — loads code location definitions from workspace.yaml.

Supports an optional `workspace.yaml` file at `DAGSTER_HOME/workspace.yaml`
that defines code locations in a static configuration format. On webserver
startup, entries are upserted into the `code_location_definitions` table.

This provides a simple alternative to defining locations via the admin
portal or REST API — useful for developers and operators who prefer
infrastructure-as-code style configuration.

YAML format::

    locations:
      - name: my-location
        python_file: /path/to/file.py
        working_directory: /path/to/dir
        labels:
          deployment: production

      - name: etl-pipelines
        module_name: my_package
        working_directory: /opt/etl
        executable_path: /usr/bin/python3
"""

from __future__ import annotations

import logging
import os
from typing import Any

import yaml

logger = logging.getLogger("dagster-webserver.workspace_loader")


def load_workspace_yaml(dagster_home: str | None = None) -> list[dict[str, Any]]:
    """Load code location definitions from workspace.yaml.

    Args:
        dagster_home: Path to DAGSTER_HOME. Defaults to $DAGSTER_HOME or ".".

    Returns:
        List of location definition dicts, each with at minimum a "name" key.
        Returns empty list if the file doesn't exist or is empty.
    """
    if dagster_home is None:
        dagster_home = os.environ.get("DAGSTER_HOME", ".")

    yaml_path = os.path.join(dagster_home, "workspace.yaml")

    if not os.path.exists(yaml_path):
        return []

    try:
        with open(yaml_path) as f:
            data = yaml.safe_load(f) or {}
    except (yaml.YAMLError, OSError) as exc:
        logger.error("Failed to read workspace.yaml at %s: %s", yaml_path, exc)
        return []

    locations = data.get("locations", [])
    if not isinstance(locations, list):
        logger.error(
            "workspace.yaml 'locations' must be a list, got %s",
            type(locations).__name__,
        )
        return []

    # Validate and normalize entries
    valid_locations = []
    for i, entry in enumerate(locations):
        if not isinstance(entry, dict):
            logger.warning("workspace.yaml location[%d] is not a mapping, skipping", i)
            continue

        name = entry.get("name")
        if not name:
            logger.warning("workspace.yaml location[%d] missing 'name', skipping", i)
            continue

        # Build deploy_metadata from known fields
        deploy_metadata: dict[str, Any] = {}
        for key in (
            "python_file",
            "module_name",
            "working_directory",
            "executable_path",
            "attribute",
            "image",
        ):
            if key in entry:
                deploy_metadata[key] = entry[key]

        valid_locations.append(
            {
                "name": name,
                "deploy_metadata": deploy_metadata,
                "labels": entry.get("labels"),
            }
        )

    logger.info("Loaded %d code location(s) from %s", len(valid_locations), yaml_path)
    return valid_locations


async def upsert_workspace_yaml_locations(
    session_factory, dagster_home: str | None = None
) -> int:
    """Load workspace.yaml and upsert entries into the database.

    Called during webserver startup to sync YAML-defined locations.

    Args:
        session_factory: AsyncSession factory for the database.
        dagster_home: Path to DAGSTER_HOME (optional).

    Returns:
        Number of locations upserted.
    """
    from dagster_webserver.database.agent_queries import upsert_code_location

    locations = load_workspace_yaml(dagster_home)
    if not locations:
        return 0

    count = 0
    async with session_factory() as session:
        for loc in locations:
            await upsert_code_location(
                session,
                location_name=loc["name"],
                deploy_metadata=loc.get("deploy_metadata", {}),
                labels=loc.get("labels"),
            )
            count += 1

    logger.info("Upserted %d location(s) from workspace.yaml", count)
    return count
