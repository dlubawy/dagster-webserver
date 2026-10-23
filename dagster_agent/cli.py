"""CLI entry point for the dagster-agent process."""

from __future__ import annotations

import logging
import os

import click


@click.group(name="dagster-agent")
def cli():
    """Dagster agent — manages code locations and processes work requests."""
    pass


@cli.command(name="run", help="Start the Dagster agent process.")
@click.option(
    "--dagster-home",
    type=click.Path(),
    default=None,
    envvar="DAGSTER_HOME",
    help="Path to DAGSTER_HOME directory containing dagster.yaml config.",
)
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error"], case_sensitive=False),
    help="Logging level.",
)
def run(dagster_home: str | None, log_level: str) -> None:
    """Start the agent main loop."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if dagster_home:
        os.environ["DAGSTER_HOME"] = str(dagster_home)

    # Import here to avoid circular imports at module load time
    try:
        from dagster_agent.instance import DagsterAgentInstance
    except ImportError as exc:
        raise click.ClickException(
            f"Failed to import agent instance: {exc}\n"
            "Ensure the agent is properly configured in dagster.yaml."
        ) from exc

    try:
        with DagsterAgentInstance.get() as instance:
            launcher = instance.user_code_launcher
            launcher.start()

            from dagster_agent.agent import DagsterAgent

            with DagsterAgent(instance) as agent:
                agent.run_loop(launcher)
    except KeyboardInterrupt:
        click.echo("\nAgent shutting down.")
    except Exception as exc:
        raise click.ClickException(f"Agent failed: {exc}") from exc


if __name__ == "__main__":
    cli()
