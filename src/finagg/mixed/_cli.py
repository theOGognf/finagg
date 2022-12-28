"""CLI and tools for aggregating mixed features."""

import click


@click.group(help="Mixed feature tools.")
def entry_point() -> None:
    ...


@entry_point.command(
    help="Drop and recreate tables, and install features into the SQL database.",
)
def install() -> None:
    from . import install

    install.run()
