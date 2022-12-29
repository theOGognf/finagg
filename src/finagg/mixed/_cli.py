"""CLI and tools for aggregating mixed features."""

import click

from . import install as _install


@click.group(help="Mixed feature tools.")
def entry_point() -> None:
    ...


@entry_point.command(
    help="Drop and recreate tables, and install features into the SQL database.",
)
def install() -> None:
    _install.run()
