"""CLI and tools for aggregating mixed features."""

import logging

import click

from . import install as _install


@click.group(help="Mixed feature tools.")
def entry_point() -> None:
    ...


@entry_point.command(
    help="Drop and recreate tables, and install features into the SQL database.",
)
@click.option(
    "-v/--verbose",
    is_flag=True,
    default=False,
    help="Log installation errors for each series.",
)
def install(verbose: bool = False) -> None:
    if verbose:
        _install.logger.setLevel(logging.DEBUG)
    _install.run()
