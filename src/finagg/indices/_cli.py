"""CLI and tools for aggregating tickers in common indices."""

import click


@click.group(help="Tools for managing index data.")
def entry_point() -> None:
    ...


@entry_point.command(
    help="Drop and recreate tables, " "and scrape tickers into the SQL database.",
)
def install() -> None:
    from . import install

    install.run()
