"""FRED CLI and tools."""

import logging
from typing import Sequence

import click

from . import install as _install
from . import scrape as _scrape
from . import sql


@click.group(help="Federal Reserve Economic Data (FRED) tools.")
def entry_point() -> None:
    ...


@entry_point.command(
    help=(
        "Set the FRED API key, drop and recreate tables, "
        "and scrape the recommended datasets and features into the SQL database."
    ),
)
@click.option(
    "--install-features",
    is_flag=True,
    default=False,
    help="Whether to install features with the recommended datasets.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Log installation errors for each series.",
)
def install(install_features: bool = False, verbose: bool = False) -> None:
    if verbose:
        _install.logger.setLevel(logging.DEBUG)
    _install.run(install_features=install_features)


@entry_point.command(help="List all economic data series within the SQL database.")
def ls() -> None:
    print(sorted(sql.get_series_set()))


@entry_point.command(
    help="Scrape a specified economic data series into the SQL database.",
)
@click.option("--series", required=True, multiple=True, help="Series IDs to scrape.")
def scrape(series: Sequence[str]) -> None:
    _scrape.run(series)
