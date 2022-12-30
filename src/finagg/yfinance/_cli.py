"""CLI and tools for yfinance."""

import logging
from typing import Sequence

import click

from . import install as _install
from . import scrape as _scrape
from . import sql


@click.group(help="Yahoo! finance tools.")
def entry_point() -> None:
    ...


@entry_point.command(
    help=(
        "Drop and recreate tables, "
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
    help="Log installation errors for each ticker.",
)
def install(install_features: bool = False, verbose: bool = False) -> None:
    if verbose:
        _install.logger.setLevel(logging.DEBUG)
    _install.run(install_features=install_features)


@entry_point.command(help="List all tickers within the SQL database.")
def ls() -> None:
    print(sorted(sql.get_ticker_set()))


@entry_point.command(help="Scrape a specified ticker history into the SQL database.")
@click.option("--ticker", required=True, multiple=True, help="Ticker to scrape.")
def scrape(ticker: Sequence[str]) -> None:
    _scrape.run(ticker)
