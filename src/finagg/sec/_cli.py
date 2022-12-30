"""SEC CLI and tools."""

import logging
from typing import Sequence

import click

from . import features
from . import install as _install
from . import scrape as _scrape
from . import sql


@click.group()
def entry_point() -> None:
    ...


@entry_point.command(
    help=(
        "Set the SEC API key, drop and recreate tables, "
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


@entry_point.command(
    help="Scrape a specified ticker quarterly report into the SQL database."
)
@click.option("--ticker", required=True, multiple=True, help="Ticker to scrape.")
@click.option("--tag", default=None, help="XBRL tag to scrape.")
@click.option("--taxonomy", default=None, help="XBRL taxonomy to scrape.")
@click.option("--units", default=None, help="Units to scrape.")
def scrape(ticker: Sequence[str], tag: str, taxonomy: str, units: str) -> None:
    if tag and taxonomy and units:
        concepts = [{"tag": tag, "taxonomy": taxonomy, "units": units}]
    else:
        concepts = list(features.quarterly_features.concepts)
    _scrape.run(ticker, concepts=concepts)
