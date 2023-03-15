"""CLI and tools for yfinance."""

import logging
import multiprocessing as mp
from typing import Literal

import click

from .. import indices, sec
from . import feat as _feat

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


@click.group(help="Yahoo! finance tools.")
def entry_point() -> None:
    ...


@entry_point.command(
    help=(
        "Drop and recreate tables, and install the recommended "
        "tables into the SQL database."
    ),
)
@click.option(
    "--raw",
    "-r",
    is_flag=True,
    default=False,
    help="Whether to install raw Yahoo! Finance historical price data.",
)
@click.option(
    "--refined",
    "-ref",
    type=click.Choice(["daily"]),
    multiple=True,
    help=(
        "Refined tables to install. This requires raw data to be "
        "installed beforehand using the `--raw` flag or for the "
        "`--raw` flag to be set when this option is provided."
    ),
)
@click.option(
    "--all",
    "-a",
    "all_",
    is_flag=True,
    default=False,
    help="Whether to install all defined tables (including all refined tables).",
)
@click.option(
    "--ticker-set",
    "-ts",
    "ticker_set",
    type=click.Choice(["indices", "sec"]),
    default="indices",
    help=(
        "Set of tickers whose data is attempted to be downloaded and "
        "inserted into the raw SQL tables. 'indices' indicates the set "
        "of tickers from the three most popular indices (DJIA, "
        "Nasdaq 100, and S&P 500). 'sec' indicates all the tickers that "
        "have data available through the SEC API (which is approximately "
        "all publicly-traded US companies). 'indices' will effectively "
        "only attempt to download and install data for relatively "
        "popular and large market cap companies, while 'sec' will "
        "attempt to download and install data for nearly all "
        "publicly-traded US companies. Choosing 'indices' will be fast, "
        "while choosing 'sec' will be slow but will include more diverse data."
    ),
)
@click.option(
    "--processes",
    "-n",
    type=int,
    default=mp.cpu_count() - 1,
    help=("Number of background processes to use for installing refined data. "),
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Sets the log level to DEBUG to show installation errors for each ticker.",
)
def install(
    raw: bool = False,
    refined: list[Literal["daily"]] = [],
    all_: bool = False,
    ticker_set: Literal["indices", "sec"] = "indices",
    processes: int = mp.cpu_count() - 1,
    verbose: bool = False,
) -> int:
    if verbose:
        logger.setLevel(logging.DEBUG)

    total_rows = 0
    if all_ or raw:
        match ticker_set:
            case "indices":
                tickers = indices.api.get_ticker_set()
            case "sec":
                tickers = sec.api.get_ticker_set()

        total_rows += _feat.prices.install(tickers)

    all_refined = set()
    if all_:
        all_refined = {"daily"}
    elif refined:
        all_refined = set(refined)

    if "daily" in all_refined:
        total_rows += _feat.daily.install(tickers=tickers, processes=processes)

    if all_ or all_refined or raw:
        logger.info(f"{total_rows} total rows inserted for {__package__}")
    else:
        logger.info(
            "Skipping installation because no installation options are provided"
        )
    return total_rows
