"""CLI and tools for yfinance."""

import logging
from typing import Literal

import click

from .. import indices, sec, utils
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
    type=click.Choice(["prices"]),
    multiple=True,
    help=(
        "Raw tables to install. `prices` indicates daily historical stock "
        "price data. `prices` must be specified to enable installing refined "
        "data using the `refined` flag."
    ),
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
    "--ticker",
    "-t",
    multiple=True,
    help=(
        "Ticker whose data is attempted to be downloaded and inserted into "
        "the SQL tables. Multiple tickers can be specified by providing "
        "multiple `ticker` options, by separating tickers with a comma (e.g., "
        "`AAPL,MSFT,NVDA`), or by providing tickers in a CSV file by "
        "specifying a file path (e.g., `bank_tickers.txt`). The CSV file "
        "can be formatted such that there's one ticker per line or multiple "
        "tickers per line (delimited by a comma). The tickers specified "
        "by this option are combined with the tickers specified by the "
        "`ticker-set` option."
    ),
)
@click.option(
    "--ticker-set",
    "-ts",
    "ticker_set",
    type=click.Choice(["indices", "sec"]),
    default=None,
    help=(
        "Set of tickers whose data is attempted to be downloaded and "
        "inserted into the SQL tables. 'indices' indicates the set "
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
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Sets the log level to DEBUG to show installation errors for each ticker.",
)
def install(
    raw: list[Literal["prices"]] = [],
    refined: list[Literal["daily"]] = [],
    all_: bool = False,
    ticker: list[str] = [],
    ticker_set: None | Literal["indices", "sec"] = None,
    verbose: bool = False,
) -> int:
    if verbose:
        logger.setLevel(logging.DEBUG)

    total_rows = 0
    all_raw = set()
    if all_:
        all_raw = {"prices"}
    elif raw:
        all_raw = set(raw)

    all_tickers = utils.expand_csv(ticker)
    if all_raw:
        match ticker_set:
            case "indices":
                all_tickers |= indices.api.get_ticker_set()
            case "sec":
                all_tickers |= sec.api.get_ticker_set()

        if not all_tickers:
            logger.info(
                f"Skipping {__package__} installation because no tickers were "
                "provided (by the `ticker` option or by the `ticker-set` option)"
            )
            return total_rows

        if "prices" in all_raw:
            total_rows += _feat.prices.install(all_tickers)

    all_refined = set()
    if all_:
        all_refined = {"daily"}
    elif refined:
        all_refined = set(refined)

    if "daily" in all_refined:
        total_rows += _feat.daily.install(tickers=all_tickers)

    if all_ or all_refined or all_raw:
        if total_rows:
            logger.info(f"{total_rows} total rows inserted for {__package__}")
        else:
            logger.warning(
                f"No rows were inserted for {__package__}. This is likely an "
                "error. Set the verbose flag with the `--verbose/-v` option "
                "to enable debug logging."
            )
    else:
        logger.info(
            f"Skipping {__package__} installation because no installation "
            "options are provided"
        )
    return total_rows
