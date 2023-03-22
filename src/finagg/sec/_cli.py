"""SEC CLI and tools."""

import logging
import os
from typing import Literal

import click

from .. import indices, utils
from . import api as _api
from . import feat as _feat

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


@click.group(help="Securities and Exchange Commission (SEC) tools.")
def entry_point() -> None:
    ...


@entry_point.command(
    help=(
        "Set the SEC API key, drop and recreate tables, "
        "and install the recommended tables into the SQL database."
    ),
)
@click.option(
    "--raw",
    "-r",
    type=click.Choice(["submissions", "tags"]),
    multiple=True,
    help=(
        "Raw tables to install. `submissions` indicates company metadata "
        "(e.g., company name, industry code, etc.) while `tags` indicates "
        "SEC EDGAR tags (e.g., earnings-per-share, current assets, etc.)."
        "Both `submissions` and `tags` must be specified to enable installing "
        "refined data using the `refined` flag."
    ),
)
@click.option(
    "--refined",
    "-ref",
    type=click.Choice(["quarterly", "quarterly.normalized"]),
    multiple=True,
    help=(
        "Refined tables to install. This requires raw SEC data to be "
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
    raw: list[Literal["submissions", "tags"]] = [],
    refined: list[Literal["quarterly", "quarterly.normalized"]] = [],
    all_: bool = False,
    ticker: list[str] = [],
    ticker_set: None | Literal["indices", "sec"] = None,
    verbose: bool = False,
) -> int:
    if verbose:
        logging.getLogger(__package__).setLevel(logging.DEBUG)

    if "SEC_API_USER_AGENT" not in os.environ:
        user_agent = input(
            "Enter your SEC API user agent below (this should be of format "
            "'FIRST_NAME LAST_NAME E_MAIL').\n\n"
            "SEC API user agent (leave blank and hit ENTER to skip): "
        ).strip()
        if not user_agent:
            logger.warning(
                "An empty SEC API user agent was given. Skipping finagg.sec "
                "installation."
            )
            return 0
        p = utils.setenv("SEC_API_USER_AGENT", user_agent)
        logger.info(f"SEC API user agent written to {p}")
    else:
        logger.info("SEC API user agent found in the environment")

    total_rows = 0
    all_raw = set()
    if all_:
        all_raw = {"submissions", "tags"}
    elif raw:
        all_raw = set(raw)

    all_tickers = utils.expand_csv(ticker)
    if all_raw:
        match ticker_set:
            case "indices":
                all_tickers |= indices.api.get_ticker_set()
            case "sec":
                all_tickers |= _api.get_ticker_set()

        if not all_tickers:
            logger.info(
                f"Skipping {__package__} installation because no tickers were "
                "provided (by the `ticker` option or by the `ticker-set` option)"
            )
            return total_rows

        if "submissions" in all_raw:
            total_rows += _feat.submissions.install(all_tickers)

        if "tags" in all_raw:
            total_rows += _feat.tags.install(all_tickers)

    all_refined = set()
    if all_:
        all_refined = {"quarterly", "quarterly.normalized"}
    elif refined:
        all_refined = set(refined)

    if "quarterly" in all_refined:
        total_rows += _feat.quarterly.install(tickers=all_tickers)

    if "quarterly.normalized" in all_refined:
        total_rows += _feat.quarterly.normalized.install(tickers=all_tickers)

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
            "options were provided"
        )
    return total_rows
