"""SEC CLI and tools."""

import logging
import multiprocessing as mp
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
    is_flag=True,
    default=False,
    help="Whether to install raw SEC data.",
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
    help=(
        "Number of background processes to use for installing refined data. "
        "Installation of raw SEC data is limited to one process because "
        "the SEC rate-limits its API."
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
    raw: bool = False,
    refined: list[Literal["quarterly", "quarterly.normalized"]] = [],
    all_: bool = False,
    ticker_set: Literal["indices", "sec"] = "indices",
    processes: int = mp.cpu_count() - 1,
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
    if all_ or raw:
        match ticker_set:
            case "indices":
                tickers = indices.api.get_ticker_set()
            case "sec":
                tickers = _api.get_ticker_set()

        total_rows += _feat.submissions.install(tickers)
        total_rows += _feat.tags.install(tickers)

    all_refined = set()
    if all_:
        all_refined = {"quarterly", "quarterly.normalized"}
    elif refined:
        all_refined = set(refined)

    if "quarterly" in all_refined:
        total_rows += _feat.quarterly.install(processes=processes)

    if "quarterly.normalized" in all_refined:
        total_rows += _feat.quarterly.normalized.install(processes=processes)

    if all_ or all_refined or raw:
        logger.info(f"{total_rows} total rows inserted for {__package__}")
    else:
        logger.info(
            "Skipping installation because no installation options are provided"
        )
    return total_rows
