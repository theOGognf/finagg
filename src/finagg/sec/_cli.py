"""SEC CLI and tools."""

import logging
import multiprocessing as mp
import os
from typing import Literal

import click

from .. import utils
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
    type=click.Choice(
        ["annual", "annual.normalized", "quarterly", "quarterly.normalized"]
    ),
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
    type=click.Choice(["sec"]),
    default=None,
    help=(
        "Set of tickers whose data is attempted to be downloaded and "
        "inserted into the SQL tables. 'sec' indicates all the tickers that "
        "have data available through the SEC API (which is approximately "
        "all publicly-traded US companies)."
    ),
)
@click.option(
    "--from-zip",
    "-z",
    is_flag=True,
    default=False,
    help=(
        "Whether to install raw data from bulk data zip files that're compiled by the"
        " SEC nightly. Installing all SEC data with this option can take upwards of 1.5"
        " hours to complete."
    ),
)
@click.option(
    "--processes",
    "-n",
    type=int,
    default=mp.cpu_count() - 1,
    help=(
        "Number of backgruond processes to run in parallel when installing data. Note,"
        " not all tables support installations with multiprocessing."
    ),
)
@click.option(
    "--recreate-tables",
    "-r",
    is_flag=True,
    default=False,
    help=(
        "Whether to reset the tables associated with the install options by "
        "dropping and recreating them."
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
    ticker_set: None | Literal["sec"] = None,
    from_zip: bool = False,
    processes: int = mp.cpu_count() - 1,
    recreate_tables: bool = False,
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
            case "sec":
                all_tickers |= _api.get_ticker_set()

        if not all_tickers:
            logger.info(
                f"Skipping {__package__} installation because no tickers were "
                "provided (by the `ticker` option or by the `ticker-set` option)"
            )
            return total_rows

        if "submissions" in all_raw:
            if from_zip:
                total_rows += _feat.submissions.install_from_zip(
                    all_tickers, recreate_tables=recreate_tables
                )
            else:
                total_rows += _feat.submissions.install(
                    all_tickers, recreate_tables=recreate_tables
                )

        if "tags" in all_raw:
            if from_zip:
                total_rows += _feat.tags.install_from_zip(
                    all_tickers, processes=processes, recreate_tables=recreate_tables
                )
            else:
                total_rows += _feat.tags.install(
                    all_tickers, recreate_tables=recreate_tables
                )

    all_refined = set()
    if all_:
        all_refined = {
            "annual",
            "annual.normalized",
            "quarterly",
            "quarterly.normalized",
        }
    elif refined:
        all_refined = set(refined)

    if "annual" in all_refined:
        total_rows += _feat.annual.install(
            tickers=all_tickers, processes=processes, recreate_tables=recreate_tables
        )

    if "annual.normalized" in all_refined:
        total_rows += _feat.annual.normalized.install(
            tickers=all_tickers, processes=processes, recreate_tables=recreate_tables
        )

    if "quarterly" in all_refined:
        total_rows += _feat.quarterly.install(
            tickers=all_tickers, processes=processes, recreate_tables=recreate_tables
        )

    if "quarterly.normalized" in all_refined:
        total_rows += _feat.quarterly.normalized.install(
            tickers=all_tickers, processes=processes, recreate_tables=recreate_tables
        )

    if all_ or all_refined or all_raw:
        if total_rows:
            logger.info(f"{total_rows} total rows inserted for {__package__}")
        else:
            logger.warning(
                f"No rows were inserted for {__package__}. This could be an error if"
                " installations were skipped. Set the verbose flag with the"
                " `--verbose/-v` option to enable debug logging."
            )
    else:
        logger.info(
            f"Skipping {__package__} installation because no installation "
            "options were provided"
        )
    return total_rows
