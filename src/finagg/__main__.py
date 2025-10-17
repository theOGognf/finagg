"""Main CLI entry points."""

import datetime
import logging
import multiprocessing as mp
import os
import pathlib
import time
from typing import Literal

import click

from . import bea, fred, sec, utils

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


@click.group()
def cli() -> None:
    ...


cli.add_command(bea._cli.entry_point, "bea")
cli.add_command(fred._cli.entry_point, "fred")
cli.add_command(sec._cli.entry_point, "sec")


@cli.command(
    help=(
        "Set API keys/user agents, drop and recreate tables, "
        "and install the recommended datasets into the SQL database."
    ),
)
@click.option(
    "--skip",
    "-s",
    type=click.Choice(["bea", "fred", "sec"]),
    multiple=True,
    help=(
        "Subpackage installations to skip. Useful to avoid reinstalling data "
        "that you don't want to when using the general install command."
    ),
)
@click.option(
    "--series",
    "-sid",
    multiple=True,
    help=(
        "FRED economic series whose data is attempted to be downloaded and "
        "inserted into the SQL tables. Multiple series can be specified by "
        "providing multiple `series` options, by separating series with a "
        "comma (e.g., `GDP,FEDFUNDS`), or by providing IDs in a CSV file by "
        "specifying a file path (e.g., `fred_series.txt`). The CSV file "
        "can be formatted such that there's one string per line or multiple "
        "strings per line (delimited by a comma). The strings specified "
        "by this option are combined with the strings specified by the "
        "`series-set` option."
    ),
)
@click.option(
    "--series-set",
    "-ss",
    "series_set",
    type=click.Choice(["economic"]),
    default=None,
    help=(
        "Set of FRED economic series whose data is attempted to be downloaded "
        "and inserted into the SQL tables. 'economic' indicates the recommended "
        "and most popular series (e.g., consumer price index, gross domestic "
        "product, etc.)."
    ),
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
@click.pass_context
def install(
    ctx: click.Context,
    skip: list[str] = [],
    series: list[str] = [],
    series_set: None | Literal["economic"] = None,
    ticker: list[str] = [],
    ticker_set: None | Literal["sec"] = None,
    from_zip: bool = False,
    processes: int = mp.cpu_count() - 1,
    recreate_tables: bool = False,
    verbose: bool = False,
) -> None:
    if "FINAGG_ROOT_PATH" not in os.environ:
        entered_root_path = input(
            "Enter the path to your finagg root directory. finagg data will "
            "be written to /path/to/root/findata/.\n\n"
            "Root path (hit ENTER to use your current working directory): "
        ).strip()
        if not entered_root_path:
            root_path = pathlib.Path.cwd()
        else:
            root_path = pathlib.Path(entered_root_path).expanduser().resolve()
        p = utils.setenv("FINAGG_ROOT_PATH", str(root_path))
        logger.info(f"FINAGG_ROOT_PATH written to {p}")
    else:
        logger.info("FINAGG_ROOT_PATH found in the environment")

    start = time.monotonic()

    all_skips = set(skip)
    if "bea" not in all_skips:
        ctx.invoke(bea._cli.install)

    if "fred" not in all_skips:
        ctx.invoke(
            fred._cli.install,
            all_=True,
            series=series,
            series_set=series_set,
            recreate_tables=recreate_tables,
            verbose=verbose,
        )

    if "sec" not in all_skips:
        ctx.invoke(
            sec._cli.install,
            all_=True,
            ticker=ticker,
            ticker_set=ticker_set,
            from_zip=from_zip,
            processes=processes,
            recreate_tables=recreate_tables,
            verbose=verbose,
        )

    td = datetime.timedelta(seconds=int(time.monotonic() - start))
    logger.info(f"Installation took {td}")


def main() -> int:
    """Create and run parsers according to the given commands."""
    cli()
    return 0


if __name__ == "__main__":
    SystemExit(main())
