"""FRED CLI and tools."""

import logging
import os
from typing import Literal

import click

from .. import utils
from . import feat as _feat

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


@click.group(help="Federal Reserve Economic Data (FRED) tools.")
def entry_point() -> None:
    ...


@entry_point.command(
    help=(
        "Set the FRED API key, drop and recreate tables, "
        "and install the recommended tables into the SQL database."
    ),
)
@click.option(
    "--raw",
    "-r",
    type=click.Choice(["series"]),
    multiple=True,
    help=(
        "Raw tables to install. `series` indicates raw FRED economic series "
        "observations/measurements (e.g., consumer price index, gross domestic "
        "product, etc.). At least `series` must be specified to enable "
        "installing refined data using the `refined` flag."
    ),
)
@click.option(
    "--refined",
    "-ref",
    type=click.Choice(["economic", "economic.normalized"]),
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
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Sets the log level to DEBUG to show installation errors for each series.",
)
def install(
    raw: list[Literal["series"]] = [],
    refined: list[Literal["economic", "economic.normalized"]] = [],
    all_: bool = False,
    series: list[str] = [],
    series_set: None | Literal["economic"] = None,
    verbose: bool = False,
) -> int:
    if verbose:
        logging.getLogger(__package__).setLevel(logging.DEBUG)

    if "FRED_API_KEY" not in os.environ:
        api_key = input(
            "Enter your FRED API key below.\n\n"
            "You can request a FRED API key at\n"
            "https://fred.stlouisfed.org/docs/api/api_key.html.\n\n"
            "FRED API key (leave blank and hit ENTER to skip): "
        ).strip()
        if not api_key:
            logger.warning(
                "An empty FRED API key was given. Skipping finagg.fred installation."
            )
            return 0
        p = utils.setenv("FRED_API_KEY", api_key)
        logger.info(f"FRED API key written to {p}")
    else:
        logger.info("FRED API key found in the environment")

    total_rows = 0
    all_raw = set()
    if all_:
        all_raw = {"series"}
    elif raw:
        all_raw = set(raw)

    all_series = utils.expand_csv(series)
    if all_raw:
        match series_set:
            case "economic":
                all_series |= set(_feat.economic.series_ids)

        if not all_series:
            logger.info(
                f"Skipping {__package__} installation because no series were "
                "provided (by the `series` option or by the `series-set` option)"
            )
            return total_rows

    if "series" in all_raw:
        total_rows += _feat.series.install(all_series)

    all_refined = set()
    if all_:
        all_refined = {"economic", "economic.normalized"}
    elif refined:
        all_refined = set(refined)

    if "economic" in all_refined:
        total_rows += _feat.economic.install()

    if "economic.normalized" in all_refined:
        total_rows += _feat.economic.normalized.install()

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
