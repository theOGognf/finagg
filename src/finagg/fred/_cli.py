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
    is_flag=True,
    default=False,
    help="Whether to install raw FRED series data.",
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
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Sets the log level to DEBUG to show installation errors for each series.",
)
def install(
    raw: bool = False,
    refined: list[Literal["economic", "economic.normalized"]] = [],
    all_: bool = False,
    verbose: bool = False,
) -> int:
    if verbose:
        logger.setLevel(logging.DEBUG)

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
    if all_ or raw:
        _feat.series.install()

    all_refined = set()
    if all_:
        all_refined = {"economic", "economic.normalized"}
    elif refined:
        all_refined = set(refined)

    if "economic" in all_refined:
        total_rows += _feat.economic.install()

    if "economic.normalized" in all_refined:
        total_rows += _feat.economic.normalized.install()

    if all_ or all_refined or raw:
        logger.info(f"{total_rows} total rows inserted for {__package__}")
    else:
        logger.info(
            f"Skipping {__package__} installation because no installation "
            "options are provided"
        )
    return total_rows
