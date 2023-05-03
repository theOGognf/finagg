"""CLI and tools for aggregating fundamental features."""

import logging
import os
from typing import Literal

import click

from . import feat as _feat

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


@click.group(help="Fundamental feature tools.")
def entry_point() -> None:
    ...


@entry_point.command(
    help=(
        "Drop and recreate tables, and install recommended tables into the "
        "SQL database."
    ),
)
@click.option(
    "--refined",
    type=click.Choice(["fundam", "fundam.normalized"]),
    multiple=True,
    help=(
        "Refined tables to install. This requires Yahoo! Finance and "
        "SEC refined tables to be installed beforehand."
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
    help="Sets the log level to DEBUG to show installation errors for each series.",
)
def install(
    refined: list[Literal["fundam", "fundam.normalized"]] = [],
    all_: bool = False,
    recreate_tables: bool = False,
    verbose: bool = False,
) -> int:
    if verbose:
        logging.getLogger(__package__).setLevel(logging.DEBUG)

    if "SEC_API_USER_AGENT" not in os.environ:
        logger.warning(
            "No SEC API user agent found in the environment. "
            "Skipping finagg.fundam installation."
        )
        return 0

    total_rows = 0
    all_refined = set()
    if all_:
        all_refined = {"fundam", "fundam.normalized"}
    elif refined:
        all_refined = set(refined)

    if "fundam" in all_refined:
        total_rows += _feat.fundam.install(recreate_tables=recreate_tables)

    if "fundam.normalized" in all_refined:
        total_rows += _feat.fundam.normalized.install(recreate_tables=recreate_tables)

    if all_ or all_refined:
        if total_rows:
            logger.info(f"{total_rows} total rows inserted for {__package__}")
        else:
            logger.warning(
                f"No rows were inserted for {__package__}. This could be an error if"
                " installations were not skipped. Set the verbose flag with the"
                " `--verbose/-v` option to enable debug logging."
            )
    else:
        logger.info(
            f"Skipping {__package__} installation because no installation "
            "options are provided"
        )
    return total_rows
