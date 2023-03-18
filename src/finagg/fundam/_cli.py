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
    "-ref",
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
def install(
    refined: list[Literal["fundam", "fundam.normalized"]] = [],
    all_: bool = False,
) -> int:
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
        total_rows += _feat.fundam.install()

    if "fundam.normalized" in all_refined:
        total_rows += _feat.fundam.normalized.install()

    if all_ or all_refined:
        logger.info(f"{total_rows} total rows inserted for {__package__}")
    else:
        logger.info(
            f"Skipping {__package__} installation because no installation "
            "options are provided"
        )
    return total_rows
