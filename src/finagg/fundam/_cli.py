"""CLI and tools for aggregating fundamental features."""

import logging
import multiprocessing as mp

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
@click.option(
    "--processes",
    "-n",
    type=int,
    default=mp.cpu_count() - 1,
    help="Number of background processes to use for installing refined data.",
)
def install(
    refined: list[str] = [],
    all_: bool = False,
    processes: int = mp.cpu_count() - 1,
) -> int:
    total_rows = 0
    all_refined = set()
    if all_:
        all_refined = {"fundam", "fundam.normalized"}
    elif refined:
        all_refined = set(refined)

    if "fundam" in all_refined:
        total_rows += _feat.fundam.install(processes=processes)

    if "fundam.normalized" in all_refined:
        total_rows += _feat.fundam.normalized.install(processes=processes)

    if all_ or all_refined:
        logger.info(f"{total_rows} total rows inserted for {__package__}")
    else:
        logger.info(
            "Skipping installation because no installation options are provided"
        )
    return total_rows
