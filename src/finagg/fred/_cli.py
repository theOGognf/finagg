"""FRED CLI and tools."""

import logging
import os

import click
from requests.exceptions import HTTPError
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

from .. import backend, utils
from . import api as _api
from . import feat as _feat
from . import sql as _sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def _install_raw_data(series_id: str, /) -> tuple[bool, int]:
    """Helper for getting raw SEC economic data.

    Args:
        series_id: Series to aggregate data for.

    Returns:
        Whether an error occurred and the total rows inserted for the series.

    """
    errored = False
    total_rows = 0
    with backend.engine.begin() as conn:
        try:
            df = _api.series.observations.get(
                series_id,
                realtime_start=0,
                realtime_end=-1,
                output_type=4,
            )
            rowcount = len(df.index)
            if not rowcount:
                logger.debug(f"Skipping {series_id} due to missing data")
                return True, 0

            conn.execute(_sql.series.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
            total_rows += rowcount
        except (HTTPError, IntegrityError) as e:
            logger.debug(f"Skipping {series_id} due to {e}")
            return True, total_rows
    logger.debug(f"{total_rows} total rows inserted for {series_id}")
    return errored, total_rows


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
    refined: list[str] = [],
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
            "FRED API key: "
        ).strip()
        if not api_key:
            raise RuntimeError("An empty FRED API key was given.")
        p = utils.setenv("FRED_API_KEY", api_key)
        logger.info(f"FRED API key written to {p}")
    else:
        logger.info("FRED API key found in the environment")

    total_rows = 0
    if all_ or raw:
        _sql.series.drop(backend.engine, checkfirst=True)
        _sql.series.create(backend.engine)

        total_errors = 0
        with tqdm(
            total=len(_feat.economic.series_ids),
            desc="Installing raw FRED economic data",
            position=0,
            leave=True,
            disable=verbose,
        ) as pbar:
            for series_id in _feat.economic.series_ids:
                errored, rowcount = _install_raw_data(series_id)
                total_errors += errored
                total_rows += rowcount
                pbar.update()

        logger.info(
            f"{pbar.total - total_errors}/{pbar.total} FRED series datasets "
            "sucessfully inserted"
        )

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
            "Skipping installation because no installation options are provided"
        )
    return total_rows
