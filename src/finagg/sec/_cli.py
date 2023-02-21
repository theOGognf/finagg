"""SEC CLI and tools."""

import logging
import multiprocessing as mp
import os

import click
from requests.exceptions import HTTPError
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

from .. import backend, indices, utils
from . import api as _api
from . import feat as _features
from . import sql as _sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def _install_raw_data(ticker: str, /) -> tuple[bool, int]:
    """Helper for creating and inserting data into the SEC raw data
    table.

    No data is inserted if no rows are found.

    Args:
        ticker: Ticker to aggregate data for.

    Returns:
        Whether an error occurred and

    """
    errored = False
    total_rows = 0
    with backend.engine.begin() as conn:
        try:
            rowcount = conn.execute(
                _sql.submissions.insert(),
                _api.submissions.get(ticker=ticker)["metadata"],
            ).rowcount
            if not rowcount:
                logger.debug(f"Skipping {ticker} due to missing metadata")
                return True, 0
            for concept in _features.quarterly.concepts:
                tag = concept["tag"]
                taxonomy = concept["taxonomy"]
                units = concept["units"]
                df = _api.company_concept.get(
                    tag,
                    ticker=ticker,
                    taxonomy=taxonomy,
                    units=units,
                )
                df = _features.get_unique_filings(df, form="10-Q", units=units)
                rowcount = len(df.index)
                if not rowcount:
                    logger.debug(
                        f"Skipping {ticker} concept {tag} due to missing filings"
                    )
                    errored = True
                    continue
                conn.execute(_sql.tags.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
                total_rows += rowcount
        except (HTTPError, IntegrityError, KeyError) as e:
            logger.debug(f"Skipping {ticker} due to {e}")
            return True, total_rows
    logger.debug(f"{total_rows} total rows written for {ticker}")
    return errored, total_rows


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
    "--feature",
    "-f",
    type=click.Choice(["quarterly", "relative-quarterly"]),
    multiple=True,
    help=(
        "Feature tables to install. This requires raw SEC data to be "
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
    help="Whether to install all defined tables (including all feature tables).",
)
@click.option(
    "--processes",
    "-n",
    type=int,
    default=mp.cpu_count() - 1,
    help=(
        "Number of background processes to use for installing features. "
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
    feature: list[str] = [],
    all_: bool = False,
    processes: int = mp.cpu_count() - 1,
    verbose: bool = False,
) -> int:
    if verbose:
        logger.setLevel(logging.DEBUG)

    if "SEC_API_USER_AGENT" not in os.environ:
        user_agent = input(
            "Enter your SEC API user agent below.\n\n" "SEC API user agent: "
        ).strip()
        if not user_agent:
            raise RuntimeError("An empty SEC API user agent was given.")
        p = utils.setenv("SEC_API_USER_AGENT", user_agent)
        logger.info(f"SEC API user agent written to {p}")
    else:
        logger.info("SEC API user agent found in the environment")

    total_rows = 0
    if all_ or raw:
        _sql.submissions.drop(backend.engine, checkfirst=True)
        _sql.submissions.create(backend.engine)
        _sql.tags.drop(backend.engine, checkfirst=True)
        _sql.tags.create(backend.engine)

        tickers = indices.api.get_id_set()
        total_errors = 0
        with tqdm(
            total=len(tickers),
            desc="Installing raw quarterly SEC data",
            position=0,
            leave=True,
            disable=verbose,
        ) as pbar:
            for ticker in tickers:
                errored, rowcount = _install_raw_data(ticker)
                total_errors += errored
                total_rows += rowcount
                pbar.update()

        logger.info(
            f"{pbar.total - total_errors}/{pbar.total} company datasets "
            "sucessfully written"
        )

    features = set()
    if all_:
        features = {"quarterly", "relative-quarterly"}
    elif feature:
        features = set(feature)

    if "quarterly" in features:
        total_rows += _features.quarterly.install(processes=processes)

    if "relative-quarterly" in features:
        total_rows += _features.relative_quarterly.install(processes=processes)

    if all_ or features or raw:
        logger.info(f"{total_rows} total rows written")
    else:
        logger.info(
            "Skipping installation because no installation options are provided"
        )
    return total_rows
