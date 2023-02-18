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
from . import features as _features
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
                df = _api.company_concept.get(
                    concept["tag"],
                    ticker=ticker,
                    taxonomy=concept["taxonomy"],
                    units=concept["units"],
                )
                df = _features.get_unique_filings(
                    df, form="10-Q", units=concept["units"]
                )
                rowcount = len(df.index)
                if not rowcount:
                    logger.debug(
                        f"Skipping {ticker} concept {concept['tag']} due to "
                        "missing filings"
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
    "--feature",
    "-f",
    type=click.Choice(["quarterly"]),
    multiple=True,
    help="Additional features to install after installing raw SEC data.",
)
@click.option(
    "--processes",
    "-n",
    type=int,
    default=mp.cpu_count() - 1,
    help=(
        "Number of background processes to use for installing features "
        "after installing raw SEC data. Installation of raw SEC data is "
        "limited to one process because of the SEC API's rate limiting."
    ),
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Sets the log level to DEBUG to show installation errors for each ticker.",
)
def install(
    feature: list[str] = [], processes: int = mp.cpu_count() - 1, verbose: bool = False
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
        logger.info("SEC API user agent already exists in the environment")

    _sql.submissions.drop(backend.engine, checkfirst=True)
    _sql.submissions.create(backend.engine)
    _sql.tags.drop(backend.engine, checkfirst=True)
    _sql.tags.create(backend.engine)

    tickers = indices.api.get_ticker_set()
    total_errors = 0
    total_rows = 0
    with tqdm(
        total=len(tickers),
        desc="Installing SEC quarterly features",
        position=0,
        leave=True,
        disable=verbose,
    ) as pbar:
        for ticker in tickers:
            errored, rows = _install_raw_data(ticker)
            total_errors += errored
            total_rows += rows
            pbar.update()

    logger.info(
        f"{pbar.total - total_errors}/{pbar.total} company datasets "
        "sucessfully written"
    )

    if feature:
        features = set(feature)
        for f in features:
            if f == "quarterly":
                total_rows += _features.quarterly.install(processes=processes)

    logger.info(f"{total_rows} total rows written")
    return total_rows
