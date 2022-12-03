"""Utils to set the SEC API key and scrape an initial dataset."""

import logging
import multiprocessing as mp
import os
import sys

import pandas as pd
from requests.exceptions import HTTPError
from sqlalchemy.exc import IntegrityError

from ..tickers import api as tickers_api
from ..utils import setenv
from .api import api
from .scrape import get_unique_10q
from .sql import engine, metadata
from .sql import tags as tags_table

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | shark.sec.install - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def is_valid_fiscal_seq(seq: list[int]) -> bool:
    """Determine if the sequence of fiscal
    quarter differences is continuous.

    Args:
        seq: Sequence of integers.

    Returns:
        Whether the sequence is valid.

    """
    valid = {(1, 1, 2), (1, 2, 1), (2, 1, 1), (1, 1), (2, 1), (1, 2)}
    for i in range(len(seq) - 1):
        subseq = tuple(seq[i : i + 3])
        if subseq not in valid:
            return False
    return True


def get_valid_concept(
    ticker: str, tag: str, taxonomy: str, units: str, /
) -> None | pd.DataFrame:
    """Return a filtered dataframe if it's valid.

    A valid dataframe will be a continuous reporting of
    SEC filings and not have duplicate data.

    Args:
        ticker: Company SEC ticker.
        tag: SEC XBRL data tag.
        taxonomy: SEC XBRL data taxonomy.
        units: SEC XBRL data units.

    Returns:
        A dataframe or `None` if a valid dataframe doesn't exist.

    """
    try:
        df = api.company_concept.get(tag, ticker=ticker, taxonomy=taxonomy, units=units)
        df = get_unique_10q(df, units=units)
        if len(df.index) == 0:
            return None
        return df
    except (HTTPError, KeyError):
        return None


def search(args: tuple[str, dict[str, str]]) -> tuple[str, str, None | pd.DataFrame]:
    ticker, concept = args
    tag = concept["tag"]
    taxonomy = concept["taxonomy"]
    units = concept["units"]
    return ticker, tag, get_valid_concept(ticker, tag, taxonomy, units)


def install(init_db: bool = True, processes: int = mp.cpu_count() - 1) -> None:
    """Set the `SEC_API_USER_AGENT` environment variable and
    optionally initialize local SQL tables with popular
    ticker data.

    Args:
        init_db: Whether to initialize local SQL tables
            with popular ticker data.

    Returns:
        Mapping of tickers to rows scraped for them.
        Empty if no ticker data is scraped.

    """
    if "SEC_API_USER_AGENT" not in os.environ:
        user_agent = input(
            "Enter your SEC API user agent below.\n\n" "SEC API user agent: "
        ).strip()
        if not user_agent:
            raise RuntimeError("An empty SEC API user agent was given.")
        p = setenv("SEC_API_USER_AGENT", user_agent)
        logger.info(f"SEC API user agent writtern to {p}")
    else:
        logger.info("SEC API user agent already exists in env")
    if init_db:
        metadata.drop_all(engine)
        metadata.create_all(engine)

        tickers = tickers_api.get_ticker_set()
        concepts = [
            {"tag": "AssetsCurrent", "taxonomy": "us-gaap", "units": "USD"},
            {
                "tag": "EarningsPerShareBasic",
                "taxonomy": "us-gaap",
                "units": "USD/shares",
            },
            {"tag": "InventoryNet", "taxonomy": "us-gaap", "units": "USD"},
            {"tag": "LiabilitiesCurrent", "taxonomy": "us-gaap", "units": "USD"},
            {"tag": "NetIncomeLoss", "taxonomy": "us-gaap", "units": "USD"},
            {"tag": "OperatingIncomeLoss", "taxonomy": "us-gaap", "units": "USD"},
            {"tag": "StockholdersEquity", "taxonomy": "us-gaap", "units": "USD"},
        ]
        tickers_to_dfs: dict[str, list[pd.DataFrame]] = {}
        tickers_to_inserts = {}
        tags_to_misses = {}
        searches = []
        skipped_tickers = set()
        with engine.connect() as conn:
            with mp.Pool(processes=processes) as pool:
                for ticker in sorted(tickers):
                    tickers_to_inserts[ticker] = 0
                    tickers_to_dfs[ticker] = []
                    for concept in concepts:
                        tags_to_misses[concept["tag"]] = 0
                        searches.append([ticker, concept])

                for ticker, tag, df in pool.imap_unordered(search, searches):
                    if df is None:
                        if ticker not in skipped_tickers:
                            logger.info(f"Skipping {ticker} due to missing data")
                        skipped_tickers.add(ticker)
                        tags_to_misses[tag] += 1
                        continue

                    tickers_to_dfs[ticker].append(df)
                    if len(tickers_to_dfs[ticker]) == len(concepts):
                        dfs = tickers_to_dfs.pop(ticker)
                        dfs = pd.concat(dfs)
                        try:
                            conn.execute(
                                tags_table.insert(), dfs.to_dict(orient="records")
                            )
                        except IntegrityError:
                            logger.info(
                                f"Skipping {ticker} due to duplicate data "
                                "(some tickers contain duplicate data)"
                            )
                            continue
                        tickers_to_inserts[ticker] += len(dfs.index)
                        logger.info(
                            f"{tickers_to_inserts[ticker]} rows written for {ticker}"
                        )
        logger.info(f"Total rows written: {sum(tickers_to_inserts.values())}")
        logger.info(f"Number of tickers skipped: {len(skipped_tickers)}/{len(tickers)}")
        logger.info(f"Missed tags summary: {tags_to_misses}")
