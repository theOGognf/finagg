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


def _get_valid_concept(
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


def _search(params: dict[str, str]) -> dict:
    """Wrapper for `_get_valid_concept` to enable dispatching
    to `multiprocessing.Pool.imap`.

    """
    ticker = params["ticker"]
    tag = params["tag"]
    taxonomy = params["taxonomy"]
    units = params["units"]
    return {
        "ticker": ticker,
        "tag": tag,
        "result": _get_valid_concept(ticker, tag, taxonomy, units),
    }


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
                        search = concept.copy()
                        search["ticker"] = ticker
                        tags_to_misses[concept["tag"]] = 0
                        searches.append(search)

                for output in pool.imap_unordered(_search, searches):
                    ticker = output["ticker"]
                    tag = output["tag"]
                    df = output["result"]
                    if df is None:
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
