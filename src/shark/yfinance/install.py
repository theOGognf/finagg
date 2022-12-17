"""Utils to scrape an initial stock price historical dataset."""
import logging
import multiprocessing as mp
import sys

import pandas as pd
from sqlalchemy.exc import IntegrityError

from ..tickers import api as tickers_api
from . import api, sql

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | shark.yfinance.install - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def _get(ticker: str) -> tuple[str, pd.DataFrame]:
    """Wrapper for `api.get` to enable dispatching to
    `multiprocessing.Pool.imap`.

    """
    try:
        df = api.get(ticker, interval="1d", period="max")
    except pd.errors.EmptyDataError:
        df = pd.DataFrame()
    return ticker, df


def run(processes: int = mp.cpu_count() - 1) -> None:
    """Initialize local SQL tables with yfinance
    stock price data.

    """
    tickers = tickers_api.get_ticker_set()

    sql.metadata.drop_all(sql.engine)
    sql.metadata.create_all(sql.engine)

    tickers_to_inserts = {}
    skipped_tickers = set()
    with sql.engine.connect() as conn:
        with mp.Pool(processes=processes) as pool:
            for output in pool.imap_unordered(_get, tickers):
                ticker, df = output
                if not len(df.index):
                    skipped_tickers.add(ticker)
                    logger.info(
                        f"Skipping {ticker} due to missing data "
                        "(some tickers may be delisted)"
                    )
                    continue

                try:
                    conn.execute(sql.prices.insert(), df.to_dict(orient="records"))
                except IntegrityError:
                    logger.info(
                        f"Skipping {ticker} due to missing data "
                        "(some tickers may be delisted)"
                    )
                    continue
                tickers_to_inserts[ticker] = len(df.index)
                logger.info(f"{tickers_to_inserts[ticker]} rows written for {ticker}")
    logger.info(f"Total rows written: {sum(tickers_to_inserts.values())}")
    logger.info(f"Number of tickers skipped: {len(skipped_tickers)}/{len(tickers)}")
