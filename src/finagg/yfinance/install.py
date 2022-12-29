"""Utils to scrape an initial stock price historical dataset."""

import logging
import multiprocessing as mp
import sys

import pandas as pd
import tqdm
from sqlalchemy.exc import IntegrityError

from .. import indices
from . import api, features, sql, store

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | finagg.yfinance.install - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def _api_get(ticker: str) -> tuple[str, pd.DataFrame]:
    """Wrapper for `api.get` to enable dispatching to
    `multiprocessing.Pool.imap`.

    """
    try:
        df = api.get(ticker, interval="1d", period="max")
    except pd.errors.EmptyDataError as e:
        logger.debug(f"Skipping {ticker} due to {e}")
        df = pd.DataFrame()
    return ticker, df


def _features_get(ticker: str) -> tuple[str, pd.DataFrame]:
    """Wrapper for feature getter to enable dispatching to
    `multiprocessing.Pool.imap`.

    """
    df = features.daily_features.from_sql(ticker)
    return ticker, df


def run(processes: int = mp.cpu_count() - 1, install_features: bool = False) -> None:
    """Initialize local SQL tables with yfinance
    stock price data.

    Args:
        processes: Number of background processes used to
            scrape data.
        install_features: Whether to install features from
            the scraped data.

    """
    tickers = indices.api.get_ticker_set()
    tickers += {"VOO", "VGT"}

    sql.metadata.drop_all(sql.engine)
    sql.metadata.create_all(sql.engine)

    raw_tickers_to_inserts = {}
    skipped_raw_tickers = set()
    with sql.engine.connect() as conn:
        with mp.Pool(processes=processes) as pool:
            with tqdm.tqdm(
                total=len(tickers),
                desc="Installing raw yfinance data",
                position=0,
                leave=True,
            ) as pbar:
                for output in pool.imap_unordered(_api_get, tickers):
                    pbar.update()
                    ticker, df = output
                    if len(df.index) == 0:
                        skipped_raw_tickers.add(ticker)
                        continue

                    try:
                        conn.execute(sql.prices.insert(), df.to_dict(orient="records"))
                    except IntegrityError:
                        continue
                    raw_tickers_to_inserts[ticker] = len(df.index)
    if not raw_tickers_to_inserts:
        raise RuntimeError(
            "An error occurred when installing Yahoo! finance raw data. "
            "Set the logging mode to debug or use the verbose flag with the CLI for more info."
        )
    logger.info(f"Total rows written: {sum(raw_tickers_to_inserts.values())}")
    logger.info(f"Number of tickers skipped: {len(skipped_raw_tickers)}/{len(tickers)}")

    if install_features:
        store.metadata.drop_all(store.engine)
        store.metadata.create_all(store.engine)

        feature_tickers_to_inserts = {}
        skipped_feature_tickers = set()
        with mp.Pool(processes=processes, initializer=sql.engine.dispose) as pool:
            with tqdm.tqdm(
                total=len(raw_tickers_to_inserts),
                desc="Installing yfinance features",
                position=0,
                leave=True,
            ) as pbar:
                for output in pool.imap_unordered(
                    _features_get, raw_tickers_to_inserts.keys()
                ):
                    pbar.update()
                    ticker, df = output
                    if len(df.index) > 0:
                        features.daily_features.to_store(ticker, df)
                        feature_tickers_to_inserts[ticker] = len(df.index)
                    else:
                        skipped_feature_tickers.add(ticker)
        if not feature_tickers_to_inserts:
            raise RuntimeError(
                "An error occurred when installing Yahoo! finance features. "
                "Set the logging mode to debug or use the verbose flag with the CLI for more info."
            )
        logger.info(f"Total rows written: {sum(feature_tickers_to_inserts.values())}")
        logger.info(
            "Number of tickers skipped: "
            f"{len(skipped_feature_tickers)}/{len(raw_tickers_to_inserts)}"
        )

    logger.info("Installation complete!")
