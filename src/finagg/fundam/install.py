"""Utils to scrape features from multiple data sources."""

import logging
import multiprocessing as mp
import sys

import pandas as pd
import tqdm

from .. import sec, yfinance
from . import features, store

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | finagg.fundamental.install - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def _features_get(ticker: str) -> tuple[str, pd.DataFrame]:
    """Wrapper for feature getter to enable dispatching to
    `multiprocessing.Pool.imap`.

    """
    try:
        df = features.fundam.from_raw(ticker)
    except (IndexError, KeyError) as e:
        logger.debug(f"Skipping {ticker} due to {e}")
        return ticker, pd.DataFrame()
    return ticker, df


def _initialize() -> None:
    """Multiprocessing pool initializer."""
    sec.sql.engine.dispose()
    yfinance.sql.engine.dispose()


def run(processes: int = mp.cpu_count() - 1) -> None:
    """Install fundamental features using pre-scraped Yahoo!
    finance and SEC data.

    Args:
        processes: Number of background processes used to
            install data.

    """
    store.metadata.drop_all(store.engine)
    store.metadata.create_all(store.engine)

    tickers = sec.sql.get_ticker_set()
    tickers_to_inserts = {}
    skipped_tickers = set()
    with mp.Pool(processes=processes, initializer=_initialize) as pool:
        with tqdm.tqdm(
            total=len(tickers),
            desc="Installing fundamental features",
            position=0,
            leave=True,
        ) as pbar:
            for output in pool.imap_unordered(_features_get, tickers):
                pbar.update()
                ticker, df = output
                if len(df.index) > 0:
                    features.fundam.to_refined(ticker, df)
                    tickers_to_inserts[ticker] = len(df.index)
                else:
                    skipped_tickers.add(ticker)
    if not tickers_to_inserts:
        raise RuntimeError(
            "An error occurred when installing fundamental features. "
            "Set the logging mode to debug or use the verbose flag with the CLI for more info."
        )
    logger.info(f"Total feature rows written: {sum(tickers_to_inserts.values())}")
    logger.info(
        "Number of tickers skipped: "
        f"{len(skipped_tickers)}/{len(tickers_to_inserts)}"
    )
    logger.info("Installation complete!")
