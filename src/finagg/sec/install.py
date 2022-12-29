"""Utils to set the SEC API key and scrape an initial dataset."""

import logging
import multiprocessing as mp
import os
import sys

import pandas as pd
import tqdm
from requests.exceptions import HTTPError
from sqlalchemy.exc import IntegrityError

from .. import indices, utils
from . import api, features, sql, store

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | finagg.sec.install - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def _features_get(ticker: str) -> tuple[str, pd.DataFrame]:
    """Wrapper for feature getter to enable dispatching to
    `multiprocessing.Pool.imap`.

    """
    df = features.quarterly_features.from_sql(ticker)
    return ticker, df


def _get_valid_concept(
    ticker: str, tag: str, taxonomy: str, units: str, /
) -> pd.DataFrame:
    """Return a filtered dataframe.

    A valid dataframe will be a continuous reporting of
    SEC filings and not have duplicate data.

    Args:
        ticker: Company SEC ticker.
        tag: SEC XBRL data tag.
        taxonomy: SEC XBRL data taxonomy.
        units: SEC XBRL data units.

    Returns:
        A dataframe with unique form 10-Q rows or
        an empty dataframe if no rows exist.

    """
    try:
        df = api.company_concept.get(tag, ticker=ticker, taxonomy=taxonomy, units=units)
        df = features.get_unique_10q(df, units=units)
        if len(df.index) == 0:
            logger.debug(f"Skipping {ticker} due to missing data")
            return pd.DataFrame()
        return df
    except (HTTPError, KeyError) as e:
        logger.debug(f"Skipping {ticker} due to {e}")
        return pd.DataFrame()


def run(processes: int = mp.cpu_count() - 1, install_features: bool = False) -> None:
    """Set the `SEC_API_USER_AGENT` environment variable and
    optionally initialize local SQL tables with popular
    ticker data.

    Args:
        processes: Number of background processes to
            use to scrape data.
        install_features: Whether to install features from
            the scraped data.

    """
    if "SEC_API_USER_AGENT" not in os.environ:
        user_agent = input(
            "Enter your SEC API user agent below.\n\n" "SEC API user agent: "
        ).strip()
        if not user_agent:
            raise RuntimeError("An empty SEC API user agent was given.")
        p = utils.setenv("SEC_API_USER_AGENT", user_agent)
        logger.info(f"SEC API user agent writtern to {p}")
    else:
        logger.info("SEC API user agent already exists in env")
    sql.metadata.drop_all(sql.engine)
    sql.metadata.create_all(sql.engine)

    tickers = indices.api.get_ticker_set()
    concepts = features.quarterly_features.concepts
    total_searches = len(tickers) * len(concepts)
    tickers_to_dfs: dict[str, list[pd.DataFrame]] = {}
    raw_tickers_to_inserts = {}
    tags_to_misses = {c["tag"]: 0 for c in concepts}
    skipped_raw_tickers = set()
    with sql.engine.connect() as conn:
        with tqdm.tqdm(
            total=total_searches, desc="Installing raw SEC data", position=0, leave=True
        ) as pbar:
            for ticker in tickers:
                tickers_to_dfs[ticker] = []
                for concept in concepts:
                    pbar.update()
                    tag = concept["tag"]
                    taxonomy = concept["taxonomy"]
                    units = concept["units"]
                    df = _get_valid_concept(ticker, tag, taxonomy, units)
                    if len(df.index) == 0:
                        skipped_raw_tickers.add(ticker)
                        tags_to_misses[tag] += 1
                        break

                    tickers_to_dfs[ticker].append(df)

                if len(tickers_to_dfs[ticker]) == len(concepts):
                    dfs = tickers_to_dfs.pop(ticker)
                    df = pd.concat(dfs)
                    try:
                        conn.execute(sql.tags.insert(), df.to_dict(orient="records"))
                        raw_tickers_to_inserts[ticker] = len(df.index)
                    except IntegrityError:
                        continue
    if not raw_tickers_to_inserts:
        raise RuntimeError(
            "An error occurred when installing SEC raw data. "
            "Set the logging mode to debug or use the verbose flag with the CLI for more info."
        )
    logger.info(f"Total raw rows written: {sum(raw_tickers_to_inserts.values())}")
    logger.info(f"Number of tickers skipped: {len(skipped_raw_tickers)}/{len(tickers)}")
    logger.info(f"Missed tags summary: {tags_to_misses}")

    if install_features:
        store.metadata.drop_all(store.engine)
        store.metadata.create_all(store.engine)

        feature_tickers_to_inserts = {}
        skipped_feature_tickers = set()
        with store.engine.connect() as conn:
            with mp.Pool(processes=processes, initializer=sql.engine.dispose) as pool:
                with tqdm.tqdm(
                    total=len(raw_tickers_to_inserts),
                    desc="Installing SEC features",
                    position=0,
                    leave=True,
                ) as pbar:
                    for output in pool.imap_unordered(
                        _features_get, raw_tickers_to_inserts.keys()
                    ):
                        pbar.update()
                        ticker, df = output
                        if len(df.index) > 0:
                            features.quarterly_features.to_store(ticker, df)
                            feature_tickers_to_inserts[ticker] = len(df.index)
                        else:
                            skipped_feature_tickers.add(ticker)
        if not feature_tickers_to_inserts:
            raise RuntimeError(
                "An error occurred when installing SEC features. "
                "Set the logging mode to debug or use the verbose flag with the CLI for more info."
            )
        logger.info(
            f"Total feature rows written: {sum(feature_tickers_to_inserts.values())}"
        )
        logger.info(
            "Number of tickers skipped: "
            f"{len(skipped_feature_tickers)}/{len(raw_tickers_to_inserts)}"
        )
    logger.info("Installation complete!")
