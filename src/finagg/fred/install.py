"""Utils to set the FRED API key and scrape an initial dataset."""

import logging
import os
import sys

from .. import utils
from . import features, scrape, store

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | finagg.fred.install - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def run(install_features: bool = False) -> None:
    """Set the `FRED_API_KEY` environment variable and
    optionally initialize local SQL tables with
    popular economic data.

    Args:
        install_features: Whether to install features from
            the scraped data.

    """
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
        logger.info(f"FRED API key writtern to {p}")
    else:
        logger.info("FRED API key already exists in env")
    raw_count = scrape.run(features.economic_features.series_ids, drop_tables=True)
    logger.info(f"{sum(raw_count.values())} raw rows written")

    if install_features:
        store.metadata.drop_all(store.engine)
        store.metadata.create_all(store.engine)

        df = features.economic_features.from_sql()
        feature_count = features.economic_features.to_store(df)
        logger.info(f"{feature_count} feature rows written")

    logger.info("Installation complete!")
