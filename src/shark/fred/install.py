"""Utils to set the FRED API key and scrape an initial dataset."""

import logging
import os
import sys

from .. import utils
from . import features, scrape

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | shark.fred.install - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def run() -> None:
    """Set the `FRED_API_KEY` environment variable and
    optionally initialize local SQL tables with
    popular economic data.

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
    c = scrape.run(features.economic_features.series_ids, drop_tables=True)
    logger.info(f"{sum(c.values())} rows written")
