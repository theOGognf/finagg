"""Utils to scrape an initial ticker dataset."""

import logging
import sys

from . import scrape

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | shark.tickers.install - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def run() -> None:
    """Initialize a local SQL table with popular ticker info."""
    c = scrape.run(djia=True, sp500=True, nasdaq100=True)
    logger.info(f"{sum(c.values())} rows written")
