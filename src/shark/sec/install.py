"""Utils to set the SEC API key and scrape an initial dataset."""

import logging
import os
import sys

from ..utils import setenv
from .scrape import scrape

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | shark.fred.install - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def install(init_db: bool = True) -> None:
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
        logger.info(f"FRED API key writtern to {p}")
    else:
        logger.info("SEC API user agent already exists in env")
    if init_db:
        c = scrape(
            [
                "DJIA",
                "NASDAQ100",
                "SP500",
            ]
        )
        logger.info(f"{sum(c.values())} rows written")
