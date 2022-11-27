"""Utils to set the FRED API key and scrape an initial dataset."""

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
    """Set the `FRED_API_KEY` environment variable and
    optionally initialize local SQL tables with
    popular economic data.

    Args:
        init_db: Whether to initialize local SQL tables
            with popular economic data.

    Returns:
        Mapping of economic series IDs to rows scraped
        for them. Empty if no economic data is scraped.

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
        p = setenv("FRED_API_KEY", api_key)
        logger.info(f"FRED API key writtern to {p}")
    else:
        logger.info("FRED API key already exists in env")
    if init_db:
        c = scrape(
            [
                "CPIAUCNS",  # Consumer price index
                "CSUSHPINSA",  # S&P/Case-Shiller national home price index
                "FEDFUNDS",  # Federal funds interest rate
                "GDP",  # Gross domestic product
                "GDPC1",  # Real gross domestic product
                "GS10",  # 10-Year treasury yield
                "MICH",  # University of Michigan: inflation expectation
                "UMCSENT",  # University of Michigan: consumer sentiment
                "UNRATE",  # Unemployment rate
                "WALCL",  # US assets, total assets (less eliminations from consolidation)
            ]
        )
        logger.info(f"{sum(c.values())} rows written")
