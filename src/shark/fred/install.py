"""Utils to set the FRED API key and scrape an initial dataset."""

from ..utils import setenv
from .scrape import scrape


def install(series_id: None | str | list[str] = None) -> None:
    """Set the `FRED_API_KEY` environment variable and scrape economic
    series data (if provided).

    Args:
        series_id: Economic series IDs to scrape. Defaults to `None`
            to skip scraping.

    Returns:
        Mapping of series IDs to values scraped for them. Empty if
        no series are scraped.

    """
    api_key = input(
        "Enter your FRED API key below.\n\n"
        "You can request a FRED API key at\n"
        "https://fred.stlouisfed.org/docs/api/api_key.html.\n\n"
        "FRED API key: "
    ).strip()
    if not api_key:
        raise RuntimeError("An empty FRED API key was given.")
    setenv("FRED_API_KEY", api_key)
    if series_id is not None:
        scrape(series_id)
