"""Scrape the FRED API for economic series data and store into local SQL tables."""

from . import api
from .sql import engine, metadata
from .sql import series as series_table


def scrape(series_ids: str | list[str], /) -> dict[str, int]:
    """Scrape FRED economic series observation data
    from the FRED API.

    ALL TABLES ARE DROPPED PRIOR TO SCRAPING!
    Scraped data is loaded into local SEC SQL tables.

    Only the initial release of each observation is scraped.
    You must write your own scrape method to scrape additional
    data (such as updates or republications).

    Args:
        series_ids: FRED economic series IDs to scrape.

    Returns:
        A dictionary mapping series IDs to number of rows scraped
        for each series.

    """
    if isinstance(series_ids, str):
        series_ids = [series_ids]

    metadata.drop_all(engine)
    metadata.create_all(engine)

    with engine.connect() as conn:
        series_to_inserts = {}
        for series_id in series_ids:
            df = api.series.observations.get(
                series_id,
                realtime_start=0,
                realtime_end=-1,
                output_type=4,
            )
            series_to_inserts[series_id] = len(df.index)
            conn.execute(series_table.insert(), df.to_dict(orient="records"))

    return series_to_inserts
