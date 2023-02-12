"""Scrape the FRED API for economic series data and store into local SQL tables."""

from typing import Sequence

from sqlalchemy.engine import Engine

from . import api, sql


def run(
    series_ids: str | Sequence[str],
    /,
    *,
    engine: Engine = sql.engine,
    drop_tables: bool = False,
) -> dict[str, int]:
    """Scrape FRED economic series observation data
    from the FRED API.

    Scraped data is loaded into local SEC SQL tables.

    Only the initial release of each observation is scraped.
    You must write your own scrape method to scrape additional
    data (such as updates or republications).

    Args:
        series_ids: FRED economic series IDs to scrape.
        engine: Custom database engine to use.
        drop_tables: Whether to drop tables before scraping.

    Returns:
        A dictionary mapping series IDs to number of rows scraped
        for each series.

    """
    if isinstance(series_ids, str):
        series_ids = [series_ids]

    if drop_tables:
        sql.metadata.drop_all(engine)

    sql.metadata.create_all(engine)

    with engine.begin() as conn:
        series_to_inserts = {}
        for series_id in series_ids:
            df = api.series.observations.get(
                series_id,
                realtime_start=0,
                realtime_end=-1,
                output_type=4,
            )
            count = conn.execute(
                sql.series.insert(), df.to_dict(orient="records")  # type: ignore[arg-type]
            ).rowcount
            series_to_inserts[series_id] = count

    return series_to_inserts
