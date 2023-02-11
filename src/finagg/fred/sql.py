"""FRED SQLAlchemy interfaces."""


from functools import cache

from sqlalchemy import Column, Float, MetaData, String, Table, create_engine
from sqlalchemy.engine import Engine

from .. import backend


def _define_db(
    url: str = backend.database_url,
) -> tuple[tuple[Engine, MetaData], tuple[Table, ...]]:
    """Utility method for defining the SQLAlchemy elements.

    Used for the main SQL tables and for creating test
    databases.

    Args:
        url: SQLAlchemy database URL.
        path: Path to database file.

    Returns:
        The engine, metadata, and tables associated with
        the database definition.

    """
    engine = backend.engine if url == backend.engine.url else create_engine(url)
    metadata = MetaData()
    series = Table(
        "series",
        metadata,
        Column("series_id", String, primary_key=True, doc="Economic series ID."),
        Column(
            "realtime_start",
            String,
            primary_key=True,
            doc="Start date for values according to their publication date.",
        ),
        Column(
            "realtime_end",
            String,
            primary_key=True,
            doc="End date for values according to their publication date.",
        ),
        Column("date", String, primary_key=True, doc="Series value publication date."),
        Column("value", Float, doc="Economic series value for a particular date."),
    )
    return (engine, metadata), (series,)


(engine, metadata), (series,) = _define_db()


@cache
def get_series_set() -> set[str]:
    """Get all unique series in the raw SQL tables."""
    with engine.begin() as conn:
        series_ids = set()
        for series_id in conn.execute(series.select().distinct(series.c.series_id)):
            (series_id,) = series_id
            series_ids.add(str(series_id))
    return series_ids
