"""FRED SQLAlchemy interfaces."""


from functools import cache

from sqlalchemy import (
    Column,
    Float,
    MetaData,
    String,
    Table,
    create_engine,
    distinct,
    inspect,
    select,
)
from sqlalchemy.engine import Engine, Inspector

from .. import backend


def _define_db(
    url: str = backend.database_url,
) -> tuple[tuple[Engine, MetaData], Inspector, tuple[Table, ...]]:
    """Utility method for defining the SQLAlchemy elements.

    Used for the main SQL tables and for creating test
    databases.

    Args:
        url: SQLAlchemy database URL.
        path: Path to database file.

    Returns:
        The engine, engine inspector, metadata, and tables associated with
        the database definition.

    """
    if url != backend.engine.url:
        engine = create_engine(url)
        inspector: Inspector = inspect(engine)
    else:
        engine = backend.engine
        inspector = backend.inspector
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
    return (engine, metadata), inspector, (series,)


(engine, metadata), inspector, (series,) = _define_db()


@cache
def get_series_set() -> set[str]:
    """Get all unique series in the raw SQL tables."""
    with engine.connect() as conn:
        series_ids = set()
        for series_id in conn.execute(select(distinct(series.c.series_id))):
            (series_id,) = series_id
            series_ids.add(series_id)
    return series_ids
