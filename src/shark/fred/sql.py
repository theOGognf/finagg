"""FRED SQLAlchemy interfaces."""

import os
import pathlib

from sqlalchemy import Column, Float, MetaData, String, Table, create_engine, inspect
from sqlalchemy.engine import Engine, Inspector

_DATABASE_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "fred.sqlite"
)

_DATABASE_URL = os.environ.get(
    "FRED_DATABASE_URL",
    f"sqlite:///{_DATABASE_PATH}",
)


def define_db(
    url: str = _DATABASE_URL,
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
    engine = create_engine(url)
    inspector: Inspector = inspect(engine)
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


(engine, metadata), inspector, (series,) = define_db()
