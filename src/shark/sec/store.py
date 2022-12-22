"""SQLAlchemy interfaces for SEC features."""

import os
import pathlib

from sqlalchemy import Column, MetaData, String, Table, create_engine, inspect
from sqlalchemy.engine import Engine, Inspector

_DATABASE_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "sec_features.sqlite"
)

_DATABASE_URL = os.environ.get(
    "SEC_FEATURES_DATABASE_URL",
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

    Returns:
        The engine, engine inspector, metadata, and tables associated with
        the database definition.

    """
    engine = create_engine(url)
    inspector: Inspector = inspect(engine)
    metadata = MetaData()
    if inspector.has_table("quarterly_features"):
        quarterly_features = Table(
            "quarterly_features",
            metadata,
            Column("ticker", String, primary_key=True, doc="Unique company ticker."),
            Column("filed", String, primary_key=True, doc="Filing date."),
            autoload_with=engine,
        )
    else:
        quarterly_features = None
    return (engine, metadata), inspector, (quarterly_features,)


(engine, metadata), inspector, (quarterly_features,) = define_db()
