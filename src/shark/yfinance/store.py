"""SQLAlchemy interfaces for Yahoo! finance features."""

import os
import pathlib

from sqlalchemy import Column, MetaData, String, Table, create_engine, inspect
from sqlalchemy.engine import Engine, Inspector

_SQL_DB_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "yfinance_features.sqlite"
)

_SQL_DB_URL = os.environ.get(
    "YFINANCE_FEATURES_SQL_DB_URL",
    f"sqlite:///{_SQL_DB_PATH}",
)


def define_db(
    path: str = _SQL_DB_URL,
) -> tuple[Engine, Inspector, MetaData, Table]:
    """Utility method for defining the SQLAlchemy elements.

    Used for the main SQL tables and for creating test
    databases.

    Args:
        path: SQLAlchemy database URL.

    Returns:
        The engine, engine inspector, metadata, and tables associated with
        the database definition.

    """
    engine = create_engine(path)
    inspector: Inspector = inspect(engine)
    metadata = MetaData()
    if inspector.has_table("daily_features"):
        daily_features = Table(
            "daily_features",
            metadata,
            Column("ticker", String, primary_key=True, doc="Unique company ticker."),
            Column("date", String, primary_key=True, doc="Stock price date."),
            autoload_with=engine,
        )
    else:
        daily_features = None
    return engine, inspector, metadata, daily_features


engine, inspector, metadata, daily_features = define_db()
