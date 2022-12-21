"""SQLAlchemy interfaces for SEC features."""

import os
import pathlib

from sqlalchemy import Column, MetaData, String, Table, create_engine, inspect
from sqlalchemy.engine import Inspector

_SQL_DB_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "sec_features.sqlite"
)

_SQL_DB_URL = os.environ.get(
    "SEC_FEATURES_SQL_DB_URL",
    f"sqlite:///{_SQL_DB_PATH}",
)

#: SQLAlchemy engine used for all operations.
engine = create_engine(_SQL_DB_URL)

#: SQLAlchemy metadata used by all feature store tables.
metadata = MetaData()

#: SQLAlchemy engine inspector for dynamically reflecting
#: tables.
inspector: Inspector = inspect(engine)

if inspector.has_table("quarterly_features"):
    quarterly_features = Table(
        "quarterly_features",
        metadata,
        Column("ticker", String, primary_key=True, doc="Unique company ticker."),
        Column("filed", String, primary_key=True, doc="Filing date."),
        autoload_with=engine,
    )
