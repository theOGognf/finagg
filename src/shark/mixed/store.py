"""SQLAlchemy interfaces for mixed features."""

import os
import pathlib

from sqlalchemy import Column, MetaData, String, Table, create_engine, inspect
from sqlalchemy.engine import Inspector

_SQL_DB_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "mixed_features.sqlite"
)

_SQL_DB_URL = os.environ.get(
    "MIXED_FEATURES_SQL_DB_URL",
    f"sqlite:///{_SQL_DB_PATH}",
)

#: SQLAlchemy engine used for all operations.
engine = create_engine(_SQL_DB_URL)

#: SQLAlchemy metadata used by all feature store tables.
metadata = MetaData()

#: SQLAlchemy engine inspector for dynamically reflecting
#: tables.
inspector: Inspector = inspect(engine)  # type: ignore

#: Features have minimal constraints to allow quick experimentation.
if inspector.has_table("fundamental_features"):
    fundamental_features = Table(
        "fundamental_features",
        metadata,
        Column("ticker", String, primary_key=True, doc="Unique company ticker."),
        Column("date", String, primary_key=True, doc="Filing and stock price dates."),
        autoload_with=engine,
    )
