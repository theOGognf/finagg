"""SQLAlchemy interfaces for mixed features."""

import os
import pathlib

from sqlalchemy import Column, MetaData, String, Table, create_engine, inspect
from sqlalchemy.engine import Inspector

_SQL_DB_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "fred_features.sqlite"
)

_SQL_DB_URL = os.environ.get(
    "FRED_FEATURES_SQL_DB_URL",
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
if inspector.has_table("economic_features"):
    economic_features = Table(
        "economic_features",
        metadata,
        Column(
            "date", String, primary_key=True, doc="Economic data series release date."
        ),
        autoload_with=engine,
    )
