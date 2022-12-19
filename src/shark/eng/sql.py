"""SQLAlchemy interfaces for engineered features."""

import os
import pathlib

from sqlalchemy import Column, MetaData, String, Table, create_engine, inspect
from sqlalchemy.engine import Inspector

_SQL_DB_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent / "data" / "eng.sqlite"
)

_SQL_DB_URL = os.environ.get(
    "ENG_SQL_DB_URL",
    f"sqlite:///{_SQL_DB_PATH}",
)

#: SQLAlchemy engine used for all operations.
engine = create_engine(_SQL_DB_URL)

#: SQLAlchemy metadata used by all feature store tables.
metadata = MetaData()

#: SQLAlchemy engine inspector for dynamically reflecting
#: tables.
inspector: Inspector = inspect(engine)

#: Features have minimal constraints to allow quick experimentation.
if inspector.has_table("daily_features"):
    daily_features = Table(
        "daily_features",
        metadata,
        Column("ticker", String, primary_key=True, doc="Unique company ticker."),
        Column("date", String, primary_key=True, doc="Stock price date."),
        autoload_with=engine,
    )

if inspector.has_table("fundamental_features"):
    fundamental_features = Table(
        "fundamental_features",
        metadata,
        Column("ticker", String, primary_key=True, doc="Unique company ticker."),
        Column("date", String, primary_key=True, doc="Filing and stock price dates."),
        autoload_with=engine,
    )

if inspector.has_table("quarterly_features"):
    quarterly_features = Table(
        "quarterly_features",
        metadata,
        Column("ticker", String, primary_key=True, doc="Unique company ticker."),
        Column("filed", String, primary_key=True, doc="Filing date."),
        autoload_with=engine,
    )
