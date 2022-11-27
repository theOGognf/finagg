"""FRED SQLAlchemy interfaces."""

import os
import pathlib

from sqlalchemy import Column, Float, MetaData, String, Table, create_engine

_SQL_DB_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "fred.sqlite"
)

_SQL_DB_URL = os.environ.get(
    "FRED_SQL_DB_URL",
    f"sqlite:///{_SQL_DB_PATH}",
)

#: SQLAlchemy engine used for all operations.
engine = create_engine(_SQL_DB_URL)

#: SQLAlchemy metadata used by all datasets.
metadata = MetaData()

#: FRED economic series data as SQL tables.
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
