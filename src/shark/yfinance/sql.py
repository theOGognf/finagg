"""Yahoo! finance SQLAlchemy interfaces."""

import os
import pathlib

from sqlalchemy import Column, Float, MetaData, String, Table, create_engine

_SQL_DB_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "yfinance.sqlite"
)

_SQL_DB_URL = os.environ.get(
    "YFINANCE_SQL_DB_URL",
    f"sqlite:///{_SQL_DB_PATH}",
)

#: SQLAlchemy engine used for all operations.
engine = create_engine(_SQL_DB_URL)

#: SQLAlchemy metadata used by all datasets.
metadata = MetaData()

#: yfinance ticker data SQL tables.
prices = Table(
    "prices",
    metadata,
    Column("ticker", String, primary_key=True, doc="Unique company ticker."),
    Column("date", String, primary_key=True, doc="Stock price date."),
    Column("open", Float, doc="Stock price at market open."),
    Column("high", Float, doc="Stock price max during trading hours."),
    Column("low", Float, doc="Stock price min during trading hours."),
    Column("close", Float, doc="Stock price at market close."),
    Column("volume", Float, doc="Units traded during trading hours."),
)
