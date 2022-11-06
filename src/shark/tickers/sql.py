"""Ticker SQLAlchemy interfaces."""

import os
import pathlib

from sqlalchemy import Column, Float, MetaData, String, Table, create_engine

_SQL_DB_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "tickers.sqlite"
)

_SQL_DB_URL = os.environ.get(
    "TICKERS_SQL_DB_URL",
    f"sqlite:///{_SQL_DB_PATH}",
)

#: SQLAlchemy engine used for all operations.
engine = create_engine(_SQL_DB_URL)

#: SQLAlchemy metadata used by all datasets.
metadata = MetaData()

#: Ticker dataset APIs as SQL tables.
djia = Table(
    "djia",
    metadata,
    Column("company", String, doc="Company name."),
    Column("exchange", String, doc="Exchange the company is listed on."),
    Column("ticker", String, primary_key=True, doc="Company ticker/symbol."),
    Column("industry", String, doc="The company's industry class."),
    Column("added", String, doc="Date the company was added to the DJIA."),
    Column("weight", Float, doc="Relative weight the company holds in the DJIA."),
)

nasdaq100 = Table(
    "nasdaq100",
    metadata,
    Column("company", String, doc="Company name."),
    Column("ticker", String, primary_key=True, doc="Company ticker/symbol."),
    Column("industry", String, doc="The company's industry class."),
    Column("sub_industry", String, doc="The company's more specific industry class."),
)

sp500 = Table(
    "sp500",
    metadata,
    Column("ticker", String, primary_key=True, doc="Company ticker/symbol."),
    Column("company", String, doc="Company name."),
    Column("industry", String, doc="The company's industry class."),
    Column("sub_industry", String, doc="The company's more specific industry class."),
    Column("headquarters", String, doc="Company headquarters location."),
    Column("added", String, doc="Date the company was added to the S&P 500."),
    Column("cik", String, doc="The company's unique SEC CIK."),
    Column("founded", String, doc="When the company was founded."),
)