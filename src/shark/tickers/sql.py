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
    Column("company", String),
    Column("exchange", String),
    Column("ticker", String, primary_key=True),
    Column("industry", String),
    Column("added", String),
    Column("weight", Float),
)

nasdaq100 = Table(
    "nasdaq100",
    metadata,
    Column("company", String),
    Column("ticker", String, primary_key=True),
    Column("industry", String),
    Column("sub_industry", String),
)

sp500 = Table(
    "sp500",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("company", String),
    Column("industry", String),
    Column("sub_industry", String),
    Column("headquarters", String),
    Column("added", String),
    Column("cik", String),
    Column("founded", String),
)
