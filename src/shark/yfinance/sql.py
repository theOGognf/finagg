"""Yahoo! finance SQLAlchemy interfaces."""

import os
import pathlib
from functools import cache

from sqlalchemy import (
    Column,
    Float,
    MetaData,
    String,
    Table,
    create_engine,
    distinct,
    inspect,
    select,
)
from sqlalchemy.engine import Engine, Inspector

_DATABASE_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "yfinance.sqlite"
)

_DATABASE_URL = os.environ.get(
    "YFINANCE_DATABASE_URL",
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
    return (engine, metadata), inspector, (prices,)


(engine, metadata), inspector, (prices,) = define_db()


@cache
def get_ticker_set() -> set[str]:
    """Get all unique tickers in the feature SQL tables."""
    with engine.connect() as conn:
        tickers = set()
        for ticker in conn.execute(select(distinct(prices.c.ticker))):
            (ticker,) = ticker
            tickers.add(ticker)
    return tickers
