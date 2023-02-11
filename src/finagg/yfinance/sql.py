"""Yahoo! finance SQLAlchemy interfaces."""

from functools import cache

from sqlalchemy import Column, Float, MetaData, String, Table, create_engine
from sqlalchemy.engine import Engine

from .. import backend


def _define_db(
    url: str = backend.database_url,
) -> tuple[tuple[Engine, MetaData], tuple[Table, ...]]:
    """Utility method for defining the SQLAlchemy elements.

    Used for the main SQL tables and for creating test
    databases.

    Args:
        url: SQLAlchemy database URL.

    Returns:
        The engine, metadata, and tables associated with
        the database definition.

    """
    engine = backend.engine if url == backend.engine.url else create_engine(url)
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
    return (engine, metadata), (prices,)


(engine, metadata), (prices,) = _define_db()


@cache
def get_ticker_set() -> set[str]:
    """Get all unique tickers in the feature SQL tables."""
    with engine.begin() as conn:
        tickers = set()
        for ticker in conn.execute(prices.select().distinct(prices.c.ticker)):
            (ticker,) = ticker
            tickers.add(str(ticker))
    return tickers
