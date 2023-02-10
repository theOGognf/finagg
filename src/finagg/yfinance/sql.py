"""Yahoo! finance SQLAlchemy interfaces."""

from functools import cache

from sqlalchemy import Column, Float, MetaData, String, Table, create_engine, inspect
from sqlalchemy.engine import Engine, Inspector

from .. import backend


def _define_db(
    url: str = backend.database_url,
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
    if url != backend.engine.url:
        engine = create_engine(url)
        inspector: Inspector = inspect(engine)
    else:
        engine = backend.engine
        inspector = backend.inspector
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


(engine, metadata), inspector, (prices,) = _define_db()


@cache
def get_ticker_set() -> set[str]:
    """Get all unique tickers in the feature SQL tables."""
    with engine.connect() as conn:
        tickers = set()
        for ticker in conn.execute(prices.select().distinct(prices.c.ticker)):
            (ticker,) = ticker
            tickers.add(str(ticker))
    return tickers
