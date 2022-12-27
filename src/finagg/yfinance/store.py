"""SQLAlchemy interfaces for Yahoo! finance features."""

from functools import cache

from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    create_engine,
    distinct,
    inspect,
    select,
)
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
        path: Path to database file.

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
    if inspector.has_table("daily_features"):
        daily_features = Table(
            "daily_features",
            metadata,
            Column("ticker", String, primary_key=True, doc="Unique company ticker."),
            Column("date", String, primary_key=True, doc="Stock price date."),
            autoload_with=engine,
        )
    else:
        daily_features = None
    return (engine, metadata), inspector, (daily_features,)


(engine, metadata), inspector, (daily_features,) = _define_db()


@cache
def get_ticker_set() -> set[str]:
    """Get all unique tickers in the feature SQL tables."""
    with engine.connect() as conn:
        tickers = set()
        for ticker in conn.execute(select(distinct(daily_features.c.ticker))):
            (ticker,) = ticker
            tickers.add(ticker)
    return tickers