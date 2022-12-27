"""Ticker SQLAlchemy interfaces."""


from sqlalchemy import Column, Float, MetaData, String, Table, create_engine, inspect
from sqlalchemy.engine import Engine, Inspector

from .. import backend


def define_db(
    url: str = backend.database_url,
) -> tuple[tuple[Engine, MetaData], Inspector, tuple[Table, ...]]:
    """Utility method for defining the SQLAlchemy elements.

    Used for the main SQL tables and for creating test
    databases.

    Args:
        url: SQLAlchemy database URL.

    Returns:
        The engine, metadata, and tables associated with
        the database definition.

    """
    if url != backend.engine.url:
        engine = create_engine(url)
        inspector: Inspector = inspect(engine)
    else:
        engine = backend.engine
        inspector = backend.inspector
    metadata = MetaData()
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
        Column(
            "sub_industry", String, doc="The company's more specific industry class."
        ),
    )

    sp500 = Table(
        "sp500",
        metadata,
        Column("ticker", String, primary_key=True, doc="Company ticker/symbol."),
        Column("company", String, doc="Company name."),
        Column("industry", String, doc="The company's industry class."),
        Column(
            "sub_industry", String, doc="The company's more specific industry class."
        ),
        Column("headquarters", String, doc="Company headquarters location."),
        Column("added", String, doc="Date the company was added to the S&P 500."),
        Column("cik", String, doc="The company's unique SEC CIK."),
        Column("founded", String, doc="When the company was founded."),
    )
    return (engine, metadata), inspector, (djia, nasdaq100, sp500)


(engine, metadata), inspector, (djia, nasdaq100, sp500) = define_db()
