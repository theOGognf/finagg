"""Indices SQLAlchemy interfaces."""


import sqlalchemy as sa
from sqlalchemy.engine import Engine

from .. import backend

metadata = sa.MetaData()
"""The metadata associated with all SQL tables defined in this module.

:meta hide-value:
"""

djia = sa.Table(
    "indices.raw.djia",
    metadata,
    sa.Column("company", sa.String, doc="Company name."),
    sa.Column("exchange", sa.String, doc="Exchange the company is listed on."),
    sa.Column("ticker", sa.String, primary_key=True, doc="Company ticker/symbol."),
    sa.Column("industry", sa.String, doc="The company's industry class."),
    sa.Column("added", sa.String, doc="Date the company was added to the DJIA."),
    sa.Column("weight", sa.Float, doc="Relative weight the company holds in the DJIA."),
)
"""SQL table for storing raw data as returned by
:data:`finagg.indices.api.djia` (an alias for
:class:`finagg.indices.api.DJIA`).

:meta hide-value:
"""

nasdaq100 = sa.Table(
    "indices.raw.nasdaq100",
    metadata,
    sa.Column("company", sa.String, doc="Company name."),
    sa.Column("ticker", sa.String, primary_key=True, doc="Company ticker/symbol."),
    sa.Column("industry", sa.String, doc="The company's industry class."),
    sa.Column(
        "sub_industry", sa.String, doc="The company's more specific industry class."
    ),
)
"""SQL table for storing raw data as returned by
:data:`finagg.indices.api.nasdaq100` (an alias for
:class:`finagg.indices.api.Nasdaq100`).

:meta hide-value:
"""

sp500 = sa.Table(
    "indices.raw.sp500",
    metadata,
    sa.Column("ticker", sa.String, primary_key=True, doc="Company ticker/symbol."),
    sa.Column("company", sa.String, doc="Company name."),
    sa.Column("industry", sa.String, doc="The company's industry class."),
    sa.Column(
        "sub_industry", sa.String, doc="The company's more specific industry class."
    ),
    sa.Column("headquarters", sa.String, doc="Company headquarters location."),
    sa.Column("added", sa.String, doc="Date the company was added to the S&P 500."),
    sa.Column("cik", sa.String, doc="The company's unique SEC CIK."),
    sa.Column("founded", sa.String, doc="When the company was founded."),
)
"""SQL table for storing raw data as returned by
:data:`finagg.indices.api.sp500` (an alias for
:class:`finagg.indices.api.SP500`).

:meta hide-value:
"""


def get_ticker_set(*, engine: None | Engine = None) -> set[str]:
    """Get all unique tickers in the raw SQL tables.

    Args:
        engine: Feature store database engine. Defaults to the engine
            at :data:`finagg.backend.engine`.

    Examples:
        >>> "AAPL" in finagg.indices.sql.get_ticker_set()
        True

    """
    engine = engine or backend.engine
    for table in (djia, nasdaq100, sp500):
        if not sa.inspect(engine).has_table(table.name):
            table.create(engine)
    with engine.begin() as conn:
        tickers: set[str] = set()
        for table in (djia, nasdaq100, sp500):
            tickers.update(conn.execute(sa.select(table.c.ticker)).scalars().all())
    return tickers
