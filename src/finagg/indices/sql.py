"""Indices SQLAlchemy interfaces."""

from functools import cache

import sqlalchemy as sa

from .. import backend

metadata = sa.MetaData()

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


@cache
def get_ticker_set() -> set[str]:
    """Get all unique tickers in the raw SQL tables."""
    with backend.engine.begin() as conn:
        tickers = set()
        for table in (djia, nasdaq100, sp500):
            for row in conn.execute(sa.select(table.c.ticker)):
                (ticker,) = row
                tickers.add(str(ticker))
    return tickers
