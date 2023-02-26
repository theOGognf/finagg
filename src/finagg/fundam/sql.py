"""SQLAlchemy interfaces for fundamental features."""

from functools import cache

import sqlalchemy as sa

from .. import backend, sec, yfinance

metadata = sa.MetaData()

fundam = sa.Table(
    "fundam.refined.fundam",
    metadata,
    sa.Column(
        "ticker",
        sa.String,
        sa.ForeignKey(sec.sql.submissions.c.ticker, ondelete="CASCADE"),
        sa.ForeignKey(yfinance.sql.prices.c.ticker, ondelete="CASCADE"),
        primary_key=True,
        doc="Unique company ticker.",
    ),
    sa.Column("date", sa.String, primary_key=True, doc="Filing and stock price dates."),
    sa.Column("name", sa.String, primary_key=True, doc="Feature name."),
    sa.Column("value", sa.Float, nullable=False, doc="Feature value."),
)


normalized_fundam = sa.Table(
    "fundam.refined.fundam.normalized",
    metadata,
    sa.Column(
        "ticker",
        sa.String,
        sa.ForeignKey(fundam.c.ticker, ondelete="CASCADE"),
        primary_key=True,
        doc="Unique company ticker.",
    ),
    sa.Column("date", sa.String, primary_key=True, doc="Filing and stock price dates."),
    sa.Column("name", sa.String, primary_key=True, doc="Feature name."),
    sa.Column("value", sa.Float, nullable=False, doc="Feature value."),
)


@cache
def get_ticker_set(lb: int = 1) -> set[str]:
    """Get all unique tickers in the raw SQL tables."""
    with backend.engine.begin() as conn:
        tickers = set()
        for row in conn.execute(
            sa.select(fundam.c.ticker)
            .distinct()
            .group_by(fundam.c.ticker)
            .having(sa.func.count(fundam.c.date) >= lb)
        ):
            (ticker,) = row
            tickers.add(str(ticker))
    return tickers
