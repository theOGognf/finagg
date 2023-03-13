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
    """Get all unique ticker symbols in the raw SQL tables that have at least
    ``lb`` rows.

    This method is convenient for accessing the tickers that have raw SQL data
    associated with them so the data associated with those tickers can be
    further refined. A common pattern is to use this method and other
    ``get_ticker_set`` methods (such as those found in :mod:`finagg.fundam.feat`)
    to determine which tickers are missing data from other tables or features.

    Args:
        lb: Lower bound number of rows that a company must have for its ticker
            to be included in the set returned by this method.

    Examples:
        >>> "AAPL" in finagg.fundam.sql.get_ticker_set()
        True

    """
    with backend.engine.begin() as conn:
        tickers = (
            conn.execute(
                sa.select(fundam.c.ticker)
                .group_by(fundam.c.ticker)
                .having(sa.func.count(fundam.c.date) >= lb)
            )
            .scalars()
            .all()
        )
    return set(tickers)
