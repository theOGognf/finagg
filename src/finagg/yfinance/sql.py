"""Yahoo! finance SQLAlchemy interfaces."""

from functools import cache

import sqlalchemy as sa

from .. import backend

metadata = sa.MetaData()

prices = sa.Table(
    "yfinance.raw.prices",
    metadata,
    sa.Column("ticker", sa.String, primary_key=True, doc="Unique company ticker."),
    sa.Column("date", sa.String, primary_key=True, doc="Stock price date."),
    sa.Column("open", sa.Float, doc="Stock price at market open."),
    sa.Column("high", sa.Float, doc="Stock price max during trading hours."),
    sa.Column("low", sa.Float, doc="Stock price min during trading hours."),
    sa.Column("close", sa.Float, doc="Stock price at market close."),
    sa.Column("volume", sa.Float, doc="Units traded during trading hours."),
)

daily = sa.Table(
    "yfinance.refined.daily",
    metadata,
    sa.Column(
        "ticker",
        sa.String,
        sa.ForeignKey(prices.c.ticker, ondelete="CASCADE"),
        primary_key=True,
        doc="Unique company ticker.",
    ),
    sa.Column("date", sa.String, primary_key=True, doc="Date associated with feature."),
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
    ``get_ticker_set`` methods (such as those found in :mod:`finagg.yfinance.feat`)
    to determine which tickers are missing data from other tables or features.

    Args:
        lb: Lower bound number of rows that a company must have for its ticker
            to be included in the set returned by this method.

    Examples:
        >>> "AAPL" in finagg.yfinance.sql.get_ticker_set()
        True

    """
    with backend.engine.begin() as conn:
        tickers = set()
        for row in conn.execute(
            sa.select(prices.c.ticker)
            .group_by(prices.c.ticker)
            .having(sa.func.count(prices.c.date) >= lb)
        ):
            (ticker,) = row
            tickers.add(str(ticker))
    return tickers
