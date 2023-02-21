"""Yahoo! finance SQLAlchemy interfaces."""

from functools import cache

import sqlalchemy as sa

from .. import backend

metadata = sa.MetaData()

prices = sa.Table(
    "prices",
    metadata,
    sa.Column("ticker", sa.String, primary_key=True, doc="Unique company ticker."),
    sa.Column("date", sa.String, primary_key=True, doc="Stock price date."),
    sa.Column("open", sa.Float, doc="Stock price at market open."),
    sa.Column("high", sa.Float, doc="Stock price max during trading hours."),
    sa.Column("low", sa.Float, doc="Stock price min during trading hours."),
    sa.Column("close", sa.Float, doc="Stock price at market close."),
    sa.Column("volume", sa.Float, doc="Units traded during trading hours."),
)

daily_features = sa.Table(
    "daily_features",
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
    sa.Column("value", sa.Float, doc="Feature value."),
)


@cache
def get_ticker_set(lb: int = 1) -> set[str]:
    """Get all unique tickers in the raw SQL tables."""
    with backend.engine.begin() as conn:
        tickers = set()
        for row in conn.execute(
            sa.select(prices.c.ticker)
            .distinct()
            .group_by(prices.c.ticker)
            .having(sa.func.count(prices.c.date) >= lb)
        ):
            (ticker,) = row
            tickers.add(str(ticker))
    return tickers
