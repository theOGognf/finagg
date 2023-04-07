"""Yahoo! finance SQLAlchemy interfaces."""

import sqlalchemy as sa

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
