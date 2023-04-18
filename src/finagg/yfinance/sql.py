"""Yahoo! finance SQLAlchemy interfaces."""

import sqlalchemy as sa

metadata = sa.MetaData()
"""The metadata associated with all SQL tables defined in this module.

:meta hide-value:
"""

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
"""SQL table for storing raw data as managed by
:data:`finagg.yfinance.feat.prices` (an alias for
:class:`finagg.yfinance.feat.Prices`).

:meta hide-value:
"""

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
    sa.Column(
        "LOG_CHANGE(open)",
        sa.Float,
        nullable=False,
        doc=(
            "Logarithmic change in stock price at market open between the "
            "row's date and the previous date."
        ),
    ),
    sa.Column(
        "LOG_CHANGE(high)",
        sa.Float,
        nullable=False,
        doc=(
            "Logarithmic change in stock price max during trading hours "
            "between the row's date and the previous date."
        ),
    ),
    sa.Column(
        "LOG_CHANGE(low)",
        sa.Float,
        nullable=False,
        doc=(
            "Logarithmic change in stock price min during trading hours "
            "between the row's date and the previous date."
        ),
    ),
    sa.Column(
        "LOG_CHANGE(close)",
        sa.Float,
        nullable=False,
        doc=(
            "Logarithmic change in stock price at market close between "
            "the row's date and the previous date."
        ),
    ),
    sa.Column(
        "LOG_CHANGE(volume)",
        sa.Float,
        nullable=False,
        doc=(
            "Logarithmic change in stock units traded between the row's "
            "date and the previous date."
        ),
    ),
    sa.Column(
        "LOG_CHANGE(high, open)",
        sa.Float,
        nullable=False,
        doc=(
            "Logarithmic change between the stock price at market open "
            "and the stock price max during trading hours."
        ),
    ),
    sa.Column(
        "LOG_CHANGE(low, open)",
        sa.Float,
        nullable=False,
        doc=(
            "Logarithmic change between the stock price at market open "
            "and the stock price min during trading hours."
        ),
    ),
)
"""SQL table for storing refined data as managed by
:data:`finagg.yfinance.feat.daily` (an alias for
:class:`finagg.yfinance.feat.Daily`).

:meta hide-value:
"""
