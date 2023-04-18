"""SQLAlchemy interfaces for fundamental features."""


import sqlalchemy as sa

from .. import sec, yfinance

metadata = sa.MetaData()
"""The metadata associated with all SQL tables defined in this module.

:meta hide-value:
"""

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
    sa.Column(
        "PriceBookRatio",
        sa.Float,
        nullable=False,
        doc="Market share price over book share price.",
    ),
    sa.Column(
        "PriceEarningsRatio",
        sa.Float,
        nullable=False,
        doc="Market share price over earnings per share.",
    ),
)
"""SQL table for storing refined data as managed by
:data:`finagg.fundam.feat.fundam` (an alias for
:class:`finagg.fundam.feat.Fundamental`).

:meta hide-value:
"""

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
    sa.Column(
        "NORM(PriceBookRatio)",
        sa.Float,
        nullable=False,
        doc=(
            "Market share price over book share price normalized against the "
            "company's industry."
        ),
    ),
    sa.Column(
        "NORM(PriceEarningsRatio)",
        sa.Float,
        nullable=False,
        doc=(
            "Market share price over earnings per share normalized against the "
            "company's industry."
        ),
    ),
)
"""SQL table for storing refined data as managed by
:attr:`finagg.fundam.feat.Fundamental.normalized` (an alias for
:class:`finagg.fundam.feat.NormalizedFundamental`).

:meta hide-value:
"""
