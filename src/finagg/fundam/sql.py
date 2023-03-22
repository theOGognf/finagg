"""SQLAlchemy interfaces for fundamental features."""


import sqlalchemy as sa

from .. import sec, yfinance

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
