"""FRED SQLAlchemy interfaces."""


import sqlalchemy as sa

metadata = sa.MetaData()
"""The metadata associated with all SQL tables defined in this module.

:meta hide-value:
"""

series = sa.Table(
    "fred.raw.series",
    metadata,
    sa.Column("series_id", sa.String, primary_key=True, doc="Economic series ID."),
    sa.Column(
        "realtime_start",
        sa.String,
        primary_key=True,
        doc="Start date for values according to their publication date.",
    ),
    sa.Column(
        "realtime_end",
        sa.String,
        primary_key=True,
        doc="End date for values according to their publication date.",
    ),
    sa.Column(
        "date", sa.String, primary_key=True, doc="Series value publication date."
    ),
    sa.Column(
        "value",
        sa.Float,
        doc="Economic series value for a particular date.",
    ),
)
"""SQL table for storing raw data as managed by
:data:`finagg.fred.feat.series` (an alias for
:class:`finagg.fred.feat.Series`).

:meta hide-value:
"""

economic = sa.Table(
    "fred.refined.economic",
    metadata,
    sa.Column(
        "date",
        sa.String,
        primary_key=True,
        doc="Economic data series release date.",
    ),
    sa.Column(
        "CIVPART", sa.Float, nullable=False, doc="Labor force participation rate."
    ),
    sa.Column(
        "LOG_CHANGE(CPIAUCNS)",
        sa.Float,
        nullable=False,
        doc="Logarithmic change in consumer price index between days.",
    ),
    sa.Column(
        "LOG_CHANGE(CSUSHPINSA)",
        sa.Float,
        nullable=False,
        doc=(
            "Logarithmic change in S&P/Case-Shiller national home price index between"
            " days."
        ),
    ),
    sa.Column(
        "LOG_CHANGE(DJIA)",
        sa.Float,
        nullable=False,
        doc="Logarithmic change in Dow Jones Industrial index.",
    ),
    sa.Column("FEDFUNDS", sa.Float, nullable=False, doc="Federal funds interest rate."),
    sa.Column(
        "LOG_CHANGE(GDP)",
        sa.Float,
        nullable=False,
        doc="Logarithmic change in gross domestic product between days.",
    ),
    sa.Column(
        "LOG_CHANGE(GDPC1)",
        sa.Float,
        nullable=False,
        doc="Logarithmic change in real gross domestic product between days.",
    ),
    sa.Column("GS10", sa.Float, nullable=False, doc="10-Year treasury yield."),
    sa.Column(
        "LOG_CHANGE(M2)",
        sa.Float,
        nullable=False,
        doc=(
            "Logarithmic change in money stock measures (i.e., savings and related"
            " balances) between days."
        ),
    ),
    sa.Column(
        "MICH",
        sa.Float,
        nullable=False,
        doc="University of Michigan: inflation expectation.",
    ),
    sa.Column(
        "LOG_CHANGE(NASDAQ100)",
        sa.Float,
        nullable=False,
        doc="Logarithmic change in Nasdaq 100 index.",
    ),
    sa.Column(
        "LOG_CHANGE(NASDAQCOM)",
        sa.Float,
        nullable=False,
        doc="Logarithmic change in Nasdaq Composite index.",
    ),
    sa.Column("PSAVERT", sa.Float, nullable=False, doc="Personal savings rate."),
    sa.Column(
        "LOG_CHANGE(SP500)",
        sa.Float,
        nullable=False,
        doc="Logarithmic change in S&P 500 index.",
    ),
    sa.Column(
        "UMCSENT",
        sa.Float,
        nullable=False,
        doc="University of Michigan: consumer sentiment.",
    ),
    sa.Column("UNRATE", sa.Float, nullable=False, doc="Unemployment rate."),
    sa.Column(
        "LOG_CHANGE(WALCL)",
        sa.Float,
        nullable=False,
        doc=(
            "Logarithmic change in US assets, total assets (less eliminations from"
            " consolidation) between days."
        ),
    ),
)
"""SQL table for storing refined data as managed by :data:`finagg.fred.feat.economic`
(an alias for :class:`finagg.fred.feat.Economic`).

:meta hide-value:
"""
