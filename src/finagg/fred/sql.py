"""FRED SQLAlchemy interfaces."""


import sqlalchemy as sa

metadata = sa.MetaData()

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
        nullable=False,
        doc="Economic series value for a particular date.",
    ),
)

economic = sa.Table(
    "fred.refined.economic",
    metadata,
    sa.Column(
        "date",
        sa.String,
        primary_key=True,
        doc="Economic data series release date.",
    ),
    sa.Column("name", sa.String, primary_key=True, doc="Feature name."),
    sa.Column("value", sa.Float, nullable=False, doc="Feature value."),
)

normalized_economic = sa.Table(
    "fred.refined.economic.normalized",
    metadata,
    sa.Column(
        "date",
        sa.String,
        primary_key=True,
        doc="Economic data series release date.",
    ),
    sa.Column("name", sa.String, primary_key=True, doc="Feature name."),
    sa.Column("value", sa.Float, nullable=False, doc="Feature value."),
)
