"""FRED SQLAlchemy interfaces."""


from functools import cache

import sqlalchemy as sa

from .. import backend

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
    sa.Column("value", sa.Float, doc="Economic series value for a particular date."),
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
    sa.Column("value", sa.Float, doc="Feature value."),
)


@cache
def get_id_set(lb: int = 1) -> set[str]:
    """Get all unique series IDs in the raw SQL tables."""
    with backend.engine.begin() as conn:
        series_ids = set()
        for row in conn.execute(
            sa.select(series.c.series_id)
            .distinct()
            .group_by(series.c.series_id)
            .having(sa.func.count(series.c.date) >= lb)
        ):
            (series_id,) = row
            series_ids.add(str(series_id))
    return series_ids
