"""SEC SQLAlchemy interfaces."""

from functools import cache

import sqlalchemy as sa

from .. import backend
from . import api

metadata = sa.MetaData()

submissions = sa.Table(
    "submissions",
    metadata,
    sa.Column("cik", sa.String, primary_key=True, doc="Unique SEC ID."),
    sa.Column(
        "entity_type", sa.String, doc="Type of company standing (e.g., operating)."
    ),
    sa.Column("sic", sa.String, nullable=False, doc="Industry code."),
    sa.Column("sic_description", sa.String, doc="Industry code description."),
    sa.Column("name", sa.String, doc="Company name."),
    sa.Column(
        "exchanges",
        sa.String,
        doc="Comma-separated exchanges the company is found on.",
    ),
    sa.Column("ein", sa.String, doc="Entity identification number."),
    sa.Column("description", sa.String, doc="Entity description (often empty/null)."),
    sa.Column("category", sa.String, doc="SEC entity category."),
    sa.Column(
        "fiscal_year_end",
        sa.String,
        doc="The company's last day of the fiscal year (MMDD).",
    ),
)

tags = sa.Table(
    "tags",
    metadata,
    sa.Column(
        "cik",
        sa.String,
        sa.ForeignKey(submissions.c.cik, ondelete="CASCADE"),
        primary_key=True,
        doc="Unique SEC ID.",
    ),
    sa.Column("accn", sa.String, doc="Unique submission/access number."),
    sa.Column(
        "taxonomy", sa.String, doc="XBRL taxonomy the submission's tag belongs to."
    ),
    sa.Column(
        "tag",
        sa.String,
        primary_key=True,
        doc="XBRL submission tag (e.g., NetIncomeLoss).",
    ),
    sa.Column(
        "form", sa.String, nullable=False, doc="Submission form type (e.g., 10-Q)."
    ),
    sa.Column(
        "units",
        sa.String,
        nullable=False,
        doc="Unit of measurements for tag value (e.g., USD or shares).",
    ),
    sa.Column(
        "fy", sa.Integer, primary_key=True, doc="Fiscal year the submission is for."
    ),
    sa.Column(
        "fp",
        sa.String,
        primary_key=True,
        doc="Fiscal period the submission is for (e.g., Q1 or FY).",
    ),
    sa.Column(
        "start",
        sa.String,
        nullable=True,
        doc="When the tag's value's measurements started.",
    ),
    sa.Column(
        "end",
        sa.String,
        nullable=True,
        doc="When the tag's value's measurements ended.",
    ),
    sa.Column(
        "filed",
        sa.String,
        nullable=False,
        doc="When the submission was actually filed.",
    ),
    sa.Column(
        "frame",
        sa.String,
        nullable=True,
        doc="Often a concatenation of `fy` and `fp`.",
    ),
    sa.Column(
        "label",
        sa.String,
        nullable=True,
        doc="More human readable version of `tag`.",
    ),
    sa.Column(
        "description",
        sa.String,
        nullable=True,
        doc="Long description of `tag` and `label`.",
    ),
    sa.Column("entity", sa.String, doc="Company name."),
    sa.Column("value", sa.Float, nullable=False, doc="Tag value with units `units`."),
)

quarterly_features = sa.Table(
    "quarterly_features",
    metadata,
    sa.Column(
        "cik",
        sa.String,
        sa.ForeignKey(submissions.c.cik, ondelete="CASCADE"),
        primary_key=True,
        doc="Unique company ticker.",
    ),
    sa.Column("filed", sa.String, nullable=False, doc="Filing date."),
    sa.Column("name", sa.String, primary_key=True, doc="Feature name."),
    sa.Column("fy", sa.Integer, primary_key=True, doc="Fiscal year the value is for."),
    sa.Column(
        "fp",
        sa.String,
        primary_key=True,
        doc="Fiscal period the value is for (e.g., Q1 or FY).",
    ),
    sa.Column("value", sa.Float, nullable=False, doc="Feature value."),
)


relative_quarterly_features = sa.Table(
    "relative_quarterly_features",
    metadata,
    sa.Column(
        "cik",
        sa.String,
        sa.ForeignKey(submissions.c.cik, ondelete="CASCADE"),
        primary_key=True,
        doc="Unique company ticker.",
    ),
    sa.Column("filed", sa.String, nullable=False, doc="Filing date."),
    sa.Column("name", sa.String, primary_key=True, doc="Feature name."),
    sa.Column("fy", sa.Integer, primary_key=True, doc="Fiscal year the value is for."),
    sa.Column(
        "fp",
        sa.String,
        primary_key=True,
        doc="Fiscal period the value is for (e.g., Q1 or FY).",
    ),
    sa.Column("value", sa.Float, nullable=False, doc="Feature value."),
)


@cache
def get_ticker_set(lb: int = 1) -> set[str]:
    """Get all unique tickers in the raw SQL tables."""
    with backend.engine.begin() as conn:
        tickers = set()
        for cik in conn.execute(
            sa.select(tags.c.cik)
            .distinct()
            .group_by(tags.c.cik)
            .having(sa.func.count(tags.c.filed) >= lb)
        ):
            (cik,) = cik
            ticker = api.get_ticker(str(cik))
            tickers.add(ticker)
    return tickers
