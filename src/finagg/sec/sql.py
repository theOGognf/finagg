"""SEC SQLAlchemy interfaces."""

from functools import cache

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from .. import backend
from . import api


def _define_db(
    url: str = backend.database_url,
) -> tuple[tuple[Engine, sa.MetaData], tuple[sa.Table, ...]]:
    """Utility method for defining the SQLAlchemy elements.

    Used for the main SQL tables and for creating test
    databases.

    Args:
        url: SQLAlchemy database URL.

    Returns:
        The engine, metadata, and tables associated with
        the database definition.

    """
    engine = backend.engine if url == backend.engine.url else sa.create_engine(url)
    metadata = sa.MetaData()
    submissions = sa.Table(
        "submissions",
        metadata,
        sa.Column("cik", sa.String, primary_key=True, doc="Unique SEC ID."),
        sa.Column(
            "entity_type", sa.String, doc="Type of company standing (e.g., operating)."
        ),
        sa.Column("sic", sa.String, doc="Industry code."),
        sa.Column("sic_description", sa.String, doc="Industry code description."),
        sa.Column(
            "insider_transaction_for_owner_exists",
            sa.Integer,
            doc="Whether owner insider transactions data exists.",
        ),
        sa.Column(
            "insider_transaction_for_issuer_exists",
            sa.Integer,
            doc="Whether issuer insider transactions data exists.",
        ),
        sa.Column("name", sa.String, doc="Company name."),
        sa.Column(
            "tickers",
            sa.String,
            doc="Comma-separated tickers/symbols the company uses.",
        ),
        sa.Column(
            "exchanges",
            sa.String,
            doc="Comma-separated exchanges the company is found on.",
        ),
        sa.Column("ein", sa.String, doc="Entity identification number."),
        sa.Column(
            "description", sa.String, doc="Entity description (often empty/null)."
        ),
        sa.Column("website", sa.String, doc="Company website (often empty/null)."),
        sa.Column(
            "investor_website", sa.String, doc="Investor website (often empty/null)."
        ),
        sa.Column("category", sa.String, doc="SEC entity category."),
        sa.Column(
            "fiscal_year_end",
            sa.String,
            doc="The company's last day of the fiscal year (MMDD).",
        ),
        sa.Column(
            "state_of_incorporation", sa.String, doc="Official incorporation state."
        ),
        sa.Column(
            "state_of_incorporation_description",
            sa.String,
            doc="State of incorporation description.",
        ),
    )

    tags = sa.Table(
        "tags",
        metadata,
        sa.Column("cik", sa.String, primary_key=True, doc="Unique SEC ID."),
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
        sa.Column("form", sa.String, doc="Submission form type (e.g., 10-Q)."),
        sa.Column(
            "units",
            sa.String,
            doc="Unit of measurements for tag value (e.g., USD or shares).",
        ),
        sa.Column("fy", sa.Integer, doc="Fiscal year the submission is for."),
        sa.Column(
            "fp", sa.String, doc="Fiscal period the submission is for (e.g., Q1 or FY)."
        ),
        sa.Column(
            "start",
            sa.String,
            nullable=True,
            doc="When the tag's value's measurements started.",
        ),
        sa.Column("end", sa.String, doc="When the tag's value's measurements ended."),
        sa.Column(
            "filed",
            sa.String,
            primary_key=True,
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
        sa.Column("value", sa.Float, doc="Tag value with units `units`."),
    )

    quarterly_features = sa.Table(
        "quarterly_features",
        metadata,
        sa.Column("cik", sa.String, primary_key=True, doc="Unique company ticker."),
        sa.Column("filed", sa.String, primary_key=True, doc="Filing date."),
        sa.Column("name", sa.String, primary_key=True, doc="Feature name."),
        sa.Column("value", sa.Float, doc="Feature value."),
    )

    return (engine, metadata), (submissions, tags, quarterly_features)


(engine, metadata), (submissions, tags, quarterly_features) = _define_db()


@cache
def get_ticker_set() -> set[str]:
    """Get all unique tickers in the raw SQL tables."""
    with engine.begin() as conn:
        tickers = set()
        for cik in conn.execute(sa.select(tags.c.cik).distinct()):
            (cik,) = cik
            ticker = api.get_ticker(str(cik))
            tickers.add(ticker)
    return tickers
