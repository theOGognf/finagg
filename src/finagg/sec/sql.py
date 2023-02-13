"""SEC SQLAlchemy interfaces."""

from functools import cache

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    select,
)
from sqlalchemy.engine import Engine

from .. import backend
from . import api


def _define_db(
    url: str = backend.database_url,
) -> tuple[tuple[Engine, MetaData], tuple[Table, ...]]:
    """Utility method for defining the SQLAlchemy elements.

    Used for the main SQL tables and for creating test
    databases.

    Args:
        url: SQLAlchemy database URL.

    Returns:
        The engine, metadata, and tables associated with
        the database definition.

    """
    engine = backend.engine if url == backend.engine.url else create_engine(url)
    metadata = MetaData()
    submissions = Table(
        "submissions",
        metadata,
        Column("cik", String, primary_key=True, doc="Unique SEC ID."),
        Column(
            "entity_type", String, doc="Type of company standing (e.g., operating)."
        ),
        Column("sic", String, doc="Industry code."),
        Column("sic_description", String, doc="Industry code description."),
        Column(
            "insider_transaction_for_owner_exists",
            Integer,
            doc="Whether owner insider transactions data exists.",
        ),
        Column(
            "insider_transaction_for_issuer_exists",
            Integer,
            doc="Whether issuer insider transactions data exists.",
        ),
        Column("name", String, doc="Company name."),
        Column(
            "tickers", String, doc="Comma-separated tickers/symbols the company uses."
        ),
        Column(
            "exchanges",
            String,
            doc="Comma-separated exchanges the company is found on.",
        ),
        Column("ein", String, doc="Entity identification number."),
        Column("description", String, doc="Entity description (often empty/null)."),
        Column("website", String, doc="Company website (often empty/null)."),
        Column("investor_website", String, doc="Investor website (often empty/null)."),
        Column("category", String, doc="SEC entity category."),
        Column(
            "fiscal_year_end",
            String,
            doc="The company's last day of the fiscal year (MMDD).",
        ),
        Column("state_of_incorporation", String, doc="Official incorporation state."),
        Column(
            "state_of_incorporation_description",
            String,
            doc="State of incorporation description.",
        ),
    )

    tags = Table(
        "tags",
        metadata,
        Column("cik", String, primary_key=True, doc="Unique SEC ID."),
        Column("accn", String, doc="Unique submission/access number."),
        Column(
            "taxonomy", String, doc="XBRL taxonomy the submission's tag belongs to."
        ),
        Column(
            "tag",
            String,
            primary_key=True,
            doc="XBRL submission tag (e.g., NetIncomeLoss).",
        ),
        Column("form", String, doc="Submission form type (e.g., 10-Q)."),
        Column(
            "units",
            String,
            doc="Unit of measurements for tag value (e.g., USD or shares).",
        ),
        Column("fy", Integer, doc="Fiscal year the submission is for."),
        Column(
            "fp", String, doc="Fiscal period the submission is for (e.g., Q1 or FY)."
        ),
        Column(
            "start",
            String,
            nullable=True,
            doc="When the tag's value's measurements started.",
        ),
        Column("end", String, doc="When the tag's value's measurements ended."),
        Column(
            "filed",
            String,
            primary_key=True,
            doc="When the submission was actually filed.",
        ),
        Column(
            "frame",
            String,
            nullable=True,
            doc="Often a concatenation of `fy` and `fp`.",
        ),
        Column(
            "label", String, nullable=True, doc="More human readable version of `tag`."
        ),
        Column(
            "description",
            String,
            nullable=True,
            doc="Long description of `tag` and `label`.",
        ),
        Column("entity", String, doc="Company name."),
        Column("value", Float, doc="Tag value with units `units`."),
    )
    return (engine, metadata), (submissions, tags)


(engine, metadata), (submissions, tags) = _define_db()


@cache
def get_ticker_set() -> set[str]:
    """Get all unique tickers in the raw SQL tables."""
    with engine.begin() as conn:
        tickers = set()
        for cik in conn.execute(select(tags.c.cik).distinct()):
            (cik,) = cik
            ticker = api.get_ticker(str(cik))
            tickers.add(ticker)
    return tickers
