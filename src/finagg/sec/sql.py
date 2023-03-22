"""SEC SQLAlchemy interfaces."""

from typing import Any, Literal

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from .. import backend

metadata = sa.MetaData()

submissions = sa.Table(
    "sec.raw.submissions",
    metadata,
    sa.Column("cik", sa.String, primary_key=True, doc="Unique SEC ID."),
    sa.Column("ticker", sa.String, nullable=False, doc="Company ticker."),
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
    "sec.raw.tags",
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

quarterly = sa.Table(
    "sec.refined.quarterly",
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


normalized_quarterly = sa.Table(
    "sec.refined.quarterly.normalized",
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


def get_cik(ticker: str, /, *, engine: None | Engine = None) -> str:
    """Use raw SQL data to find a company's SEC CIK from its ticker symbol.

    This is the preferred method for getting a company's SEC CIK if raw SQL
    data is installed. This method is a convenience over
    :meth:`finagg.sec.api.get_cik` for repeatedly getting company SEC
    CIKs without having to query the SEC EDGAR API. Use
    :meth:`finagg.sec.api.get_cik` if you want to get a company's ticker
    symbol without installing or accessing locally installed raw SQL data.

    Args:
        ticker: A company's ticker symbol.
        engine: Feature store database engine. Defaults to the engine
            at :data:`finagg.backend.engine`.

    Returns:
        The company's corresponding SEC CIK.

    Examples:
        Get Apple's SEC CIK from its ticker.

        >>> finagg.sec.sql.get_cik("AAPL") == "0000320193"
        True

    """
    engine = engine or backend.engine
    with engine.begin() as conn:
        (cik,) = conn.execute(
            sa.select(submissions.c.cik).where(submissions.c.ticker == ticker)
        ).one()
    return str(cik)


def get_metadata(
    *, cik: None | str = None, ticker: None | str = None, engine: None | Engine = None
) -> dict[str, Any]:
    """Return a company's metadata (its SEC CIK, ticker, name, and industry
    code) from its SEC CIK or its ticker symbol.

    A convenient method for getting a company's metadata using raw SQL
    data. This method is a convenience over
    :data:`finagg.sec.api.submissions` for repeatedly getting company
    metadata without having to query the SEC EDGAR API. Use
    :data:`finagg.sec.api.submissions` if you want to get a company's
    metadata without installing or accessing locally installed raw SQL data.

    Args:
        cik: Company SEC CIK. Mutually exclusive with ``ticker``.
        ticker: Company ticker. Mutually exclusive with ``cik``.
        engine: Feature store database engine. Defaults to the engine
            at :data:`finagg.backend.engine`.

    Returns:
        A company's metadata as a dictionary.

    Examples:
        >>> finagg.sec.sql.get_metadata(ticker="MSFT")
        {'cik': '0000789019', 'ticker': 'MSFT', 'name': 'microsoft corp', 'sic': '7372'}

    """
    engine = engine or backend.engine

    if bool(cik) == bool(ticker):
        raise ValueError("Must provide a `cik` or a `ticker`.")

    if ticker:
        cik = str(get_cik(ticker, engine=engine))

    with engine.begin() as conn:
        row = conn.execute(
            sa.select(
                submissions.c.cik,
                submissions.c.ticker,
                submissions.c.name,
                submissions.c.sic,
            ).where(submissions.c.cik == cik)
        ).one()
    return row._asdict()


def get_ticker(cik: str, /, *, engine: None | Engine = None) -> str:
    """Use raw SQL data to find a company's ticker from its SEC CIK.

    This is the preferred method for getting a company's ticker if raw SQL
    data is installed. This method is a convenience over
    :meth:`finagg.sec.api.get_ticker` for repeatedly getting company tickers
    without having to query the SEC EDGAR API. Use
    :meth:`finagg.sec.api.get_ticker` if you want to get a company's
    ticker symbol without installing or accessing locally installed raw
    SQL data.

    Args:
        cik: A company's SEC CIK.
        engine: Feature store database engine. Defaults to the engine
            at :data:`finagg.backend.engine`.

    Returns:
        The company's corresponding ticker symbol.

    Examples:
        Get Apple's ticker from its SEC CIK.

        >>> finagg.sec.sql.get_ticker("0000320193") == "AAPL"
        True

    """
    engine = engine or backend.engine
    with engine.begin() as conn:
        (ticker,) = conn.execute(
            sa.select(submissions.c.ticker).where(submissions.c.cik == cik)
        ).one()
    return str(ticker)


def get_ticker_set(
    lb: int = 1,
    *,
    engine: None | Engine = None,
) -> set[str]:
    """Get all unique ticker symbols in the raw SQL tables that have at least
    ``lb`` rows.

    This method is convenient for accessing the tickers that have raw SQL data
    associated with them so the data associated with those tickers can be
    further refined. A common pattern is to use this method and other
    ``get_ticker_set`` methods (such as those found in :mod:`finagg.sec.feat`)
    to determine which tickers are missing data from other tables or features.

    Args:
        lb: Lower bound number of rows that a company must have for its ticker
            to be included in the set returned by this method.
        engine: Feature store database engine. Defaults to the engine
            at :data:`finagg.backend.engine`.

    Examples:
        >>> "AAPL" in finagg.sec.sql.get_ticker_set()
        True

    """
    engine = engine or backend.engine
    with engine.begin() as conn:
        tickers = (
            conn.execute(
                sa.select(submissions.c.ticker)
                .join(tags, tags.c.cik == submissions.c.cik)
                .group_by(tags.c.cik)
                .having(sa.func.count(tags.c.filed) >= lb)
            )
            .scalars()
            .all()
        )
    return set(tickers)


def get_tickers_in_industry(
    *,
    ticker: None | str = None,
    code: None | str = None,
    level: Literal[2, 3, 4] = 2,
    engine: None | Engine = None,
) -> set[str]:
    """Get a set of tickers that all share the same industry using raw SQL data.

    This method is convenient for finding tickers within the same
    industry so they can be compared. A related and common pattern is to use
    :data:`finagg.sec.feat.quarterly.normalized` to get industry-normalized
    features for a particular company. Similar to other methods in this
    submodule, this will only return tickers that have raw SQL data associated
    with them.

    Args:
        ticker: Company ticker. Lookup the industry associated
            with this company. Mutually exclusive with ``code``.
        code: Industry SIC code to use for industry lookup.
            Mutually exclusive with ``ticker``.
        level: Industry level to find tickers within.
            The industry used according to ``ticker`` or ``code``
            is subsampled according to this value. Options include:

                - 2 = major group (e.g., furniture and fixtures)
                - 3 = industry group (e.g., office furnitures)
                - 4 = industry (e.g., wood office furniture)

        engine: Feature store database engine. Defaults to the engine
            at :data:`finagg.backend.engine`.

    Returns:
        A set of tickers that all share the same industry as denoted by
        another ticker (using the ``ticker`` arg) or an industry SIC code
        (using the ``code`` arg).

    Examples:
        >>> "ETSY" in finagg.sec.sql.get_tickers_in_industry(ticker="MSFT")
        True

    """
    engine = engine or backend.engine
    with engine.begin() as conn:
        if ticker:
            (sic,) = conn.execute(
                sa.select(submissions.c.sic).where(submissions.c.ticker == ticker)
            ).one()
            code = str(sic)[:level]
        elif code:
            code = str(code)[:level]
        else:
            raise ValueError("Must provide a `ticker` or `code`.")

        tickers = (
            conn.execute(
                sa.select(submissions.c.ticker).where(
                    submissions.c.sic.startswith(code)
                )
            )
            .scalars()
            .all()
        )
    return set(tickers)
