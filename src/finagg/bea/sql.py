"""BEA SQLAlchemy interfaces."""


from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine
from sqlalchemy.engine import Engine

from .. import backend


def _define_db(
    url: str = backend.database_url,
) -> tuple[tuple[Engine, MetaData], tuple[Table, ...]]:
    """Utility method for defining the SQLAlchemy elements.

    Used for the main SQL tables and for creating test
    databases.

    Args:
        url: SQLAlchemy database URL.
        path: Path to database file.

    Returns:
        The engine, metadata, and tables associated with
        the database definition.

    """
    engine = backend.engine if url == backend.engine.url else create_engine(url)
    metadata = MetaData()
    fixed_assets = Table(
        "fixed_assets",
        metadata,
        Column("table_id", String, primary_key=True),
        Column("series_code", String),
        Column("line", Integer, primary_key=True),
        Column("line_description", String),
        Column("year", Integer, primary_key=True),
        Column("metric", String),
        Column("units", String),
        Column("e", Integer),
        Column("value", Float),
    )

    gdp_by_industry = Table(
        "gdp_by_industry",
        metadata,
        Column("table_id", Integer, primary_key=True),
        Column("freq", String),
        Column("year", Integer, primary_key=True),
        Column("quarter", Integer, primary_key=True),
        Column("industry", String, primary_key=True),
        Column("industry_description", String),
        Column("value", Float),
    )

    input_output = Table(
        "input_output",
        metadata,
        Column("table_id", Integer, primary_key=True),
        Column("year", Integer, primary_key=True),
        Column("row_code", String, primary_key=True),
        Column("row_description", String),
        Column("row_type", String),
        Column("col_code", String, primary_key=True),
        Column("col_description", String),
        Column("col_type", String),
        Column("value", Float),
    )

    nipa = Table(
        "nipa",
        metadata,
        Column("table_id", String, primary_key=True),
        Column("series_code", String),
        Column("line", Integer, primary_key=True),
        Column("line_description", String),
        Column("year", Integer, primary_key=True),
        Column("quarter", Integer, primary_key=True),
        Column("metric", String),
        Column("units", String),
        Column("e", Integer),
        Column("value", Float),
    )

    return (
        (engine, metadata),
        (
            fixed_assets,
            gdp_by_industry,
            input_output,
            nipa,
        ),
    )


(
    (engine, metadata),
    (
        fixed_assets,
        gdp_by_industry,
        input_output,
        nipa,
    ),
) = _define_db()
