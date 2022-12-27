"""BEA SQLAlchemy interfaces."""


from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    inspect,
)
from sqlalchemy.engine import Engine, Inspector

from .. import backend


def define_db(
    url: str = backend.database_url,
) -> tuple[tuple[Engine, MetaData], Inspector, tuple[Table, ...]]:
    """Utility method for defining the SQLAlchemy elements.

    Used for the main SQL tables and for creating test
    databases.

    Args:
        url: SQLAlchemy database URL.
        path: Path to database file.

    Returns:
        The engine, engine inspector, metadata, and tables associated with
        the database definition.

    """
    if url != backend.engine.url:
        engine = create_engine(url)
        inspector: Inspector = inspect(engine)
    else:
        engine = backend.engine
        inspector = backend.inspector

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
        inspector,
        (
            fixed_assets,
            gdp_by_industry,
            input_output,
            nipa,
        ),
    )


(
    (engine, metadata),
    inspector,
    (
        fixed_assets,
        gdp_by_industry,
        input_output,
        nipa,
    ),
) = define_db()
