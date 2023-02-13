"""SQLAlchemy interfaces for mixed features."""


from sqlalchemy import Column, Float, MetaData, String, Table, create_engine
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
    economic_features = Table(
        "economic_features",
        metadata,
        Column(
            "date",
            String,
            primary_key=True,
            doc="Economic data series release date.",
        ),
        Column("name", String, primary_key=True, doc="Feature name."),
        Column("value", Float, doc="Feature value."),
    )
    return (engine, metadata), (economic_features,)


(engine, metadata), (economic_features,) = _define_db()
