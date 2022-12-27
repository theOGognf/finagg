"""SQLAlchemy interfaces for mixed features."""


from sqlalchemy import Column, MetaData, String, Table, create_engine, inspect
from sqlalchemy.engine import Engine, Inspector

from .. import backend


def _define_db(
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
    if inspector.has_table("economic_features"):
        economic_features = Table(
            "economic_features",
            metadata,
            Column(
                "date",
                String,
                primary_key=True,
                doc="Economic data series release date.",
            ),
            autoload_with=engine,
        )
    else:
        economic_features = None
    return (engine, metadata), inspector, (economic_features,)


(engine, metadata), inspector, (economic_features,) = _define_db()
