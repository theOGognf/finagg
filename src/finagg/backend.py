"""Backend API cache and database constants/definitions."""

import os
import pathlib

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Inspector

#: Path to parent directory of the `setup.py`.
project_path = pathlib.Path(__file__).resolve().parent.parent.parent

#: Path to requests cache backend. Override to change the cache location.
#: All submodules with APIs share the same cache backend.
http_cache_path = os.environ.get(
    "FINAGG_HTTP_CACHE_PATH", project_path / "data" / "http_cache"
)

#: Path/URL to data storage backend. Override to change the backend database location or type.
#: All submodules with databases share the same database.
database_path = project_path / "data" / "finagg.sqlite"
database_url = os.environ.get("FINAGG_DATABASE_URL", f"sqlite:///{database_path}")

#: SQLAlchemy engine and respective engine inspector. Used by all submodules
#: for creating/reading/writing.
engine = create_engine(database_url)
inspector: Inspector = inspect(engine)
