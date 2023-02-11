"""Backend API cache and database constants/definitions."""

import os
import pathlib

from sqlalchemy import create_engine

#: Path to artifact parent directory.
root_path = pathlib.Path(os.environ.get("FINAGG_ROOT_PATH", pathlib.Path.cwd()))

#: Path to requests cache backend. Override to change the cache location.
#: All submodules with APIs share the same cache backend.
http_cache_path = os.environ.get(
    "FINAGG_HTTP_CACHE_PATH", root_path / "findata" / "http_cache"
)

#: Path/URL to data storage backend. Override to change the backend database location or type.
#: All submodules with databases share the same database.
database_path = root_path / "findata" / "finagg.sqlite"
database_url = os.environ.get("FINAGG_DATABASE_URL", f"sqlite:///{database_path}")

#: SQLAlchemy engine for the database. Used by all submodules for
#: creating/reading/writing.
engine = create_engine(database_url)
