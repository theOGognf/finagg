"""Backend API cache and database constants/definitions."""

import os
import pathlib
import sqlite3
from urllib.parse import urlparse

import numpy as np
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


# Adding some aggregation functions.
class NumPyStdAggregate:

    values: list[float]

    def __init__(self) -> None:
        self.values = []

    def step(self, value: float) -> None:
        self.values.append(value)

    def finalize(self) -> float:
        return float(np.std(self.values))


# Custom creator for adding aggregate functions.
# Inspired by https://stackoverflow.com/a/997467.
def creator() -> sqlite3.Connection:
    conn = sqlite3.connect(urlparse(database_url).path)
    conn.create_aggregate("std", 1, NumPyStdAggregate)  # type: ignore[arg-type]
    return conn


#: SQLAlchemy engine for the database. Used by all submodules for
#: creating/reading/writing.
engine = create_engine(database_url, creator=creator)
