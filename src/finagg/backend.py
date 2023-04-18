""":mod:`finagg` configuration and global SQLAlchemy setup. Backend file paths
and SQLAlchemy engine database URLs are configured in this module at runtime
according to environment variables.

Environment variables should ideally be configured using an ``.env`` file
in the desired working directory. Running ``finagg install`` will
automaticaly setup the ``.env`` file for you according to your input values.
Environment variables assigned in the ``.env`` file are loaded on the
:mod:`finagg` module's first instantiation.

"""

import os
import pathlib

from sqlalchemy import create_engine

root_path = pathlib.Path(os.environ.get("FINAGG_ROOT_PATH", pathlib.Path.cwd()))
"""Parent directory of the ``findata`` directory where the backend database
and API cache file will be stored (unless otherwise configured according
to the relevant environment variables). This can be set with the
``FINAGG_ROOT_PATH`` environment variable. This defaults to and is typically
set to the current working directory. It's recommended you permanently set
this value using the ``finagg install`` CLI.

:meta hide-value:
"""

http_cache_path = pathlib.Path(
    os.environ.get("FINAGG_HTTP_CACHE_PATH", root_path / "findata" / "http_cache")
)
"""Path to the API cache file. This can be set with the
``FINAGG_HTTP_CACHE_PATH`` environment variable and should NOT include a file
extension. All API implementations share the same cache backend.

:meta hide-value:
"""

database_path = root_path / "findata" / "finagg.sqlite"
"""Default path to the database file. The ``FINAGG_DATABASE_URL`` environment
variable will take precedence over this value.

:meta hide-value:
"""

database_url = os.environ.get("FINAGG_DATABASE_URL", f"sqlite:///{database_path}")
"""SQLAlchemy URL to the database. This can be set with the
``FINAGG_DATABASE_URL`` environment variable and should include a file extension.
This defaults to ``f"sqlite:///{finagg.backend.database_path}"``.

:meta hide-value:
"""

engine = create_engine(database_url)
"""The default SQLAlchemy engine for the backend database. All feature and SQL
submodules use this engine and the database URL as configured by
:data:`database_url` for reading and writing to and from the database by default.

:meta hide-value:
"""
