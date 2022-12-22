"""Testing utils."""

import pathlib
from typing import Callable, Generator

from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.engine import Engine, Inspector

_CREATOR = Callable[
    [
        str,
    ],
    tuple[tuple[Engine, MetaData], Inspector, tuple[Table]],
]


def sqlite_resources(
    path: str, /, *, creator: None | _CREATOR = None
) -> Generator[tuple[Engine, MetaData], None, None]:
    """Yield a test database engine that's cleaned-up after
    usage.

    Args:
        path: Path to SQLite database file.
        creator: Callable for creating the database and related
            SQLAlchemy objects from a database URL.

    Returns:
        A database engine that's subsequently disposed of
        and whose respective database file is deleted
        after use. A metadata object is also returned for
        convenience.

    Examples:
        # Using the testing util as a pytest fixture
        >>> import pytest
        >>> from sqlalchemy import MetaData
        >>> from sqlalchemy.engine import Engine
        >>>
        >>> import shark
        >>>
        >>> @pytest.fixture
        ... def resources() -> tuple[Engine, MetaData]:
        ...     yield from shark.testing.sqlite_resources("/path/to/db.sqlite")
        ...

    """
    path_obj = pathlib.Path(path)
    path_obj = path_obj.with_stem(f"{path_obj.stem}_test")
    url = f"sqlite:///{path_obj}"
    if creator is None:
        engine = create_engine(url)
        metadata = MetaData()
    else:
        (engine, metadata), _, _ = creator(url)
    yield engine, metadata
    engine.dispose()
    path_obj.unlink()
