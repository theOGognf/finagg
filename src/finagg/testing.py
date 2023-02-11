"""Testing utils."""

import pathlib
from typing import Callable, Generator

from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.engine import Engine

_CREATOR = Callable[
    [
        str,
    ],
    tuple[tuple[Engine, MetaData], tuple[Table]],
]


def sqlite_resources(
    path: str, /, *, creator: None | _CREATOR = None
) -> Generator[Engine, None, None]:
    """Yield a test database engine that's cleaned-up after
    usage.

    Args:
        path: Path to SQLite database file.
        creator: Callable for creating the database and related
            SQLAlchemy objects from a database URL.

    Returns:
        A database engine that's subsequently disposed of
        and whose respective database file is deleted
        after use.

    Examples:
        # Using the testing util as a pytest fixture
        >>> import pytest
        >>> from sqlalchemy.engine import Engine
        >>>
        >>> import finagg
        >>>
        >>> @pytest.fixture
        ... def resources() -> Engine:
        ...     yield from finagg.testing.sqlite_resources("/path/to/db.sqlite")
        ...

    """
    path_obj = pathlib.Path(path)
    path_obj = path_obj.with_stem(f"{path_obj.stem}_test")
    url = f"sqlite:///{path_obj}"
    if creator is None:
        engine = create_engine(url)
    else:
        (engine, _), _ = creator(url)
    yield engine
    engine.dispose()
    path_obj.unlink()
