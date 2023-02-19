"""Testing utils."""

import pathlib
from typing import Generator

import sqlalchemy as sa
from sqlalchemy.engine import Engine


def sqlite_engine(
    path: str, /, *, metadata: None | sa.MetaData = None
) -> Generator[Engine, None, None]:
    """Yield a test database engine that's cleaned-up after
    usage.

    Args:
        path: Path to SQLite database file.
        metadata: Optional metadata for creating and dropping
            tables before and after yielding the engine,
            respectively.

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
        ... def engine() -> Engine:
        ...     yield from finagg.testing.sqlite_engine("/path/to/db.sqlite")
        ...

    """
    path_obj = pathlib.Path(path)
    path_obj = path_obj.with_stem(f"{path_obj.stem}_test")
    url = f"sqlite:///{path_obj}"
    engine = sa.create_engine(url)
    if metadata:
        metadata.create_all(engine)
    yield engine
    if metadata:
        metadata.drop_all(engine)
    engine.dispose()
    path_obj.unlink()
