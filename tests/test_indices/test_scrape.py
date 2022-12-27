import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

import finagg


@pytest.fixture
def resources() -> tuple[Engine, MetaData]:
    yield from finagg.testing.sqlite_resources(
        finagg.backend.database_path, creator=finagg.indices.sql.define_db
    )


def test_run(resources: tuple[Engine, MetaData]) -> None:
    engine, _ = resources
    tickers_to_inserts = finagg.indices.scrape.run(engine=engine)
    assert sum(tickers_to_inserts.values()) > 0
