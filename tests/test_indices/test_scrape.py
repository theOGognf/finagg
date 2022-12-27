import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

import shark


@pytest.fixture
def resources() -> tuple[Engine, MetaData]:
    yield from shark.testing.sqlite_resources(
        shark.backend.database_path, creator=shark.indices.sql.define_db
    )


def test_run(resources: tuple[Engine, MetaData]) -> None:
    engine, _ = resources
    tickers_to_inserts = shark.indices.scrape.run(engine=engine)
    assert sum(tickers_to_inserts.values()) > 0
