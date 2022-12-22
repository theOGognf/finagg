import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

import shark
from shark.sec.sql import _DATABASE_PATH


@pytest.fixture
def resources() -> tuple[Engine, MetaData]:
    yield from shark.testing.sqlite_resources(
        _DATABASE_PATH, creator=shark.sec.sql.define_db
    )


def test_run(resources: tuple[Engine, MetaData]) -> None:
    engine, _ = resources
    tickers_to_inserts = shark.sec.scrape.run(
        "AAPL", concepts=shark.sec.features.quarterly_features.concepts, engine=engine
    )
    assert sum(tickers_to_inserts.values()) > 0
