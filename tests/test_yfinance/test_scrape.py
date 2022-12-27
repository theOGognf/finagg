import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

import finagg


@pytest.fixture
def resources() -> tuple[Engine, MetaData]:
    yield from finagg.testing.sqlite_resources(
        finagg.backend.database_path, creator=finagg.yfinance.sql._define_db
    )


def test_run(resources: tuple[Engine, MetaData]) -> None:
    engine, _ = resources
    tickers_to_inserts = finagg.yfinance.scrape.run("AAPL", engine=engine)
    assert sum(tickers_to_inserts.values()) > 0
