import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

import shark
from shark.testing import yield_sqlite_test_resources
from shark.yfinance.sql import _SQL_DB_PATH, define_db


@pytest.fixture
def resources() -> tuple[Engine, MetaData]:
    yield from yield_sqlite_test_resources(_SQL_DB_PATH, creator=define_db)


def test_run(resources: tuple[Engine, MetaData]) -> None:
    engine, _ = resources
    tickers_to_inserts = shark.yfinance.scrape.run("AAPL", engine=engine)
    assert sum(tickers_to_inserts.values()) > 0
