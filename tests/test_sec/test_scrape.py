import pytest
from sqlalchemy.engine import Engine

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, creator=finagg.sec.sql._define_db
    )


def test_run(engine: Engine) -> None:
    tickers_to_inserts = finagg.sec.scrape.run(
        "AAPL", concepts=finagg.sec.features.quarterly_features.concepts, engine=engine
    )
    assert sum(tickers_to_inserts.values()) > 0
