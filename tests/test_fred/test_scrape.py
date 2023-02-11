import pytest
from sqlalchemy.engine import Engine

import finagg


@pytest.fixture
def resources() -> Engine:
    yield from finagg.testing.sqlite_resources(
        finagg.backend.database_path, creator=finagg.fred.sql._define_db
    )


def test_run(engine: Engine) -> None:
    tickers_to_inserts = finagg.fred.scrape.run(
        finagg.fred.features.economic_features.series_ids, engine=engine
    )
    assert sum(tickers_to_inserts.values()) > 0
