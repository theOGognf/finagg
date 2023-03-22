import pytest
from sqlalchemy.engine import Engine

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, table=finagg.yfinance.sql.daily
    )


def test_get_ticker_set(engine: Engine) -> None:
    finagg.yfinance.feat.prices.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.yfinance.sql.get_ticker_set(engine=engine)
