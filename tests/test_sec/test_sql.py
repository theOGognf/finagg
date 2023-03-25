import pytest
from sqlalchemy.engine import Engine

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, metadata=finagg.sec.sql.metadata
    )


def test_get_cik(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    assert finagg.sec.sql.get_cik("AAPL", engine=engine) == "0000320193"


def test_get_metadata(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    assert (
        finagg.sec.sql.get_metadata(ticker="AAPL", engine=engine)["cik"] == "0000320193"
    )


def test_get_ticker(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    assert finagg.sec.sql.get_ticker("0000320193", engine=engine) == "AAPL"


def test_get_ticker_set(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    finagg.sec.feat.tags.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.sec.sql.get_ticker_set(engine=engine)


def test_get_tickers_in_industry(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"HD", "LOW"}, engine=engine)
    assert "LOW" in finagg.sec.sql.get_tickers_in_industry(ticker="HD", engine=engine)
