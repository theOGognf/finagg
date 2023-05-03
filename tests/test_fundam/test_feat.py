import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, metadata=finagg.fundam.sql.metadata
    )


def test_fundam_get_candidate_ticker_set(engine: Engine) -> None:
    finagg.yfinance.feat.prices.install({"AAPL"}, engine=engine)
    finagg.yfinance.feat.daily.install({"AAPL"}, engine=engine)
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    finagg.sec.feat.tags.install({"AAPL"}, engine=engine)
    finagg.sec.feat.quarterly.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.fundam.feat.fundam.get_candidate_ticker_set(engine=engine)


def test_fundam_get_candidate_ticker_set_empty(engine: Engine) -> None:
    assert len(finagg.fundam.feat.fundam.get_candidate_ticker_set(engine=engine)) == 0


def test_fundam_get_ticker_set(engine: Engine) -> None:
    finagg.yfinance.feat.prices.install({"AAPL"}, engine=engine)
    finagg.yfinance.feat.daily.install({"AAPL"}, engine=engine)
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    finagg.sec.feat.tags.install({"AAPL"}, engine=engine)
    finagg.sec.feat.quarterly.install({"AAPL"}, engine=engine)
    finagg.fundam.feat.fundam.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.fundam.feat.fundam.get_ticker_set(engine=engine)


def test_fundam_get_ticker_set_empty(engine: Engine) -> None:
    assert len(finagg.fundam.feat.fundam.get_ticker_set(engine=engine)) == 0


def test_fundam_to_from_refined(engine: Engine) -> None:
    df1 = finagg.fundam.feat.fundam.from_api("AAPL")
    finagg.fundam.feat.fundam.to_refined(
        "AAPL",
        df1,
        engine=engine,
    )
    with pytest.raises(IntegrityError):
        finagg.fundam.feat.fundam.to_refined(
            "AAPL",
            df1,
            engine=engine,
        )

    df2 = finagg.fundam.feat.fundam.from_refined(
        "AAPL",
        engine=engine,
    )
    pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
