import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, table=finagg.yfinance.sql.daily
    )


def test_daily_all_equal(engine: Engine) -> None:
    finagg.yfinance.feat.prices.install({"AAPL"}, engine=engine)
    finagg.yfinance.feat.daily.install({"AAPL"}, engine=engine)
    df1 = finagg.yfinance.feat.daily.from_api("AAPL").head(5)
    df2 = finagg.yfinance.feat.daily.from_raw("AAPL", engine=engine).head(5)
    df3 = finagg.yfinance.feat.daily.from_refined("AAPL", engine=engine).head(5)
    pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
    pd.testing.assert_frame_equal(df1, df3, rtol=1e-4)


def test_daily_get_candidate_ticker_set(engine: Engine) -> None:
    finagg.yfinance.feat.prices.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.yfinance.feat.prices.get_ticker_set(engine=engine)
    assert "AAPL" in finagg.yfinance.feat.daily.get_candidate_ticker_set(engine=engine)


def test_daily_get_candidate_ticker_set_empty(engine: Engine) -> None:
    assert len(finagg.yfinance.feat.daily.get_candidate_ticker_set(engine=engine)) == 0


def test_daily_get_ticker_set(engine: Engine) -> None:
    finagg.yfinance.feat.prices.install({"AAPL"}, engine=engine)
    finagg.yfinance.feat.daily.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.yfinance.feat.daily.get_candidate_ticker_set(engine=engine)
    assert "AAPL" in finagg.yfinance.feat.daily.get_ticker_set(engine=engine)


def test_daily_get_ticker_set_empty(engine: Engine) -> None:
    assert len(finagg.yfinance.feat.daily.get_ticker_set(engine=engine)) == 0


def test_daily_to_from_refined(engine: Engine) -> None:
    df1 = finagg.yfinance.feat.daily.from_api("AAPL")
    finagg.yfinance.feat.daily.to_refined("AAPL", df1, engine=engine)
    with pytest.raises(IntegrityError):
        finagg.yfinance.feat.daily.to_refined("AAPL", df1, engine=engine)

    df2 = finagg.yfinance.feat.daily.from_refined("AAPL", engine=engine)
    pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)


def test_prices_get_ticker_set(engine: Engine) -> None:
    finagg.yfinance.feat.prices.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.yfinance.feat.prices.get_ticker_set(engine=engine)
