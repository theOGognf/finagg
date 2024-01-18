from datetime import datetime, timedelta

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


def test_daily_update(engine: Engine) -> None:
    install_end = datetime.fromisoformat(finagg.utils.today) - timedelta(days=14)
    finagg.yfinance.feat.prices.install({"AAPL"}, engine=engine)
    df = finagg.yfinance.feat.daily.from_api(
        "AAPL", end=install_end.strftime("%Y-%m-%d")
    )
    finagg.yfinance.feat.daily.to_refined("AAPL", df, engine=engine)
    finagg.yfinance.feat.daily.update({"AAPL"}, engine=engine)
    update_end = install_end + timedelta(days=7)
    df1 = finagg.yfinance.feat.daily.from_api(
        "AAPL", end=update_end.strftime("%Y-%m-%d")
    )
    df2 = finagg.yfinance.feat.daily.from_refined(
        "AAPL", end=update_end.strftime("%Y-%m-%d"), engine=engine
    )
    pd.testing.assert_frame_equal(df1, df2, atol=1e-4)


def test_prices_get_ticker_set(engine: Engine) -> None:
    finagg.yfinance.feat.prices.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.yfinance.feat.prices.get_ticker_set(engine=engine)


def test_prices_update(engine: Engine) -> None:
    install_end = datetime.fromisoformat(finagg.utils.today) - timedelta(days=14)
    df = finagg.yfinance.api.get("AAPL", end=install_end.strftime("%Y-%m-%d"))
    finagg.yfinance.feat.prices.to_raw(df, engine=engine)
    finagg.yfinance.feat.prices.update({"AAPL"}, engine=engine)
    update_end = install_end + timedelta(days=7)
    df1 = finagg.yfinance.api.get("AAPL", end=update_end.strftime("%Y-%m-%d"))
    df2 = finagg.yfinance.feat.prices.from_raw(
        "AAPL", end=update_end.strftime("%Y-%m-%d"), engine=engine
    ).reset_index()
    df2["ticker"] = "AAPL"
    pd.testing.assert_frame_equal(df1, df2, atol=1e-4)
