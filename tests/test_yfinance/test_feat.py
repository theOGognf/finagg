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


def test_daily_candidate_ticker_set(engine: Engine) -> None:
    finagg.yfinance.feat.prices.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.yfinance.sql.get_ticker_set(engine=engine)
    assert "AAPL" in finagg.yfinance.feat.daily.get_candidate_ticker_set(engine=engine)


def test_daily_ticker_set(engine: Engine) -> None:
    finagg.yfinance.feat.prices.install({"AAPL"}, engine=engine)
    finagg.yfinance.feat.daily.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.yfinance.feat.daily.get_candidate_ticker_set(engine=engine)
    assert "AAPL" in finagg.yfinance.feat.daily.get_ticker_set(engine=engine)


def test_daily_to_from_refined(engine: Engine) -> None:
    df1 = finagg.yfinance.feat.daily.from_api("AAPL")
    finagg.yfinance.feat.daily.to_refined("AAPL", df1, engine=engine)
    with pytest.raises(IntegrityError):
        finagg.yfinance.feat.daily.to_refined("AAPL", df1, engine=engine)

    df2 = finagg.yfinance.feat.daily.from_refined("AAPL", engine=engine)
    pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
