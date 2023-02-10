import pandas as pd
import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import finagg


@pytest.fixture
def resources() -> tuple[Engine, MetaData]:
    yield from finagg.testing.sqlite_resources(
        finagg.backend.database_path, creator=finagg.yfinance.store._define_db
    )


def test_daily_features_to_from_store(resources: tuple[Engine, MetaData]) -> None:
    engine, _ = resources
    df1 = finagg.yfinance.features.daily_features.from_api("AAPL")
    finagg.yfinance.features.daily_features.to_store("AAPL", df1, engine=engine)
    with pytest.raises(IntegrityError):
        finagg.yfinance.features.daily_features.to_store("AAPL", df1, engine=engine)

    df2 = finagg.yfinance.features.daily_features.from_store("AAPL", engine=engine)
    pd.testing.assert_frame_equal(df1, df2)
