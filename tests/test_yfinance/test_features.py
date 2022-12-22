import pandas as pd
import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import shark
from shark.yfinance.store import _DATABASE_PATH


@pytest.fixture
def resources() -> tuple[Engine, MetaData]:
    yield from shark.testing.sqlite_resources(
        _DATABASE_PATH, creator=shark.yfinance.store.define_db
    )


def test_daily_features_to_from_store(resources: tuple[Engine, MetaData]) -> None:
    engine, metadata = resources
    df1 = shark.yfinance.features.daily_features.from_api("AAPL")
    shark.yfinance.features.daily_features.to_store(
        "AAPL", df1, engine=engine, metadata=metadata
    )
    with pytest.raises(IntegrityError):
        shark.yfinance.features.daily_features.to_store(
            "AAPL", df1, engine=engine, metadata=metadata
        )

    df2 = shark.yfinance.features.daily_features.from_store(
        "AAPL", engine=engine, metadata=metadata
    )
    pd.testing.assert_frame_equal(df1, df2)
