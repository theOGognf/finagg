import pandas as pd
import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import shark
from shark.sec.store import _SQL_DB_PATH
from shark.testing import yield_sqlite_test_resources


@pytest.fixture
def resources() -> tuple[Engine, MetaData]:
    yield from yield_sqlite_test_resources(
        _SQL_DB_PATH, creator=shark.sec.store.define_db
    )


def test_quarterly_features_to_from_store(resources: tuple[Engine, MetaData]) -> None:
    engine, metadata = resources
    df1 = shark.sec.features.quarterly_features.from_api("AAPL")
    shark.sec.features.quarterly_features.to_store(
        "AAPL", df1, engine=engine, metadata=metadata
    )
    with pytest.raises(IntegrityError):
        shark.sec.features.quarterly_features.to_store(
            "AAPL", df1, engine=engine, metadata=metadata
        )

    df2 = shark.sec.features.quarterly_features.from_store(
        "AAPL", engine=engine, metadata=metadata
    )
    pd.testing.assert_frame_equal(df1, df2)
