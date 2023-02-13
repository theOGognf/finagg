import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, creator=finagg.sec.store._define_db
    )


def test_quarterly_features_to_from_store(engine: Engine) -> None:
    df1 = finagg.sec.features.quarterly_features.from_api("AAPL")
    finagg.sec.features.quarterly_features.to_store("AAPL", df1, engine=engine)
    with pytest.raises(IntegrityError):
        finagg.sec.features.quarterly_features.to_store("AAPL", df1, engine=engine)

    df2 = finagg.sec.features.quarterly_features.from_store("AAPL", engine=engine)
    pd.testing.assert_frame_equal(df1, df2)
