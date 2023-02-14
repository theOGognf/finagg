import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, creator=finagg.fred.store._define_db
    )


def test_economic_features_to_from_store(engine: Engine) -> None:
    df1 = finagg.fred.features.economic.from_api()
    finagg.fred.features.economic.to_store(
        df1,
        engine=engine,
    )
    with pytest.raises(IntegrityError):
        finagg.fred.features.economic.to_store(
            df1,
            engine=engine,
        )

    df2 = finagg.fred.features.economic.from_store(
        engine=engine,
    )
    pd.testing.assert_frame_equal(df1, df2)
