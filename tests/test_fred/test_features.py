import pandas as pd
import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import shark


@pytest.fixture
def resources() -> tuple[Engine, MetaData]:
    yield from shark.testing.sqlite_resources(
        shark.backend.database_path, creator=shark.fred.store._define_db
    )


def test_economic_features_to_from_store(resources: tuple[Engine, MetaData]) -> None:
    engine, metadata = resources
    df1 = shark.fred.features.economic_features.from_api()
    shark.fred.features.economic_features.to_store(
        df1, engine=engine, metadata=metadata
    )
    with pytest.raises(IntegrityError):
        shark.fred.features.economic_features.to_store(
            df1, engine=engine, metadata=metadata
        )

    df2 = shark.fred.features.economic_features.from_store(
        engine=engine, metadata=metadata
    )
    pd.testing.assert_frame_equal(df1, df2)
