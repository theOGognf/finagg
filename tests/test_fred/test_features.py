import pandas as pd
import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import finagg


@pytest.fixture
def resources() -> tuple[Engine, MetaData]:
    yield from finagg.testing.sqlite_resources(
        finagg.backend.database_path, creator=finagg.fred.store._define_db
    )


def test_economic_features_to_from_store(resources: tuple[Engine, MetaData]) -> None:
    engine, metadata = resources
    df1 = finagg.fred.features.economic_features.from_api()
    finagg.fred.features.economic_features.to_store(
        df1, engine=engine, metadata=metadata
    )
    with pytest.raises(IntegrityError):
        finagg.fred.features.economic_features.to_store(
            df1, engine=engine, metadata=metadata
        )

    df2 = finagg.fred.features.economic_features.from_store(
        engine=engine, metadata=metadata
    )
    pd.testing.assert_frame_equal(df1, df2)
