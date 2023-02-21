import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, creator=finagg.fundam.store._define_db
    )


def test_fundamental_features_to_from_refined(engine: Engine) -> None:
    df1 = finagg.fundam.features.fundam.from_api("AAPL")
    finagg.fundam.features.fundam.to_refined(
        "AAPL",
        df1,
        engine=engine,
    )
    with pytest.raises(IntegrityError):
        finagg.fundam.features.fundam.to_refined(
            "AAPL",
            df1,
            engine=engine,
        )

    df2 = finagg.fundam.features.fundam.from_refined(
        "AAPL",
        engine=engine,
    )
    pd.testing.assert_frame_equal(df1, df2)
