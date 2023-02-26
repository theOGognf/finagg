import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, metadata=finagg.yfinance.sql.metadata
    )


def test_fundam_to_from_refined(engine: Engine) -> None:
    df1 = finagg.fundam.feat.fundam.from_api("AAPL")
    finagg.fundam.feat.fundam.to_refined(
        "AAPL",
        df1,
        engine=engine,
    )
    with pytest.raises(IntegrityError):
        finagg.fundam.feat.fundam.to_refined(
            "AAPL",
            df1,
            engine=engine,
        )

    df2 = finagg.fundam.feat.fundam.from_refined(
        "AAPL",
        engine=engine,
    )
    pd.testing.assert_frame_equal(df1, df2)