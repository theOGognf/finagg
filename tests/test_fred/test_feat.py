import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, table=finagg.fred.sql.economic
    )


def test_economic_all_equal(engine: Engine) -> None:
    finagg.fred.feat.series.install(engine=engine)
    finagg.fred.feat.economic.install(engine=engine)
    df1 = finagg.fred.feat.economic.from_api().head(5)
    df2 = finagg.fred.feat.economic.from_raw(engine=engine).head(5)
    df3 = finagg.fred.feat.economic.from_refined(engine=engine).head(5)
    pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
    pd.testing.assert_frame_equal(df1, df3, rtol=1e-4)


def test_economic_to_from_refined(engine: Engine) -> None:
    df1 = finagg.fred.feat.economic.from_api()
    finagg.fred.feat.economic.to_refined(
        df1,
        engine=engine,
    )
    with pytest.raises(IntegrityError):
        finagg.fred.feat.economic.to_refined(
            df1,
            engine=engine,
        )

    df2 = finagg.fred.feat.economic.from_refined(
        engine=engine,
    )
    pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
