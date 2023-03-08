import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, table=finagg.sec.sql.quarterly
    )


def test_quarterly_to_from_refined(engine: Engine) -> None:
    df1 = finagg.sec.feat.quarterly.from_api("AAPL")
    finagg.sec.feat.quarterly.to_refined("AAPL", df1, engine=engine)
    with pytest.raises(IntegrityError):
        finagg.sec.feat.quarterly.to_refined("AAPL", df1, engine=engine)

    df2 = finagg.sec.feat.quarterly.from_refined("AAPL", engine=engine)
    pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
