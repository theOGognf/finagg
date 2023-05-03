import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

import finagg


@pytest.fixture
def engine() -> Engine:
    yield from finagg.testing.sqlite_engine(
        finagg.backend.database_path, metadata=finagg.sec.sql.metadata
    )


def test_annual_all_equal(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    finagg.sec.feat.tags.install({"AAPL"}, engine=engine)
    finagg.sec.feat.annual.install({"AAPL"}, engine=engine)
    df1 = finagg.sec.feat.annual.from_api("AAPL").head(5)
    df2 = finagg.sec.feat.annual.from_raw("AAPL", engine=engine).head(5)
    df3 = finagg.sec.feat.annual.from_refined("AAPL", engine=engine).head(5)
    pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
    pd.testing.assert_frame_equal(df1, df3, rtol=1e-4)


def test_annual_get_candidate_ticker_set(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    finagg.sec.feat.tags.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.sec.feat.tags.get_ticker_set(engine=engine)
    assert "AAPL" in finagg.sec.feat.annual.get_candidate_ticker_set(engine=engine)


def test_annual_get_candidate_ticker_set_empty(engine: Engine) -> None:
    assert len(finagg.sec.feat.annual.get_candidate_ticker_set(engine=engine)) == 0


def test_annual_get_ticker_set(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    finagg.sec.feat.tags.install({"AAPL"}, engine=engine)
    finagg.sec.feat.annual.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.sec.feat.annual.get_candidate_ticker_set(engine=engine)
    assert "AAPL" in finagg.sec.feat.annual.get_ticker_set(engine=engine)


def test_annual_get_ticker_set_empty(engine: Engine) -> None:
    assert len(finagg.sec.feat.annual.get_ticker_set(engine=engine)) == 0


def test_annual_to_from_refined(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    df1 = finagg.sec.feat.annual.from_api("AAPL")
    finagg.sec.feat.annual.to_refined("AAPL", df1, engine=engine)
    with pytest.raises(IntegrityError):
        finagg.sec.feat.annual.to_refined("AAPL", df1, engine=engine)

    df2 = finagg.sec.feat.annual.from_refined("AAPL", engine=engine)
    pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)


def test_quarterly_all_equal(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    finagg.sec.feat.tags.install({"AAPL"}, engine=engine)
    finagg.sec.feat.quarterly.install({"AAPL"}, engine=engine)
    df1 = finagg.sec.feat.quarterly.from_api("AAPL").head(5)
    df2 = finagg.sec.feat.quarterly.from_raw("AAPL", engine=engine).head(5)
    df3 = finagg.sec.feat.quarterly.from_refined("AAPL", engine=engine).head(5)
    pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
    pd.testing.assert_frame_equal(df1, df3, rtol=1e-4)


def test_quarterly_get_candidate_ticker_set(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    finagg.sec.feat.tags.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.sec.feat.tags.get_ticker_set(engine=engine)
    assert "AAPL" in finagg.sec.feat.quarterly.get_candidate_ticker_set(engine=engine)


def test_quarterly_get_candidate_ticker_set_empty(engine: Engine) -> None:
    assert len(finagg.sec.feat.quarterly.get_candidate_ticker_set(engine=engine)) == 0


def test_quarterly_get_ticker_set(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    finagg.sec.feat.tags.install({"AAPL"}, engine=engine)
    finagg.sec.feat.quarterly.install({"AAPL"}, engine=engine)
    assert "AAPL" in finagg.sec.feat.quarterly.get_candidate_ticker_set(engine=engine)
    assert "AAPL" in finagg.sec.feat.quarterly.get_ticker_set(engine=engine)


def test_quarterly_get_ticker_set_empty(engine: Engine) -> None:
    assert len(finagg.sec.feat.quarterly.get_ticker_set(engine=engine)) == 0


def test_quarterly_to_from_refined(engine: Engine) -> None:
    finagg.sec.feat.submissions.install({"AAPL"}, engine=engine)
    df1 = finagg.sec.feat.quarterly.from_api("AAPL")
    finagg.sec.feat.quarterly.to_refined("AAPL", df1, engine=engine)
    with pytest.raises(IntegrityError):
        finagg.sec.feat.quarterly.to_refined("AAPL", df1, engine=engine)

    df2 = finagg.sec.feat.quarterly.from_refined("AAPL", engine=engine)
    pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
