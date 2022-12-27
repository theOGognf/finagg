import finagg


def test_get_cik() -> None:
    assert finagg.sec.api.get_cik("AAPL") == "0000320193"


def test_get_ticker() -> None:
    assert finagg.sec.api.get_ticker("0000320193") == "AAPL"


def test_submissions_get() -> None:
    finagg.sec.api.submissions.get(ticker="AAPL")


def test_tickers_get() -> None:
    finagg.sec.api.tickers.get()
