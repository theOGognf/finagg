import shark


def test_get_cik() -> None:
    assert shark.sec.api.get_cik("AAPL") == "0000320193"


def test_get_ticker() -> None:
    assert shark.sec.api.get_ticker("0000320193") == "AAPL"


def test_submissions_get() -> None:
    shark.sec.api.submissions.get(ticker="AAPL")


def test_tickers_get() -> None:
    shark.sec.api.tickers.get()
