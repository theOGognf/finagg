import finagg


def test_djia_get() -> None:
    df = finagg.indices.api.djia.get()
    assert "AAPL" in set(df["ticker"])


def test_get_ticker_set() -> None:
    tickers = finagg.indices.api.get_ticker_set()
    assert "AAPL" in tickers


def test_nasdaq100_get() -> None:
    df = finagg.indices.api.nasdaq100.get()
    assert "AAPL" in set(df["ticker"])


def test_sp500_get() -> None:
    df = finagg.indices.api.sp500.get()
    assert "AAPL" in set(df["ticker"])
