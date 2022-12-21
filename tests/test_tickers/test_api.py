import shark


def test_djia_get() -> None:
    shark.tickers.api.djia.get()


def test_get_ticker_set() -> None:
    shark.tickers.api.get_ticker_set()


def test_nasdaq100_get() -> None:
    shark.tickers.api.nasdaq100.get()


def test_sp500_get() -> None:
    shark.tickers.api.sp500.get()
