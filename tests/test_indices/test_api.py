import shark


def test_djia_get() -> None:
    shark.indices.api.djia.get()


def test_get_ticker_set() -> None:
    shark.indices.api.get_ticker_set()


def test_nasdaq100_get() -> None:
    shark.indices.api.nasdaq100.get()


def test_sp500_get() -> None:
    shark.indices.api.sp500.get()
