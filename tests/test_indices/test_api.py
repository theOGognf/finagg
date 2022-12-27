import finagg


def test_djia_get() -> None:
    finagg.indices.api.djia.get()


def test_get_ticker_set() -> None:
    finagg.indices.api.get_ticker_set()


def test_nasdaq100_get() -> None:
    finagg.indices.api.nasdaq100.get()


def test_sp500_get() -> None:
    finagg.indices.api.sp500.get()
