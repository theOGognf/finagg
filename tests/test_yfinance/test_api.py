import finagg


def test_get() -> None:
    finagg.yfinance.api.get("AAPL")
