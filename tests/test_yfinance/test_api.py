import shark


def test_get() -> None:
    shark.yfinance.api.get("AAPL")
