"""Simple wrappers for `yfinance.Ticker`."""

import pandas as pd
import yfinance as yf


def get(
    ticker: str,
    /,
    *,
    start: None | str = None,
    end: None | str = None,
    interval: str = "1d",
    period: str = "max",
) -> pd.DataFrame:
    """Get a ticker's stock price history.

    Does a simple transform on yfinance's ticker API
    dataframe result to be compatible with shark's
    local SQL tables.

    Args:
        ticker: Ticker to get.
        start: Start date for stock price history.
        end: End date for stock price history.
        interval: Frequency at which stock price history is grapped.
        period: Time period to get in the past. `"max"` is the full
            stock price history.

    Returns:
        A dataframe with normalized column names and values.

    """
    stock = yf.Ticker(ticker)
    df: pd.DataFrame = stock.history(
        period=period, interval=interval, start=start, end=end, auto_adjust=True
    )
    df = df.reset_index()

    def _strftime(item: pd.Timestamp) -> str:
        return item.strftime("%Y-%m-%d")

    df["Date"] = df["Date"].apply(_strftime)
    df["ticker"] = stock.ticker
    df = df.drop(columns=["Dividends", "Stock Splits"])
    df.columns = map(str.lower, df.columns)  # type: ignore
    return df
