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
    debug: bool = False,
) -> pd.DataFrame:
    """Get a ticker's stock price history.

    Does a simple transform on yfinance's ticker API dataframe result to be
    a bit more consistent with other API implementations.

    Args:
        ticker: Company ticker to get historical price data for.
        start: Start date for stock price history. Defaults to the first
            recorded date.
        end: End date for stock price history. Defaults to the last recorded
            date.
        interval: Frequency at which stock price history is grabbed.
        period: Time period to get in the past. ``"max"`` returns the full
            stock price history and the default.
        debug: Debug mode passed to ``yfinance``.

    Returns:
        :mod:`yfinance` auto-adjusted stock price history with slightly
        different (more normalized) column names.

    Examples:
        >>> finagg.yfinance.api.get("AAPL").head(5)
                 date    open    high     low   close     volume ticker
        0  1980-12-12  0.0997  0.1002  0.0997  0.0997  469033600   AAPL
        1  1980-12-15  0.0950  0.0950  0.0945  0.0945  175884800   AAPL
        2  1980-12-16  0.0880  0.0880  0.0876  0.0876  105728000   AAPL
        3  1980-12-17  0.0897  0.0902  0.0897  0.0897   86441600   AAPL
        4  1980-12-18  0.0924  0.0928  0.0924  0.0924   73449600   AAPL

    """
    stock = yf.Ticker(ticker)
    df = stock.history(
        period=period,
        interval=interval,
        start=start,
        end=end,
        auto_adjust=True,
        debug=debug,
    )
    df.index = pd.to_datetime(df.index).date.astype(str)
    df = df.rename_axis("date").reset_index()
    df["ticker"] = stock.ticker
    df = df.drop(columns=["Dividends", "Stock Splits"], errors="ignore")
    df.columns = map(str.lower, df.columns)
    return df  # type: ignore[no-any-return]
