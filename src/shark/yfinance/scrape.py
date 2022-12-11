"""Scrape the yfinance API for historical stock data and store into local SQL tables."""

import pandas as pd
import yfinance as yf

from .sql import engine, metadata
from .sql import prices as prices_table


def scrape(tickers: str | list[str], /) -> dict[str, int]:
    """Scrape yfinance historical stock data from the
    yfinance API.

    ALL TABLES ARE DROPPED PRIOR TO SCRAPING!
    Scraped data is loaded into local yfinance SQL tables.

    Args:
        tickers: Company tickers to scrape.

    Returns:
        A dictionary mapping tickers to number of rows scraped
        for each ticker.

    """
    if isinstance(tickers, str):
        tickers = [tickers]

    metadata.drop_all(engine)
    metadata.create_all(engine)

    with engine.connect() as conn:
        tickers_to_inserts = {}
        for ticker in tickers:
            ticker = yf.Ticker(ticker)
            df: pd.DataFrame = ticker.history(
                period="max", interval="1d", auto_adjust=True
            )
            df = df.reset_index()
            df["ticker"] = ticker.ticker
            df = df.drop(columns=["Dividends", "Stock Splits"])
            df.columns = map(str.lower, df.columns)
            tickers_to_inserts[ticker] = len(df.index)
            conn.execute(prices_table.insert(), df.to_dict(orient="records"))

    return tickers_to_inserts
