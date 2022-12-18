"""Features from yfinance sources."""

from functools import cache

import numpy as np
import pandas as pd
from sqlalchemy.sql import and_

from . import api, sql


class _DailyFeatures:
    """Methods for gathering daily stock data from Yahoo! finance."""

    @classmethod
    def _normalize(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize daily features columns."""
        df = df.drop(columns=["ticker"]).set_index("date").astype(float).sort_index()
        pct_change_columns = ["open", "high", "low", "close", "volume"]
        df["price"] = df["close"]

        def _safe_pct_change(col: pd.Series) -> pd.Series:
            col = col.pct_change().replace([-np.inf, np.inf], np.nan)
            mu = col.mean(numeric_only=True)
            two_sigma = 2 * col.std(numeric_only=True)
            return col.clip(lower=mu - two_sigma, upper=mu + two_sigma)

        df[pct_change_columns] = df[pct_change_columns].apply(_safe_pct_change)
        return df.dropna()

    @classmethod
    @cache
    def from_api(
        cls, ticker: str, /, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get daily features directly from the yfinance API.

        Args:
            ticker: Company ticker.
            start: The start date of the stock history.
                Defaults to the first recorded date.
            end: The end date of the stock history.
                Defaults to the last recorded date.

        Returns:
            Daily stock price dataframe. Sorted by date.

        """
        df = api.get(ticker, start=start, end=end)
        return cls._normalize(df)

    @classmethod
    @cache
    def from_sql(
        cls, ticker: str, /, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get daily features from local SQL tables.

        Args:
            ticker: Company ticker.
            start: The start date of the stock history.
                Defaults to the first recorded date.
            end: The end date of the stock history.
                Defaults to the last recorded date.

        Returns:
            Daily stock price dataframe. Sorted by date.

        """
        with sql.engine.connect() as conn:
            stmt = sql.prices.c.ticker == ticker
            if start:
                stmt = and_(stmt, sql.prices.c.date >= start)
            if end:
                stmt = and_(stmt, sql.prices.c.date <= end)
            df = pd.DataFrame(conn.execute(sql.prices.select(stmt)))
        return cls._normalize(df)


#: Public-facing API.
daily_features = _DailyFeatures