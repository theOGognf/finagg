"""Features engineered from several sources."""

from functools import cache

import pandas as pd

from .. import sec, utils, yfinance
from ..sec.features import quarterly_features
from ..yfinance.features import daily_features


class _FundamentalFeatures:
    """Method for gathering fundamental data on a stock using several sources."""

    @classmethod
    def _normalize(
        cls, quarterly_df: pd.DataFrame, daily_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Normalize the engineered features columns."""
        df = pd.merge(
            quarterly_df, daily_df, how="outer", left_index=True, right_index=True
        )
        df = df.fillna(method="ffill").dropna()
        df["PriceEarningsRatio"] = df["price"] / df["EarningsPerShare"]
        df = utils.quantile_clip(df)
        return df

    @classmethod
    @cache
    def from_api(
        cls, ticker: str, /, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get engineered features directly from APIs.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent publications
        are forward filled.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.

        Returns:
            Quarterly and daily data dataframe with each tag as a
            separate column. Sorted by filing date.

        """
        quarterly_features = sec.features.quarterly_features.from_api(
            ticker, start=start, end=end
        )
        start = str(quarterly_features.index[0])
        daily_features = yfinance.features.daily_features.from_api(
            ticker, start=start, end=end
        )
        return cls._normalize(quarterly_features, daily_features)

    @classmethod
    @cache
    def from_sql(
        cls, ticker: str, /, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get engineered features directly from local SQL tables.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent publications
        are forward filled.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.

        Returns:
            Quarterly and daily data dataframe with each tag as a
            separate column. Sorted by filing date.

        """
        quarterly_features = sec.features.quarterly_features.from_sql(
            ticker, start=start, end=end
        )
        start = str(quarterly_features.index[0])
        daily_features = yfinance.features.daily_features.from_sql(
            ticker, start=start, end=end
        )
        return cls._normalize(quarterly_features, daily_features)


#: Public-facing API.
fundamental_features = _FundamentalFeatures
