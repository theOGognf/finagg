"""Features from several sources."""

import pandas as pd
from sqlalchemy.engine import Engine

from .. import backend, sec, utils, yfinance
from . import store


class FundamentalFeatures:
    """Method for gathering fundamental data on a stock using several sources."""

    #: Columns within this feature set.
    columns = (
        yfinance.feat.daily.columns
        + sec.feat.quarterly.columns
        + ("PriceEarningsRatio",)
    )

    @classmethod
    def _normalize(
        cls,
        quarterly_df: pd.DataFrame,
        daily_df: pd.DataFrame,
        /,
    ) -> pd.DataFrame:
        """Normalize the feature columns."""
        df = pd.merge(
            quarterly_df, daily_df, how="outer", left_index=True, right_index=True
        )
        df = df.fillna(method="ffill").dropna()
        df["PriceEarningsRatio"] = df["price"] / df["EarningsPerShare"]
        df = utils.quantile_clip(df)
        df.index.names = ["date"]
        df = df[list(cls.columns)]
        return df.dropna()

    @classmethod
    def from_api(
        cls, ticker: str, /, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get features directly from APIs.

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
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        """
        quarterly_features = sec.feat.quarterly.from_api(ticker, start=start, end=end)
        start = str(quarterly_features.index[0])
        daily_features = yfinance.feat.daily.from_api(ticker, start=start, end=end)
        return cls._normalize(quarterly_features, daily_features)

    @classmethod
    def from_raw(
        cls,
        ticker: str,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get features directly from local SQL tables.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent publications
        are forward filled.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.
            engine: Raw data and feature store database engine.

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        """
        quarterly_features = sec.feat.quarterly.from_raw(
            ticker,
            start=start,
            end=end,
            engine=engine,
        )
        start = str(quarterly_features.index[0])
        daily_features = yfinance.feat.daily.from_raw(
            ticker,
            start=start,
            end=end,
            engine=engine,
        )
        return cls._normalize(quarterly_features, daily_features)

    @classmethod
    def from_store(
        cls,
        ticker: str,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get features from the feature-dedicated local SQL tables.

        This is the preferred method for accessing features for
        offline analysis (assuming data in the local SQL tables
        is current).

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.
            engine: Feature store database engine.

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        """
        with engine.begin() as conn:
            stmt = store.fundamental_features.c.ticker == ticker
            if start:
                stmt &= store.fundamental_features.c.date >= start
            if end:
                stmt &= store.fundamental_features.c.date <= end
            df = pd.DataFrame(
                conn.execute(store.fundamental_features.select().where(stmt))
            )
        df = df.pivot(index="date", values="value", columns="name").sort_index()
        df.columns = df.columns.rename(None)
        df = df[list(cls.columns)]
        return df

    @classmethod
    def to_store(
        cls,
        ticker: str,
        df: pd.DataFrame,
        /,
        *,
        engine: Engine = backend.engine,
    ) -> int:
        """Write the dataframe to the feature store for `ticker`.

        Does the necessary handling to transform columns to
        prepare the dataframe to be written to a dynamically-defined
        local SQL table.

        Args:
            ticker: Company ticker.
            df: Dataframe to store completely as rows in a local SQL
                table.
            engine: Feature store database engine.

        Returns:
            Number of rows written to the SQL table.

        """
        df = df.reset_index(names="date")
        df = df.melt("date", var_name="name", value_name="value")
        df["ticker"] = ticker
        with engine.begin() as conn:
            conn.execute(store.fundamental_features.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


#: Public-facing API.
fundam = FundamentalFeatures()
