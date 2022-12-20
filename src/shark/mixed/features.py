"""Features from several sources."""

from functools import cache

import pandas as pd
from sqlalchemy import Column, String, Table
from sqlalchemy.sql import and_

from .. import sec, utils, yfinance
from . import store


class _FundamentalFeatures:
    """Method for gathering fundamental data on a stock using several sources."""

    #: Name of feature store SQL table.
    table_name = "fundamental_features"

    @classmethod
    def _load_table(cls) -> None:
        """Reflect the feature store SQL table."""
        fundamental_features = Table(
            cls.table_name,
            store.metadata,
            Column("ticker", String, primary_key=True, doc="Unique company ticker."),
            Column(
                "date", String, primary_key=True, doc="Filing and stock price dates."
            ),
            autoload_with=store.engine,
        )
        store.fundamental_features = fundamental_features

    @classmethod
    def _normalize(
        cls, quarterly_df: pd.DataFrame, daily_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Normalize the feature columns."""
        df = pd.merge(
            quarterly_df, daily_df, how="outer", left_index=True, right_index=True
        )
        df = df.fillna(method="ffill").dropna()
        df["PriceEarningsRatio"] = df["price"] / df["EarningsPerShare"]
        df = utils.quantile_clip(df)
        return df.dropna()

    @classmethod
    @cache
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

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        """
        quarterly_features = sec.features.quarterly_features.from_sql(
            ticker, start=start, end=end
        )
        start = str(quarterly_features.index[0])
        daily_features = yfinance.features.daily_features.from_sql(
            ticker, start=start, end=end
        )
        return cls._normalize(quarterly_features, daily_features)

    @classmethod
    @cache
    def from_store(
        cls, ticker: str, /, *, start: None | str = None, end: None | str = None
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

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        """
        table = store.metadata.tables["fundamental_features"]
        with store.engine.connect() as conn:
            stmt = table.c.ticker == ticker
            if start:
                stmt = and_(stmt, table.c.date >= start)
            if end:
                stmt = and_(stmt, table.c.date <= end)
            df = pd.DataFrame(conn.execute(table.select(stmt)))
        df = df.set_index("date").drop(columns="ticker")
        return df

    @classmethod
    def to_store(cls, ticker: str, df: pd.DataFrame, /) -> None | int:
        """Write the dataframe to the feature store for `ticker`.

        Does the necessary handling to transform columns to
        prepare the dataframe to be written to a dynamically-defined
        local SQL table.

        Args:
            ticker: Company ticker.
            df: Dataframe to store completely as rows in a local SQL
                table.

        Returns:
            Number of rows written to the SQL table.

        """
        reflect_table = not store.inspector.has_table(cls.table_name)
        df = df.reset_index(names="date")
        df["ticker"] = ticker
        size = len(df.index)
        df = df.drop_duplicates(["ticker", "date"])
        if not reflect_table and size != len(df.index):
            raise RuntimeError(f"Primary key duplication error for `{ticker}`")
        rows = df.to_sql(cls.table_name, store.engine, if_exists="append", index=False)
        if reflect_table:
            cls._load_table()
        return rows


#: Public-facing API.
fundamental_features = _FundamentalFeatures
