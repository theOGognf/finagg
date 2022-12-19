"""Features engineered from several sources."""

from functools import cache

import pandas as pd
from sqlalchemy import Column, String, Table
from sqlalchemy.sql import and_

from .. import sec, utils, yfinance
from ..sec.features import quarterly_features
from ..yfinance.features import daily_features
from . import sql


# Add feature store methods to other feature classes.
def _quarterly_features_load_table(cls: type[quarterly_features]) -> None:
    """Reflect the feature store SQL table."""
    quarterly_features = Table(
        cls.table_name,
        sql.metadata,
        Column("ticker", String, primary_key=True, doc="Unique company ticker."),
        Column("filed", String, primary_key=True, doc="Filing date."),
        autoload_with=sql.engine,
    )
    sql.quarterly_features = quarterly_features


def _quarterly_features_from_store(
    cls: type[quarterly_features],
    ticker: str,
    /,
    *,
    start: None | str = None,
    end: None | str = None,
) -> pd.DataFrame:
    ...


def _quarterly_features_to_store(
    cls: type[quarterly_features], ticker: str, df: pd.DataFrame, /
) -> None | int:
    ...


setattr(quarterly_features, "_load_table", classmethod(_quarterly_features_load_table))
setattr(quarterly_features, "from_store", classmethod(_quarterly_features_from_store))
setattr(quarterly_features, "to_store", classmethod(_quarterly_features_to_store))


def _daily_features_load_table(cls: type[daily_features]) -> None:
    """Reflect the feature store SQL table."""
    daily_features = Table(
        cls.table_name,
        sql.metadata,
        Column("ticker", String, primary_key=True, doc="Unique company ticker."),
        Column("date", String, primary_key=True, doc="Stock price date."),
        autoload_with=sql.engine,
    )
    sql.daily_features = daily_features


def _daily_features_from_store(
    cls: type[daily_features],
    ticker: str,
    /,
    *,
    start: None | str = None,
    end: None | str = None,
) -> pd.DataFrame:
    ...


def _daily_features_to_store(
    cls: type[daily_features], ticker: str, df: pd.DataFrame, /
) -> None | int:
    ...


setattr(daily_features, "_load_table", classmethod(_daily_features_load_table))
setattr(daily_features, "from_store", classmethod(_daily_features_from_store))
setattr(daily_features, "to_store", classmethod(_daily_features_to_store))


class _FundamentalFeatures:
    """Method for gathering fundamental data on a stock using several sources."""

    #: Name of feature store SQL table.
    table_name = "fundamental_features"

    @classmethod
    def _load_table(cls) -> None:
        """Reflect the feature store SQL table."""
        fundamental_features = Table(
            cls.table_name,
            sql.metadata,
            Column("ticker", String, primary_key=True, doc="Unique company ticker."),
            Column(
                "date", String, primary_key=True, doc="Filing and stock price dates."
            ),
            autoload_with=sql.engine,
        )
        sql.fundamental_features = fundamental_features

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
        return df.dropna()

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
        """Get engineered features from the feature store
        dedicated local SQL tables.

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
        table = sql.metadata.tables["fundamental_features"]
        with sql.engine.connect() as conn:
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
        reflect_table = not sql.inspector.has_table(cls.table_name)
        df = df.reset_index(names="date")
        df["ticker"] = ticker
        size = len(df.index)
        df = df.drop_duplicates(["ticker", "date"])
        if not reflect_table and size != len(df.index):
            raise RuntimeError(f"Primary key duplication error for `{ticker}`")
        rows = df.to_sql(cls.table_name, sql.engine, if_exists="append", index=False)
        if reflect_table:
            cls._load_table()
        return rows


#: Public-facing API.
fundamental_features = _FundamentalFeatures
