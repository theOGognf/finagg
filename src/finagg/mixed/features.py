"""Features from several sources."""

import numpy as np
import pandas as pd
from sqlalchemy import Column, Float, MetaData, String, Table, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.sql import and_

from .. import sec, utils, yfinance
from . import store


class _FundamentalFeatures:
    """Method for gathering fundamental data on a stock using several sources."""

    #: Indices to compare daily changes to.
    reference_indices = ("VOO", "VGT")

    #: Name of feature store SQL table.
    table_name = "fundamental_features"

    @classmethod
    def _create_table(
        cls,
        engine: Engine,
        metadata: MetaData,
        column_names: pd.Index,
        /,
    ) -> None:
        """Create the feature store SQL table."""
        primary_keys = {"ticker", "date"}
        table_columns = [
            Column("ticker", String, primary_key=True, doc="Unique company ticker."),
            Column(
                "date", String, primary_key=True, doc="Filing and stock price dates."
            ),
        ]

        for name in column_names.to_list():
            name = str(name)
            if name not in primary_keys:
                column = Column(name, Float)
                table_columns.append(column)

        fundamental_features = Table(
            cls.table_name,
            metadata,
            *table_columns,
        )
        fundamental_features.create(bind=engine)
        store.fundamental_features = fundamental_features

    @classmethod
    def _normalize(
        cls,
        quarterly_df: pd.DataFrame,
        daily_df: pd.DataFrame,
        indices_df: dict[str, pd.DataFrame],
        /,
    ) -> pd.DataFrame:
        """Normalize the feature columns."""
        df = pd.merge(
            quarterly_df, daily_df, how="outer", left_index=True, right_index=True
        )
        df = df.fillna(method="ffill").dropna()
        df["PriceEarningsRatio"] = df["price"] / df["EarningsPerShare"]
        df = utils.quantile_clip(df)
        relative_columns = ["open", "high", "low", "close", "volume"]
        for index, index_df in indices_df.items():
            for col in relative_columns:
                df[f"{index}_{col}"] = index_df[col] / daily_df[col]
                df[f"{index}_{col}"] = (
                    df[f"{index}_{col}"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
                )

        df.index.names = ["date"]
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
        quarterly_features = sec.features.quarterly_features.from_api(
            ticker, start=start, end=end
        )
        start = str(quarterly_features.index[0])
        daily_features = yfinance.features.daily_features.from_api(
            ticker, start=start, end=end
        )
        indices_features = {
            index: yfinance.features.daily_features.from_api(
                index, start=start, end=end
            )
            for index in cls.reference_indices
        }
        return cls._normalize(quarterly_features, daily_features, indices_features)

    @classmethod
    def from_sql(
        cls,
        ticker: str,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        sec_engine: Engine = sec.sql.engine,
        sec_metadata: MetaData = sec.sql.metadata,
        yf_engine: Engine = yfinance.sql.engine,
        yf_metadata: MetaData = yfinance.sql.metadata,
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
            sec_engine: Raw SEC store database engine.
            sec_metadata: Metadata associated with the SEC tables.
            yf_engine: Raw yfinance store database engine.
            yf_metadata: Metadata associated with the yfinance tables.

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        """
        quarterly_features = sec.features.quarterly_features.from_sql(
            ticker,
            start=start,
            end=end,
            engine=sec_engine,
            metadata=sec_metadata,
        )
        start = str(quarterly_features.index[0])
        daily_features = yfinance.features.daily_features.from_sql(
            ticker,
            start=start,
            end=end,
            engine=yf_engine,
            metadata=yf_metadata,
        )
        indices_features = {
            index: yfinance.features.daily_features.from_sql(
                index, start=start, end=end
            )
            for index in cls.reference_indices
        }
        return cls._normalize(quarterly_features, daily_features, indices_features)

    @classmethod
    def from_store(
        cls,
        ticker: str,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: Engine = store.engine,
        metadata: MetaData = store.metadata,
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
            metadata: Metadata associated with the tables.

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        """
        table: Table = metadata.tables[cls.table_name]
        with engine.begin() as conn:
            stmt = table.c.ticker == ticker
            if start:
                stmt = and_(stmt, table.c.date >= start)
            if end:
                stmt = and_(stmt, table.c.date <= end)
            df = pd.DataFrame(conn.execute(table.select().where(stmt)))
        df = df.set_index("date").drop(columns="ticker")
        return df

    @classmethod
    def to_store(
        cls,
        ticker: str,
        df: pd.DataFrame,
        /,
        *,
        engine: Engine = store.engine,
        metadata: MetaData = store.metadata,
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
            metadata: Metadata associated with the tables.

        Returns:
            Number of rows written to the SQL table.

        """
        df = df.reset_index(names="date")
        df["ticker"] = ticker
        inspector = store.inspector if engine is store.engine else inspect(engine)
        if not inspector.has_table(cls.table_name):
            cls._create_table(engine, metadata, df.columns)
        table: Table = metadata.tables[cls.table_name]
        with engine.begin() as conn:
            conn.execute(table.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


#: Public-facing API.
fundamental_features = _FundamentalFeatures()
