"""Features from yfinance sources."""

from functools import cache

import pandas as pd
from sqlalchemy import Column, Float, MetaData, String, Table, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.sql import and_

from .. import utils
from . import api, sql, store


class _DailyFeatures:
    """Methods for gathering daily stock data from Yahoo! finance."""

    #: Name of feature store SQL table.
    table_name = "daily_features"

    @classmethod
    def _create_table(
        cls,
        column_names: pd.Index,
        /,
        *,
        engine: Engine = store.engine,
        metadata: MetaData = store.metadata,
    ) -> None:
        """Create the feature store SQL table."""
        primary_keys = {"ticker", "date"}
        table_columns = [
            Column("ticker", String, primary_key=True, doc="Unique company ticker."),
            Column("date", String, primary_key=True, doc="Stock price date."),
        ]

        for name in column_names:
            if name not in primary_keys:
                column = Column(name, Float)
                table_columns.append(column)

        daily_features = Table(
            cls.table_name,
            metadata,
            *table_columns,
        )
        daily_features.create(bind=engine)
        store.daily_features = daily_features

    @classmethod
    def _normalize(cls, df: pd.DataFrame, /) -> pd.DataFrame:
        """Normalize daily features columns."""
        df = (
            df.drop(columns=["ticker"])
            .fillna(method="ffill")
            .dropna()
            .set_index("date")
            .astype(float)
            .sort_index()
        )
        df["price"] = df["close"]
        df = utils.quantile_clip(df)
        pct_change_columns = ["open", "high", "low", "close", "volume"]
        df[pct_change_columns] = df[pct_change_columns].apply(utils.safe_pct_change)
        df.columns = df.columns.rename(None)
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
            Daily stock price dataframe. Sorted by date.

        """
        table: Table = metadata.tables[cls.table_name]
        with engine.connect() as conn:
            stmt = table.c.ticker == ticker
            if start:
                stmt = and_(stmt, table.c.date >= start)
            if end:
                stmt = and_(stmt, table.c.date <= end)
            df = pd.DataFrame(conn.execute(table.select(stmt)))
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
            cls._create_table(df.columns, engine=engine, metadata=metadata)
        table: Table = metadata.tables[cls.table_name]
        with engine.connect() as conn:
            conn.execute(table.insert(), df.to_dict(orient="records"))
        return len(df.index)


#: Public-facing API.
daily_features = _DailyFeatures
