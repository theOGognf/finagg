"""Features from FRED sources."""

from functools import cache

import pandas as pd
from sqlalchemy import Column, String, Table
from sqlalchemy.sql import and_

from .. import utils
from . import api, sql, store


class _EconomicFeatures:
    """Methods for gathering economic data series from FRED sources."""

    #: Economic series IDs (typical economic indicators).
    series_ids = (
        "CIVPART",  # Labor force participation rate
        "CPIAUCNS",  # Consumer price index
        "CSUSHPINSA",  # S&P/Case-Shiller national home price index
        "FEDFUNDS",  # Federal funds interest rate
        "GDP",  # Gross domestic product
        "GDPC1",  # Real gross domestic product
        "GS10",  # 10-Year treasury yield
        "M2",  # Money stock measures (i.e., savings and related balances)
        "MICH",  # University of Michigan: inflation expectation
        "PSAVERT",  # Personal savings rate
        "UMCSENT",  # University of Michigan: consumer sentiment
        "UNRATE",  # Unemployment rate
        "WALCL",  # US assets, total assets (less eliminations from consolidation)
    )

    #: Name of feature store SQL table.
    table_name = "economic_features"

    @classmethod
    def _load_table(cls) -> None:
        economic_features = Table(
            "economic_features",
            store.metadata,
            Column(
                "date",
                String,
                primary_key=True,
                doc="Economic data series release date.",
            ),
            autoload_with=store.engine,
        )
        store.economic_features = economic_features

    @classmethod
    def _normalize(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize economic features columns."""
        df = (
            df.pivot(index="date", values="value", columns="series_id")
            .fillna(method="ffill")
            .dropna()
            .astype(float)
            .sort_index()
        )
        df = utils.quantile_clip(df)

        pct_change_columns = [
            "CIVPART",
            "CPIAUCNS",
            "CSUSHPINSA",
            "GDP",
            "GDPC1",
            "M2",
            "UMCSENT",
            "WALCL",
        ]
        df[pct_change_columns] = df[pct_change_columns].apply(utils.safe_pct_change)
        return df

    @classmethod
    @cache
    def from_api(
        cls, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get economic features directly from the FRED API.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent economic series
        are forward filled.

        Args:
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        """
        dfs = []
        for series_id in cls.series_ids:
            df = api.series.observations.get(
                series_id,
                realtime_start=0,
                realtime_end=-1,
                observation_start=start,
                observation_end=end,
                output_type=4,
            )
            dfs.append(df)
        df = pd.concat(dfs)
        return cls._normalize(df)

    @classmethod
    @cache
    def from_sql(
        cls, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get economic features from local FRED SQL tables.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent economic series
        are forward filled.

        Args:
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        """
        with sql.engine.connect() as conn:
            stmt = sql.series.c.series_id.in_(cls.series_ids)
            if start:
                stmt = and_(stmt, sql.series.c.date >= start)
            if end:
                stmt = and_(stmt, sql.series.c.date <= end)
            df = pd.DataFrame(conn.execute(sql.series.select(stmt)))
        return cls._normalize(df)

    @classmethod
    def from_store(
        cls, /, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        table = store.metadata.tables[cls.table_name]
        with store.engine.connect() as conn:
            stmt = table.c.date >= "0000-00-00"
            if start:
                stmt = and_(stmt, table.c.date >= start)
            if end:
                stmt = and_(stmt, table.c.date <= end)
            df = pd.DataFrame(conn.execute(table.select(stmt)))
        df = df.set_index("date")
        return df

    @classmethod
    def to_store(cls, df: pd.DataFrame, /) -> None | int:
        reflect_table = not store.inspector.has_table(cls.table_name)
        df = df.reset_index(names="date")
        size = len(df.index)
        df = df.drop_duplicates(["date"])
        if not reflect_table and size != len(df.index):
            raise RuntimeError(f"Primary key duplication error")
        rows = df.to_sql(cls.table_name, store.engine, if_exists="append", index=False)
        if reflect_table:
            cls._load_table()
        return rows


#: Public-facing API.
economic_features = _EconomicFeatures
