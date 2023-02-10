"""Features from FRED sources."""

import pandas as pd
from sqlalchemy import Column, Float, MetaData, String, Table, inspect
from sqlalchemy.engine import Engine

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
    def _create_table(
        cls,
        engine: Engine,
        metadata: MetaData,
        column_names: pd.Index,
        /,
    ) -> None:
        """Create the feature store SQL table."""
        primary_keys = {"date"}
        table_columns = [
            Column(
                "date",
                String,
                primary_key=True,
                doc="Economic data series release date.",
            ),
        ]

        for name in column_names:
            if name not in primary_keys:
                column = Column(name, Float)
                table_columns.append(column)

        economic_features = Table(
            cls.table_name,
            metadata,
            *table_columns,
        )
        economic_features.create(bind=engine)
        store.economic_features = economic_features

    @classmethod
    def _normalize(cls, df: pd.DataFrame, /) -> pd.DataFrame:
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
        df.columns = df.columns.rename(None)
        return df.dropna()

    @classmethod
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
    def from_sql(
        cls,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: Engine = sql.engine,
        metadata: MetaData = sql.metadata,
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
            engine: Raw store database engine.
            metadata: Metadata associated with the tables.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        """
        table: Table = metadata.tables["series"]
        with engine.begin() as conn:
            stmt = table.c.date >= "0000-00-00"
            stmt &= table.c.series_id.in_(cls.series_ids)
            if start:
                stmt &= table.c.date >= start
            if end:
                stmt &= table.c.date <= end
            df = pd.DataFrame(conn.execute(table.select().where(stmt)))
        return cls._normalize(df)

    @classmethod
    def from_store(
        cls,
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
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.
            engine: Feature store database engine.
            metadata: Metadata associated with the tables.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        """
        table: Table = metadata.tables[cls.table_name]
        with engine.begin() as conn:
            stmt = table.c.date >= "0000-00-00"
            if start:
                stmt &= table.c.date >= start
            if end:
                stmt &= table.c.date <= end
            df = pd.DataFrame(conn.execute(table.select().where(stmt)))
        df = df.set_index("date")
        return df

    @classmethod
    def to_store(
        cls,
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
            df: Dataframe to store completely as rows in a local SQL
                table.
            engine: Feature store database engine.
            metadata: Metadata associated with the tables.

        Returns:
            Number of rows written to the SQL table.

        """
        df = df.reset_index(names="date")
        inspector = store.inspector if engine is store.engine else inspect(engine)
        if not inspector.has_table(cls.table_name):
            cls._create_table(engine, metadata, df.columns)
        table: Table = metadata.tables[cls.table_name]
        with engine.begin() as conn:
            conn.execute(table.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


#: Public-facing API.
economic_features = _EconomicFeatures()
