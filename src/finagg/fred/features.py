"""Features from FRED sources."""

import pandas as pd
from sqlalchemy.engine import Engine

from .. import utils
from . import api, sql, store


class EconomicFeatures:
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

    #: Columns within this feature set.
    columns = series_ids

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
        df = df[list(cls.columns)]
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
    def from_raw(
        cls,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: Engine = sql.engine,
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

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        """
        table = sql.series
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

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        """
        table = store.economic_features
        with engine.begin() as conn:
            stmt = table.c.date >= "0000-00-00"
            if start:
                stmt &= table.c.date >= start
            if end:
                stmt &= table.c.date <= end
            df = pd.DataFrame(conn.execute(table.select().where(stmt)))
        df = df.pivot(index="date", values="value", columns="name").sort_index()
        df.columns = df.columns.rename(None)
        df = df[list(cls.columns)]
        return df

    @classmethod
    def to_store(
        cls,
        df: pd.DataFrame,
        /,
        *,
        engine: Engine = store.engine,
    ) -> int:
        """Write the dataframe to the feature store for `ticker`.

        Does the necessary handling to transform columns to
        prepare the dataframe to be written to a dynamically-defined
        local SQL table.

        Args:
            df: Dataframe to store completely as rows in a local SQL
                table.
            engine: Feature store database engine.

        Returns:
            Number of rows written to the SQL table.

        """
        df = df.reset_index(names="date")
        df = df.melt("date", var_name="name", value_name="value")
        table = store.economic_features
        with engine.begin() as conn:
            conn.execute(table.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


#: Public-facing API.
economic = EconomicFeatures()
