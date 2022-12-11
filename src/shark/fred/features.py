"""Features from FRED sources."""

import pandas as pd
from sqlalchemy.sql import and_

from . import api
from .sql import engine
from .sql import series as series_table


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
        df[pct_change_columns] = 100.0 * df[pct_change_columns].pct_change()
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
        dfs = pd.concat(dfs)
        return cls._normalize(dfs)

    @classmethod
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
        with engine.connect() as conn:
            stmt = series_table.c.series_id.in_(cls.series_ids)
            if start:
                stmt = and_(stmt, series_table.c.date >= start)
            if end:
                stmt = and_(stmt, series_table.c.date <= end)
            df = pd.DataFrame(conn.execute(series_table.select(stmt)))
        return cls._normalize(df)


#: Public-facing API.
economic_features = EconomicFeatures
