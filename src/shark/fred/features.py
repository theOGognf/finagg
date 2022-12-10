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
        "CPIAUCNS",  # Consumer price index
        "CSUSHPINSA",  # S&P/Case-Shiller national home price index
        "FEDFUNDS",  # Federal funds interest rate
        "GDP",  # Gross domestic product
        "GDPC1",  # Real gross domestic product
        "GS10",  # 10-Year treasury yield
        "MICH",  # University of Michigan: inflation expectation
        "UMCSENT",  # University of Michigan: consumer sentiment
        "UNRATE",  # Unemployment rate
        "WALCL",  # US assets, total assets (less eliminations from consolidation)
    )

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
        return (
            dfs.pivot(index="date", values="value", columns="series_id")
            .sort_index()
            .fillna(method="ffill")
            .dropna()
        )

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
        return (
            df.pivot(index="date", values="value", columns="series_id")
            .sort_index()
            .fillna(method="ffill")
            .dropna()
        )
