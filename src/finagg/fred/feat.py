"""Features from FRED sources."""

from functools import cache

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from .. import backend, utils
from . import api, sql


class EconomicFeatures:
    """Methods for gathering economic data series from FRED sources."""

    #: Economic series IDs (typical economic indicators).
    series_ids = [
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
    ]

    #: Columns within this feature set.
    columns = [
        "CIVPART_pct_change",
        "CPIAUCNS_pct_change",
        "CSUSHPINSA_pct_change",
        "FEDFUNDS",
        "GDP_pct_change",
        "GDPC1_pct_change",
        "GS10",
        "M2_pct_change",
        "MICH",
        "PSAVERT",
        "UMCSENT_pct_change",
        "UNRATE",
        "WALCL_pct_change",
    ]

    #: Columns that're replaced with their respective percent changes.
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
        pct_change_columns = [f"{col}_pct_change" for col in cls.pct_change_columns]
        df[pct_change_columns] = df[cls.pct_change_columns].apply(utils.safe_pct_change)
        df.columns = df.columns.rename(None)
        df = df[cls.columns]
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
        start: str = "0000-00-00",
        end: str = "9999-99-99",
        engine: Engine = backend.engine,
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
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.series.select().where(
                        sql.series.c.series_id.in_(cls.series_ids),
                        sql.series.c.date >= start,
                        sql.series.c.date <= end,
                    )
                )
            )
        return cls._normalize(df)

    @classmethod
    def from_refined(
        cls,
        /,
        *,
        start: str = "0000-00-00",
        end: str = "9999-99-99",
        engine: Engine = backend.engine,
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
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.economic.select().where(
                        sql.economic.c.date >= start, sql.economic.c.date <= end
                    )
                )
            )
        df = df.pivot(index="date", values="value", columns="name").sort_index()
        df.columns = df.columns.rename(None)
        df = df[cls.columns]
        return df

    #: The candidate set is just the raw SQL series set.
    get_candidate_id_set = sql.get_id_set

    @classmethod
    @cache
    def get_id_set(
        cls,
        lb: int = 1,
    ) -> set[str]:
        """Get all unique series IDs in the feature's SQL table.

        Args:
            lb: Minimum number of rows required to include a series ID in the
                returned set.

        Returns:
            All unique series IDs that contain all the columns for creating
            economic features that also have at least `lb` rows.

        """
        with backend.engine.begin() as conn:
            tickers = set()
            for row in conn.execute(
                sa.select(sql.economic.c.series_id)
                .distinct()
                .group_by(sql.economic.c.series_id)
                .having(sa.func.count(sql.economic.c.date) >= lb)
            ):
                (ticker,) = row
                tickers.add(str(ticker))
        return tickers

    @classmethod
    def install(cls) -> int:
        """Drop the feature's table, create a new one, and insert data
        transformed from another raw SQL table.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        sql.economic.drop(backend.engine, checkfirst=True)
        sql.economic.create(backend.engine)

        df = cls.from_raw()
        total_rows = len(df.index)
        if total_rows:
            cls.to_refined(df)
        total_rows += total_rows
        return total_rows

    @classmethod
    def to_refined(
        cls,
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
            df: Dataframe to store completely as rows in a local SQL
                table.
            engine: Feature store database engine.

        Returns:
            Number of rows written to the SQL table.

        """
        df = df.reset_index(names="date")
        df = df.melt("date", var_name="name", value_name="value")
        with engine.begin() as conn:
            conn.execute(sql.economic.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


#: Public-facing API.
economic = EconomicFeatures()