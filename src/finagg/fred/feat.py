"""Features from FRED sources."""


import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound

from .. import backend, feat, utils
from . import api, sql


class TimeSummarizedEconomicFeatures:
    """Methods for gathering time-averaged economic data from FRED
    features.

    """

    @classmethod
    def from_refined(
        cls,
        /,
        *,
        start: str = "1776-07-04",
        end: str = utils.today,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get the average and standard deviation of each series's
        feature across its history.

        Args:
            start: The start date of the observation period.
            end: The end date of the observation period.
            engine: Raw data and feature data SQL database engine.

        Returns:
            Average and standard deviation of each economic series
            across its respective history.

        Raises:
            `NoResultFound`: If there are no rows in the refined SQL table.

        """
        with engine.begin() as conn:
            stmt = sa.select(
                sql.economic.c.date,
                sql.economic.c.name,
                sa.func.avg(sql.economic.c.value).label("avg"),
                sa.func.std(sql.economic.c.value).label("std"),
            ).group_by(
                sql.economic.c.name,
            )
            df = pd.DataFrame(
                conn.execute(
                    stmt.where(
                        sql.economic.c.date >= start,
                        sql.economic.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No time-summarized economic rows found.")
        df = df.pivot(
            index=["date"],
            columns="name",
            values=["avg", "std"],
        ).sort_index()
        return df


class NormalizedEconomicFeatures:
    """Economic features from FRED data normalized according to historical
    averages.

    """

    @classmethod
    def from_other_refined(
        cls,
        *,
        start: str = "1776-07-04",
        end: str = utils.today,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get features from other feature SQL tables.

        Args:
            start: The start date of the observation period.
            end: The end date of the observation period.
            engine: Feature store database engine.

        Returns:
            Relative economic data dataframe with each series ID as a
            separate column. Sorted by date.

        """
        economic_df = EconomicFeatures.from_refined(start=start, end=end, engine=engine)
        summarized_df = TimeSummarizedEconomicFeatures.from_refined(
            start=start, end=end, engine=engine
        )
        economic_df = (economic_df - summarized_df["avg"]) / summarized_df["std"]
        economic_df = economic_df.sort_index()
        pct_change_cols = EconomicFeatures.pct_change_target_columns()
        economic_df[pct_change_cols] = economic_df[pct_change_cols].fillna(value=0.0)
        return economic_df.fillna(method="ffill").dropna()

    @classmethod
    def from_refined(
        cls,
        /,
        *,
        start: str = "1776-07-04",
        end: str = utils.today,
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

        Raises:
            `NoResultFound`: If there are no rows in the refined SQL table.

        """
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.normalized_economic.select().where(
                        sql.normalized_economic.c.date >= start,
                        sql.normalized_economic.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No normalized economic rows found.")
        df = df.pivot(index="date", values="value", columns="name").sort_index()
        df.columns = df.columns.rename(None)
        df = df[EconomicFeatures.columns]
        return df

    @classmethod
    def install(cls) -> int:
        """Drop the feature's table, create a new one, and insert data
        transformed from another raw SQL table.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        sql.normalized_economic.drop(backend.engine, checkfirst=True)
        sql.normalized_economic.create(backend.engine)

        df = cls.from_other_refined()
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

        Args:
            df: Dataframe to store completely as rows in a local SQL
                table.
            engine: Feature store database engine.

        Returns:
            Number of rows written to the SQL table.

        Raises:
            `ValueError`: If the given dataframe's columns do not match this
                feature's columns.

        """
        df = df.reset_index("date")
        if set(df.columns) < set(EconomicFeatures.columns):
            raise ValueError(f"Dataframe must have columns {EconomicFeatures.columns}")
        df = df.melt("date", var_name="name", value_name="value")
        with engine.begin() as conn:
            conn.execute(sql.normalized_economic.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class EconomicFeatures(feat.Features):
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

    #: Economic features averaged over time.
    normalized = NormalizedEconomicFeatures()

    #: Economic features aggregated over time.
    summary = TimeSummarizedEconomicFeatures()

    @classmethod
    def _normalize(cls, df: pd.DataFrame, /) -> pd.DataFrame:
        """Normalize economic features columns."""
        df = (
            df.pivot(index="date", values="value", columns="series_id")
            .sort_index()
            .fillna(method="ffill")
            .dropna()
            .astype(float)
        )
        df[cls.pct_change_target_columns()] = df[cls.pct_change_source_columns()].apply(
            utils.safe_pct_change
        )
        df.columns = df.columns.rename(None)
        df = df[cls.columns]
        return df.dropna()

    @classmethod
    def from_api(
        cls, *, start: str = "1776-07-04", end: str = utils.today
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
        start: str = "1776-07-04",
        end: str = utils.today,
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

        Raises:
            `NoResultFound`: If there are no rows in the raw SQL table.

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
        if not len(df.index):
            raise NoResultFound(f"No economic rows found.")
        return cls._normalize(df)

    @classmethod
    def from_refined(
        cls,
        /,
        *,
        start: str = "1776-07-04",
        end: str = utils.today,
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

        Raises:
            `NoResultFound`: If there are no rows in the refined SQL table.

        """
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.economic.select().where(
                        sql.economic.c.date >= start, sql.economic.c.date <= end
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No economic rows found.")
        df = df.pivot(index="date", values="value", columns="name").sort_index()
        df.columns = df.columns.rename(None)
        df = df[cls.columns]
        return df

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

        Args:
            df: Dataframe to store completely as rows in a local SQL
                table.
            engine: Feature store database engine.

        Returns:
            Number of rows written to the SQL table.

        Raises:
            `ValueError`: If the given dataframe's columns do not match this
                feature's columns.

        """
        df = df.reset_index("date")
        if set(df.columns) < set(cls.columns):
            raise ValueError(f"Dataframe must have columns {cls.columns}")
        df = df.melt("date", var_name="name", value_name="value")
        with engine.begin() as conn:
            conn.execute(sql.economic.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class SeriesFeatures:
    """Get a single economic series as-is from raw FRED data."""

    @classmethod
    def from_raw(
        cls,
        series_id: str,
        /,
        *,
        start: str = "1776-07-04",
        end: str = utils.today,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get a single economic data series as-is from raw FRED data.

        This is the preferred method for accessing raw FRED data without
        using the FRED API.

        Args:
            series_id: Economic data series ID.
            start: The start date of the observation period.
            end: The end date of the observation period.
            engine: Feature store database engine.

        Returns:
            A dataframe containing the economic data series values
            across the specified period.

        Raises:
            `NoResultFound`: If there are no rows for ``series_id`` in the
                raw SQL table.

        """
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sa.select(sql.series.c.date, sql.series.c.value).where(
                        sql.series.c.series_id == series_id,
                        sql.series.c.date >= start,
                        sql.series.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No series rows found for {series_id}.")
        return df.set_index(["date"]).sort_index()


#: Module variable intended for fully qualified name usage.
economic = EconomicFeatures()
series = SeriesFeatures()
