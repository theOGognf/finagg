"""Features from FRED sources."""

from functools import cache

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from .. import backend, utils
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
        pad_fill_columns = [
            col for col in EconomicFeatures.columns if col.endswith("pct_change")
        ]
        economic_df[pad_fill_columns] = economic_df[pad_fill_columns].fillna(
            method="pad"
        )
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
        df = df.pivot(index="date", values="value", columns="name").sort_index()
        df.columns = df.columns.rename(None)
        df = df[EconomicFeatures.columns]
        return df

    @classmethod
    def get_candidate_id_set(cls, lb: int = 1) -> set[str]:
        """The candidate ID set is just the `economic` ID set."""
        return EconomicFeatures.get_id_set(lb=lb)

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
                sa.select(sql.normalized_economic.c.series_id)
                .distinct()
                .group_by(sql.normalized_economic.c.series_id)
                .having(sa.func.count(sql.normalized_economic.c.date) >= lb)
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
        df = df.reset_index("date")
        df = df.melt("date", var_name="name", value_name="value")
        with engine.begin() as conn:
            conn.execute(sql.normalized_economic.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


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

    #: Economic features averaged over time.
    normalized = NormalizedEconomicFeatures()

    #: Economic features aggregated over time.
    summary = TimeSummarizedEconomicFeatures()

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
        pct_change_columns = [col for col in cls.columns if col.endswith("pct_change")]
        df[pct_change_columns] = df[cls.pct_change_source_columns()].apply(
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

    @classmethod
    def get_candidate_id_set(
        cls,
        lb: int = 1,
    ) -> set[str]:
        """Get all unique series IDs in the raw SQL table that MAY BE ELIGIBLE
        to be in the feature's SQL table.

        Args:
            lb: Minimum number of rows required to include a series in the
                returned set.

        Returns:
            All unique series that may be valid for creating economic features
            that also have at least `lb` rows used for constructing the
            features.

        """
        return sql.get_id_set(lb=lb)

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
    def pct_change_source_columns(cls) -> list[str]:
        """Return the names of columns used for computed percent change
        columns.

        """
        return [
            col.removesuffix("_pct_change")
            for col in cls.columns
            if col.endswith("_pct_change")
        ]

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
        df = df.reset_index("date")
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
            ).set_index(["date"])
        return df


#: Module variable intended for fully qualified name usage.
economic = EconomicFeatures()
series = SeriesFeatures()
