"""Refined FRED features (features aggregated from raw tables)."""


import logging

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound

from ... import backend, feat, utils
from .. import api, sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class TimeSummarizedEconomic:
    """Methods for gathering time-averaged economic data from FRED
    features.

    The class variable :attr:`finagg.fred.feat.Economic.summary` is an
    instance of this feature set implementation and is the most popular
    interface for calling feature methods.

    """

    @classmethod
    def from_refined(
        cls,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get the average and standard deviation of each series's
        feature across its history.

        Args:
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Average and standard deviation of each economic series
            across its respective history.

        Raises:
            `NoResultFound`: If there are no rows in the refined SQL table.

        Examples:
            >>> df = finagg.fred.economic.summary.from_refined().head(5)
            >>> df["avg"]  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
            name        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06         -6.0827e-06               0.0003 ...
            >>> df["std"]  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
            name        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06              0.0017               0.0015 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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


class NormalizedEconomic:
    """Economic features from FRED data normalized according to historical
    averages.

    The class variable :attr:`finagg.fred.feat.Economic.normalized` is an
    instance of this feature set implementation and is the most popular
    interface for calling feature methods.

    Examples:
        It doesn't matter which data source you use to gather features.
        They both return equivalent dataframes.

        >>> df1 = finagg.fred.feat.economic.normalized.from_other_refined().head(5)
        >>> df2 = finagg.fred.feat.economic.normalized.from_refined().head(5)
        >>> pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)

    """

    @classmethod
    def from_other_refined(
        cls,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get features from other feature SQL tables.

        Args:
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Relative economic data dataframe with each series ID as a
            separate column. Sorted by date.

        Examples:
            >>> finagg.fred.feat.economic.normalized.from_other_refined().head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06              0.0036              -0.1837 ...
            2014-10-08              0.0000               0.0000 ...
            2014-10-13              0.0000               0.0000 ...
            2014-10-15              0.0000               0.0000 ...
            2014-10-20              0.0000               0.0000 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        economic_df = Economic.from_refined(start=start, end=end, engine=engine)
        summarized_df = TimeSummarizedEconomic.from_refined(
            start=start, end=end, engine=engine
        )
        economic_df = (economic_df - summarized_df["avg"]) / summarized_df["std"]
        economic_df = economic_df.sort_index()
        pct_change_cols = Economic.pct_change_target_columns()
        economic_df[pct_change_cols] = economic_df[pct_change_cols].fillna(value=0.0)
        return economic_df.fillna(method="ffill").dropna()

    @classmethod
    def from_refined(
        cls,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get features from the feature-dedicated local SQL tables.

        This is the preferred method for accessing features for
        offline analysis (assuming data in the local SQL tables
        is current).

        Args:
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        Raises:
            `NoResultFound`: If there are no rows in the refined SQL table.

        Examples:
            >>> finagg.fred.feat.economic.normalized.from_refined().head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06              0.0036              -0.1837 ...
            2014-10-08              0.0000               0.0000 ...
            2014-10-13              0.0000               0.0000 ...
            2014-10-15              0.0000               0.0000 ...
            2014-10-20              0.0000               0.0000 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
        df = df[Economic.columns]
        return df

    @classmethod
    def install(
        cls, *, engine: None | Engine = None, recreate_tables: bool = False
    ) -> int:
        """Install data economic data series by pulling data from the economic
        feature SQL tables, transforming them into normalized features, and
        then writing to the refined normalized economic SQL table.

        Tables associated with this method are created if they don't already
        exist.

        Args:
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        engine = engine or backend.engine
        if recreate_tables or not sa.inspect(engine).has_table(
            sql.normalized_economic.name
        ):
            sql.normalized_economic.drop(engine, checkfirst=True)
            sql.normalized_economic.create(engine)

        df = cls.from_other_refined(engine=engine)
        rowcount = len(df.index)
        if rowcount:
            cls.to_refined(df, engine=engine)
            rowcount += rowcount
            logger.debug(f"{rowcount} normalized economic feature rows inserted")
        else:
            logger.debug("Skipping normalized economic features due to missing data")
        return rowcount

    @classmethod
    def to_refined(
        cls,
        df: pd.DataFrame,
        /,
        *,
        engine: None | Engine = None,
    ) -> int:
        """Write the dataframe to the feature store.

        Args:
            df: Dataframe to store completely as rows in a local SQL
                table.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the SQL table.

        Raises:
            `ValueError`: If the given dataframe's columns do not match this
                feature's columns.

        """
        engine = engine or backend.engine
        df = df.reset_index("date")
        if set(df.columns) < set(Economic.columns):
            raise ValueError(
                f"Dataframe must have columns {Economic.columns} but got {df.columns}"
            )
        df = df.melt("date", var_name="name", value_name="value")
        with engine.begin() as conn:
            conn.execute(sql.normalized_economic.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class Economic(feat.Features):
    """Methods for gathering economic data series from FRED sources.

    The module variable :data:`finagg.fred.feat.economic` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    Examples:
        It doesn't matter which data source you use to gather features.
        They all return equivalent dataframes.

        >>> df1 = finagg.fred.feat.economic.from_api().head(5)
        >>> df2 = finagg.fred.feat.economic.from_raw().head(5)
        >>> df3 = finagg.fred.feat.economic.from_refined().head(5)
        >>> pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
        >>> pd.testing.assert_frame_equal(df1, df3, rtol=1e-4)

    """

    #: Economic series IDs (typical economic indicators) used for constructing
    #: this feature set.
    series_ids = api.popular_series

    #: Columns within this feature set. Dataframes returned by this class's
    #: methods will always contain these columns. The refined data SQL table
    #: corresponding to these features will also have rows that have these
    #: names.
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

    normalized = NormalizedEconomic()
    """Economic features normalized over time.
    The most popular way for accessing the
    :class:`finagg.fred.feat.NormalizedEconomic` feature set.

    :meta hide-value:
    """

    summary = TimeSummarizedEconomic()
    """Economic features aggregated over time.
    The most popular way for accessing the
    :class:`finagg.fred.feat.TimeSummarizedEconomic` feature set.

    :meta hide-value:
    """

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
        cls, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get economic features directly from the FRED API.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent economic series
        are forward filled.

        Args:
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        Examples:
            >>> finagg.fred.feat.economic.from_api().head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06                 0.0                  0.0 ...
            2014-10-08                 0.0                  0.0 ...
            2014-10-13                 0.0                  0.0 ...
            2014-10-15                 0.0                  0.0 ...
            2014-10-20                 0.0                  0.0 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
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
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get economic features from local FRED SQL tables.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent economic series
        are forward filled.

        Args:
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        Raises:
            `NoResultFound`: If there are no rows in the raw SQL table.

        Examples:
            >>> finagg.fred.feat.economic.from_raw().head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06                 0.0                  0.0 ...
            2014-10-08                 0.0                  0.0 ...
            2014-10-13                 0.0                  0.0 ...
            2014-10-15                 0.0                  0.0 ...
            2014-10-20                 0.0                  0.0 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get features from the feature-dedicated local SQL tables.

        This is the preferred method for accessing features for
        offline analysis (assuming data in the local SQL tables
        is current).

        Args:
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        Raises:
            `NoResultFound`: If there are no rows in the refined SQL table.

        Examples:
            >>> finagg.fred.feat.economic.from_refined().head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06                 0.0                  0.0 ...
            2014-10-08                 0.0                  0.0 ...
            2014-10-13                 0.0                  0.0 ...
            2014-10-15                 0.0                  0.0 ...
            2014-10-20                 0.0                  0.0 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
    def install(
        cls,
        *,
        engine: None | Engine = None,
        recreate_tables: bool = False,
    ) -> int:
        """Install economic data by pulling data from the raw SQL tables,
        transforming them into economic features, and then writing to the
        refined economic data SQL table.

        Tables associated with this method are created if they don't already
        exist.

        Args:
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.
            recreate_tables: Whether to drop and recreate tables, wiping all
                previously installed data.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        engine = engine or backend.engine
        if recreate_tables or not sa.inspect(engine).has_table(sql.economic.name):
            sql.economic.drop(engine, checkfirst=True)
            sql.economic.create(engine)

        df = cls.from_raw(engine=engine)
        rowcount = len(df.index)
        if rowcount:
            cls.to_refined(df, engine=engine)
            rowcount += rowcount
            logger.debug(f"{rowcount} economic feature rows inserted")
        else:
            logger.debug("Skipping economic features due to missing data")
        return rowcount

    @classmethod
    def to_refined(
        cls,
        df: pd.DataFrame,
        /,
        *,
        engine: None | Engine = None,
    ) -> int:
        """Write the dataframe to the feature store.

        Args:
            df: Dataframe to store completely as rows in a local SQL
                table.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the SQL table.

        Raises:
            `ValueError`: If the given dataframe's columns do not match this
                feature's columns.

        """
        engine = engine or backend.engine
        df = df.reset_index("date")
        if set(df.columns) < set(cls.columns):
            raise ValueError(
                f"Dataframe must have columns {cls.columns} but got {df.columns}"
            )
        df = df.melt("date", var_name="name", value_name="value")
        with engine.begin() as conn:
            conn.execute(sql.economic.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)
